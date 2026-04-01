"""Workspace message tools: search_messages, get_channel_history."""

import logging
from typing import Dict

from lib.tools.base import ToolCategory, ToolContext, ToolResult, display_result, success, error
from lib.tools.registry import tool
from api.services.users import attach_public_profiles

logger = logging.getLogger(__name__)

# Max characters per message returned in results
_MSG_CONTENT_CAP = 500


def _format_message(msg: Dict) -> Dict:
    """Format a single channel message for AI consumption."""
    sender = None
    if msg.get("user"):
        sender = msg["user"].get("name") or "Unknown"
    elif msg.get("agent"):
        sender = msg["agent"].get("name", "Agent")

    channel_name = None
    workspace_name = None
    if msg.get("channel"):
        channel_name = msg["channel"].get("name")
        wa = msg["channel"].get("workspace_app")
        if wa and wa.get("workspace"):
            workspace_name = wa["workspace"].get("name")

    content = msg.get("content") or ""
    truncated = len(content) > _MSG_CONTENT_CAP

    formatted = {
        "id": msg["id"],
        "channel_id": msg.get("channel_id"),
        "sender": sender,
        "content": content[:_MSG_CONTENT_CAP] + ("..." if truncated else ""),
        "created_at": msg.get("created_at"),
        "reply_count": msg.get("reply_count", 0),
    }
    if channel_name:
        formatted["channel_name"] = channel_name
    if workspace_name:
        formatted["workspace_name"] = workspace_name
    return formatted


# Shared select columns for message queries
_MESSAGE_SELECT = (
    "id, content, created_at, channel_id, user_id, agent_id, "
    "thread_parent_id, reply_count, "
    "channel:channels(id, name, workspace_app:workspace_apps(workspace:workspaces(name))), "
    "agent:agent_instances(id, name, avatar_url)"
)


@tool(
    name="search_messages",
    description=(
        "Search workspace channel messages by keyword. Returns matching messages "
        "with IDs and timestamps. Use get_channel_history with a message ID to "
        "read surrounding context."
    ),
    params={
        "query": "Search query (keywords, person name, topic)",
        "channel_id": "Optional: limit search to a specific channel ID",
        "limit": "Maximum results to return (default 20, max 50)",
    },
    required=["query"],
    category=ToolCategory.MESSAGES,
    status="Searching messages..."
)
async def search_messages(args: Dict, ctx: ToolContext) -> ToolResult:
    from lib.supabase_client import get_authenticated_async_client

    query = args.get("query", "").strip()
    channel_id = args.get("channel_id")
    limit = min(int(args.get("limit", 20)), 50)

    if not query:
        return error("Search query is required")

    logger.info(f"[CHAT] User {ctx.user_id} searching messages: '{query}' (channel={channel_id}, limit={limit})")

    try:
        supabase = await get_authenticated_async_client(ctx.user_jwt)

        q = supabase.table("channel_messages") \
            .select(_MESSAGE_SELECT) \
            .text_search("content", query, options={"config": "english"}) \
            .order("created_at", desc=True) \
            .limit(limit)

        if channel_id:
            q = q.eq("channel_id", channel_id)

        result = await q.execute()
        messages = result.data or []

        logger.info(f"[CHAT] Found {len(messages)} messages matching '{query}'")

        if not messages:
            return success({"messages": [], "count": 0}, f"No messages found for '{query}'")

        await attach_public_profiles(messages)

        formatted = [_format_message(msg) for msg in messages]

        display_items = []
        for fm in formatted:
            ws = fm.get("workspace_name")
            ch = fm.get("channel_name", "unknown")
            title = f"{ws} › #{ch}" if ws else f"#{ch}"
            display_items.append({
                "id": fm["id"],
                "type": "message",
                "title": title,
                "content": fm["content"][:200],
                "metadata": {
                    "sender": fm["sender"],
                    "channel_name": ch,
                    "workspace_name": ws,
                    "channel_id": fm["channel_id"],
                    "created_at": fm["created_at"],
                    "reply_count": fm["reply_count"],
                },
            })

        return display_result(
            data={"messages": formatted, "count": len(formatted), "query": query},
            display_type="messages",
            items=display_items,
            total=len(display_items),
            description=f"Found {len(formatted)} messages matching '{query}'"
        )

    except Exception as e:
        logger.error(f"[CHAT] Message search failed: {e}")
        return error(f"Failed to search messages: {str(e)}")


@tool(
    name="get_channel_history",
    description=(
        "Read a batch of messages from a channel. Supports two modes:\n"
        "1. Around a message: pass 'around_message_id' to read context surrounding a specific message "
        "(useful after search_messages finds a hit).\n"
        "2. Recent / paginated: pass 'before' (ISO timestamp) to page backwards through history, "
        "or omit to get the most recent messages.\n"
        "Returns messages in chronological order with a 'has_more' flag for pagination."
    ),
    params={
        "channel_id": "Channel ID to read messages from",
        "around_message_id": "Optional: center the batch around this message ID (from search results)",
        "before": "Optional: ISO timestamp cursor — fetch messages before this time (for paging back)",
        "limit": "Batch size (default 30, min 20, max 50)",
    },
    required=["channel_id"],
    category=ToolCategory.MESSAGES,
    status="Reading channel history..."
)
async def get_channel_history(args: Dict, ctx: ToolContext) -> ToolResult:
    from lib.supabase_client import get_authenticated_async_client

    channel_id = args.get("channel_id")
    around_message_id = args.get("around_message_id")
    before = args.get("before")
    limit = max(20, min(int(args.get("limit", 30)), 50))

    if not channel_id:
        return error("channel_id is required")

    logger.info(
        f"[CHAT] User {ctx.user_id} reading channel history: "
        f"channel={channel_id}, around={around_message_id}, before={before}, limit={limit}"
    )

    try:
        supabase = await get_authenticated_async_client(ctx.user_jwt)

        # Mode 1: center around a specific message
        if around_message_id:
            # First, get the target message's timestamp
            anchor = await supabase.table("channel_messages") \
                .select("created_at") \
                .eq("id", around_message_id) \
                .single() \
                .execute()

            if not anchor.data:
                return error(f"Message {around_message_id} not found")

            anchor_ts = anchor.data["created_at"]
            half = limit // 2

            # Fetch messages before anchor (inclusive)
            before_q = supabase.table("channel_messages") \
                .select(_MESSAGE_SELECT) \
                .eq("channel_id", channel_id) \
                .is_("thread_parent_id", "null") \
                .lte("created_at", anchor_ts) \
                .order("created_at", desc=True) \
                .limit(half)
            before_result = await before_q.execute()

            # Fetch messages after anchor
            after_q = supabase.table("channel_messages") \
                .select(_MESSAGE_SELECT) \
                .eq("channel_id", channel_id) \
                .is_("thread_parent_id", "null") \
                .gt("created_at", anchor_ts) \
                .order("created_at", desc=False) \
                .limit(limit - half)
            after_result = await after_q.execute()

            before_msgs = list(reversed(before_result.data or []))
            after_msgs = after_result.data or []
            messages = before_msgs + after_msgs

            await attach_public_profiles(messages)

            formatted = [_format_message(msg) for msg in messages]
            return success(
                {
                    "messages": formatted,
                    "count": len(formatted),
                    "channel_id": channel_id,
                    "anchor_message_id": around_message_id,
                },
                f"Read {len(formatted)} messages around target in channel"
            )

        # Mode 2: recent or paginated
        q = supabase.table("channel_messages") \
            .select(_MESSAGE_SELECT) \
            .eq("channel_id", channel_id) \
            .is_("thread_parent_id", "null") \
            .order("created_at", desc=True) \
            .limit(limit + 1)  # +1 to detect has_more

        if before:
            q = q.lt("created_at", before)

        result = await q.execute()
        raw = result.data or []

        has_more = len(raw) > limit
        messages = list(reversed(raw[:limit]))  # chronological order

        await attach_public_profiles(messages)

        formatted = [_format_message(msg) for msg in messages]

        next_cursor = None
        if has_more and formatted:
            next_cursor = formatted[0]["created_at"]

        return success(
            {
                "messages": formatted,
                "count": len(formatted),
                "channel_id": channel_id,
                "has_more": has_more,
                "next_cursor": next_cursor,
            },
            f"Read {len(formatted)} messages from channel" + (" (more available)" if has_more else "")
        )

    except Exception as e:
        logger.error(f"[CHAT] Channel history fetch failed: {e}")
        return error(f"Failed to read channel history: {str(e)}")
