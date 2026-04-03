"""Message management service for workspace messaging."""

from typing import Dict, Any, List, Optional, Tuple, cast
import logging
from lib.supabase_client import get_authenticated_async_client
from lib.supabase_client import get_async_service_role_client
from lib.r2_client import get_r2_client
from lib.image_proxy import generate_file_url, is_image_type
from api.config import settings
from api.services.users import attach_public_profiles

logger = logging.getLogger(__name__)

_MESSAGE_SELECT = "*, agent:agent_instances(id, name, avatar_url), reactions:message_reactions(*)"


def extract_plain_text(blocks: List[Dict[str, Any]]) -> str:
    """
    Extract plain text content from blocks for search indexing.

    Args:
        blocks: Array of content blocks

    Returns:
        Plain text string
    """
    text_parts = []

    for block in blocks:
        block_type = block.get("type")
        data = block.get("data", {})

        if block_type == "text":
            text_parts.append(data.get("content", ""))
        elif block_type == "mention":
            text_parts.append(f"@{data.get('display_name', '')}")
        elif block_type == "code":
            text_parts.append(data.get("content", ""))
        elif block_type == "quote":
            text_parts.append(data.get("preview", ""))
        elif block_type == "shared_message":
            text_parts.append(data.get("original_content", ""))

    return " ".join(text_parts).strip()


async def get_messages(
    channel_id: str,
    user_jwt: str,
    limit: int = 50,
    offset: int = 0,
    before_id: Optional[str] = None,
    thread_parent_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get messages from a channel.

    Args:
        channel_id: The channel ID
        user_jwt: User's JWT for authenticated requests
        limit: Max number of messages to return
        offset: Offset for pagination
        before_id: Get messages before this message ID (for infinite scroll)
        thread_parent_id: If set, get thread replies for this message

    Returns:
        List of messages with user info
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        query = (
            supabase.table("channel_messages")
            .select(_MESSAGE_SELECT)
            .eq("channel_id", channel_id)
        )

        # Filter by thread or main messages
        if thread_parent_id:
            query = query.eq("thread_parent_id", thread_parent_id)
        else:
            query = query.is_("thread_parent_id", "null")

        # Pagination — use composite (created_at, id) cursor to avoid
        # skipping messages that share the same timestamp as the boundary.
        if before_id:
            before_result = await (
                supabase.table("channel_messages")
                .select("created_at")
                .eq("id", before_id)
                .limit(1)
                .execute()
            )
            if before_result.data and len(before_result.data) > 0:
                before_ts = before_result.data[0]["created_at"]
                # Messages strictly older, OR same timestamp but with a smaller id
                query = query.or_(
                    f"created_at.lt.{before_ts},"
                    f"and(created_at.eq.{before_ts},id.lt.{before_id})"
                )

        result = await (
            query
            .order("created_at", desc=True)
            .order("id", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )

        messages = result.data or []
        # Reverse to get chronological order
        messages.reverse()

        await attach_public_profiles(messages)

        # Enrich file blocks with presigned URLs
        await _enrich_messages_with_file_urls(messages, user_jwt)

        logger.info(f"Retrieved {len(messages)} messages from channel {channel_id}")
        return messages

    except Exception as e:
        logger.error(f"Error getting messages: {e}")
        raise


async def get_message(
    message_id: str,
    user_jwt: str,
) -> Optional[Dict[str, Any]]:
    """
    Get a single message by ID.

    Args:
        message_id: The message ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        Message data or None if not found
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        result = await (
            supabase.table("channel_messages")
            .select(_MESSAGE_SELECT)
            .eq("id", message_id)
            .limit(1)
            .execute()
        )

        if result.data and len(result.data) > 0:
            message = cast(Dict[str, Any], result.data[0])
            await attach_public_profiles([message])
            await _enrich_messages_with_file_urls([message], user_jwt)
            return message
        return None

    except Exception as e:
        logger.error(f"Error getting message {message_id}: {e}")
        raise


async def create_message(
    channel_id: str,
    user_id: str,
    user_jwt: str,
    blocks: List[Dict[str, Any]],
    thread_parent_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new message.

    Args:
        channel_id: The channel ID
        user_id: User sending the message
        user_jwt: User's JWT for authenticated requests
        blocks: Content blocks array
        thread_parent_id: If replying in a thread, the parent message ID

    Returns:
        Created message data
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Extract plain text for search
    content = extract_plain_text(blocks)

    try:
        insert_data = {
            "channel_id": channel_id,
            "user_id": user_id,
            "content": content,
            "blocks": blocks,
        }

        if thread_parent_id:
            insert_data["thread_parent_id"] = thread_parent_id

        result = await (
            supabase.table("channel_messages")
            .insert(insert_data)
            .execute()
        )

        if result.data and len(result.data) > 0:
            message = result.data[0]

            # Fetch with user info
            full_message = await get_message(message["id"], user_jwt)
            logger.info(f"Created message {message['id']} in channel {channel_id}")

            # Embed for semantic search (fire-and-forget)
            from lib.embed_hooks import embed_message
            embed_message(message["id"], content)

            return full_message or message

        raise Exception("Failed to create message")

    except Exception as e:
        logger.error(f"Error creating message: {e}")
        raise


async def update_message(
    message_id: str,
    user_jwt: str,
    blocks: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Update a message.

    Args:
        message_id: The message ID
        user_jwt: User's JWT for authenticated requests
        blocks: New content blocks array

    Returns:
        Updated message data
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Extract plain text for search
    content = extract_plain_text(blocks)

    try:
        result = await (
            supabase.table("channel_messages")
            .update({
                "content": content,
                "blocks": blocks,
                "is_edited": True,
                "edited_at": "now()",
            })
            .eq("id", message_id)
            .execute()
        )

        if result.data and len(result.data) > 0:
            # Fetch with user info (like create_message does)
            full_message = await get_message(message_id, user_jwt)
            logger.info(f"Updated message {message_id}")

            # Re-embed for semantic search (fire-and-forget)
            from lib.embed_hooks import embed_message
            embed_message(message_id, content)

            return full_message or result.data[0]

        raise Exception("Message not found or no permission")

    except Exception as e:
        logger.error(f"Error updating message {message_id}: {e}")
        raise


async def delete_message(
    message_id: str,
    user_jwt: str,
) -> bool:
    """
    Delete a message.

    Args:
        message_id: The message ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        True if successful
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        await (
            supabase.table("channel_messages")
            .delete()
            .eq("id", message_id)
            .execute()
        )

        logger.info(f"Deleted message {message_id}")
        return True

    except Exception as e:
        logger.error(f"Error deleting message {message_id}: {e}")
        raise


async def add_reaction(
    message_id: str,
    user_id: str,
    user_jwt: str,
    emoji: str,
) -> Dict[str, Any]:
    """
    Add a reaction to a message.

    Args:
        message_id: The message ID
        user_id: User adding the reaction
        user_jwt: User's JWT for authenticated requests
        emoji: Emoji character/code

    Returns:
        Created reaction data
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        result = await (
            supabase.table("message_reactions")
            .insert({
                "message_id": message_id,
                "user_id": user_id,
                "emoji": emoji,
            })
            .execute()
        )

        if result.data and len(result.data) > 0:
            logger.info(f"Added reaction {emoji} to message {message_id}")
            return result.data[0]

        raise Exception("Failed to add reaction")

    except Exception as e:
        # Could be duplicate - that's okay
        if "duplicate" in str(e).lower():
            logger.info(f"Reaction {emoji} already exists on message {message_id}")
            return {"message_id": message_id, "user_id": user_id, "emoji": emoji}
        logger.error(f"Error adding reaction: {e}")
        raise


async def remove_reaction(
    message_id: str,
    user_id: str,
    user_jwt: str,
    emoji: str,
) -> bool:
    """
    Remove a reaction from a message.

    Args:
        message_id: The message ID
        user_id: User removing the reaction
        user_jwt: User's JWT for authenticated requests
        emoji: Emoji character/code

    Returns:
        True if successful
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        await (
            supabase.table("message_reactions")
            .delete()
            .eq("message_id", message_id)
            .eq("user_id", user_id)
            .eq("emoji", emoji)
            .execute()
        )

        logger.info(f"Removed reaction {emoji} from message {message_id}")
        return True

    except Exception as e:
        logger.error(f"Error removing reaction: {e}")
        raise


async def get_thread_replies(
    parent_message_id: str,
    user_jwt: str,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Get replies to a thread.

    Args:
        parent_message_id: The parent message ID
        user_jwt: User's JWT for authenticated requests
        limit: Max number of replies
        offset: Offset for pagination

    Returns:
        List of thread replies
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        result = await (
            supabase.table("channel_messages")
            .select(_MESSAGE_SELECT)
            .eq("thread_parent_id", parent_message_id)
            .order("created_at")
            .range(offset, offset + limit - 1)
            .execute()
        )

        replies = result.data or []

        await attach_public_profiles(replies)

        # Enrich file blocks with presigned URLs
        await _enrich_messages_with_file_urls(replies, user_jwt)

        return replies

    except Exception as e:
        logger.error(f"Error getting thread replies: {e}")
        raise


# =============================================================================
# Helpers: Enrich file blocks with image proxy / presigned download URLs
# =============================================================================

async def _enrich_messages_with_file_urls(messages: List[Dict[str, Any]], user_jwt: str) -> None:
    """
    Scan message blocks and attach URLs for file blocks.

    If the image proxy is configured, generates deterministic HMAC-signed URLs
    that are CDN-cacheable. Falls back to presigned URLs if not configured.

    Mutates the message dicts in-place.
    """
    if not messages:
        return

    # Feature flag: use legacy presigned URLs if proxy is not configured
    if not settings.image_proxy_url or not settings.image_proxy_secret:
        return await _enrich_messages_with_file_urls_legacy(messages, user_jwt)

    # Collect ALL file blocks by file_id for a single batch DB lookup.
    # Never trust client-supplied r2_key — always resolve from DB via file_id
    # to prevent forged access to arbitrary R2 objects.
    file_id_to_blocks: Dict[str, List[Dict[str, Any]]] = {}

    for msg in messages:
        blocks = msg.get("blocks") or []
        if not isinstance(blocks, list) or not blocks:
            continue

        for block in blocks:
            if not isinstance(block, dict) or block.get("type") != "file":
                continue
            data = block.get("data") or {}
            fid = data.get("file_id") or data.get("id")
            if fid:
                file_id_to_blocks.setdefault(fid, []).append(data)

    if not file_id_to_blocks:
        return

    # Single batch query: resolve file_id -> (r2_key, file_type) from DB.
    # Uses the authenticated client so RLS enforces the user can only
    # access files they own or that belong to their workspace.
    try:
        auth_client = await get_authenticated_async_client(user_jwt)
        result = await (
            auth_client.table("files")
            .select("id, r2_key, file_type")
            .in_("id", list(file_id_to_blocks.keys()))
            .execute()
        )
        for f in (result.data or []):
            r2_key = f.get("r2_key")
            if not r2_key:
                continue
            mime = f.get("file_type", "application/octet-stream")
            for data_ref in file_id_to_blocks.get(f["id"], []):
                data_ref["r2_key"] = r2_key
                if is_image_type(mime):
                    # Use a dedicated chat-optimized inline variant for faster
                    # loads while keeping good quality on high-DPI screens.
                    chat_url = generate_file_url(r2_key, mime, "chat")
                    preview_url = generate_file_url(r2_key, mime, "preview")
                    full_url = generate_file_url(r2_key, mime, "full")
                    data_ref["chat_url"] = chat_url
                    data_ref["preview_url"] = preview_url
                    data_ref["full_url"] = full_url
                    data_ref["url"] = chat_url or preview_url or full_url
                else:
                    data_ref["url"] = generate_file_url(r2_key, mime, "full")
    except Exception as e:
        logger.warning(f"Batch file lookup failed, falling back to legacy: {e}")
        await _enrich_messages_with_file_urls_legacy(messages, user_jwt)


async def _enrich_messages_with_file_urls_legacy(messages: List[Dict[str, Any]], user_jwt: str) -> None:
    """Legacy enrichment using presigned URLs and per-file DB queries.

    Kept as fallback when image proxy is not configured.
    """
    if not messages:
        return

    has_file_blocks = any(
        isinstance(block, dict) and block.get("type") == "file"
        for msg in messages
        for block in (msg.get("blocks") or [])
        if isinstance(msg.get("blocks"), list)
    )
    if not has_file_blocks:
        return

    admin = await get_async_service_role_client()
    r2 = get_r2_client()

    channel_ctx_cache: Dict[str, Tuple[str, str]] = {}
    file_url_cache: Dict[str, str] = {}

    async def get_channel_ctx(channel_id: str) -> Tuple[str, str]:
        if channel_id in channel_ctx_cache:
            return channel_ctx_cache[channel_id]

        ctx_res = await (
            admin
            .table("channels")
            .select("id, workspace_app_id, app:workspace_apps(id, workspace_id)")
            .eq("id", channel_id)
            .limit(1)
            .execute()
        )
        if not ctx_res.data:
            raise Exception("Channel context not found")

        wa_id = ctx_res.data[0]["workspace_app_id"]
        ws_id = (ctx_res.data[0].get("app") or {}).get("workspace_id")
        if not ws_id:
            app_res = await (
                admin.table("workspace_apps").select("workspace_id").eq("id", wa_id).limit(1).execute()
            )
            ws_id = app_res.data[0]["workspace_id"] if app_res.data else None
        if not ws_id:
            raise Exception("Workspace context not found for channel")

        channel_ctx_cache[channel_id] = (ws_id, wa_id)
        return ws_id, wa_id

    for msg in messages:
        blocks = msg.get("blocks") or []
        if not isinstance(blocks, list) or not blocks:
            continue

        channel_id = msg.get("channel_id")
        if not channel_id:
            continue

        try:
            ws_id, wa_id = await get_channel_ctx(channel_id)
        except Exception:
            continue

        for block in blocks:
            try:
                if not isinstance(block, dict) or block.get("type") != "file":
                    continue
                data = block.get("data") or {}

                file_id = data.get("file_id") or data.get("id")
                r2_key = data.get("r2_key")
                url: Optional[str] = None

                if file_id:
                    if file_id in file_url_cache:
                        url = file_url_cache[file_id]
                    else:
                        f_res = await (
                            admin
                            .table("files")
                            .select("id, r2_key, workspace_id, workspace_app_id")
                            .eq("id", file_id)
                            .limit(1)
                            .execute()
                        )
                        if f_res.data:
                            f = f_res.data[0]
                            if f.get("workspace_id") == ws_id or f.get("workspace_app_id") == wa_id:
                                url = r2.get_presigned_url(f["r2_key"], expiration=3600)
                                file_url_cache[file_id] = url
                elif r2_key:
                    f2_res = await (
                        admin
                        .table("files")
                        .select("id, workspace_id, workspace_app_id")
                        .eq("r2_key", r2_key)
                        .limit(1)
                        .execute()
                    )
                    if f2_res.data:
                        f2 = f2_res.data[0]
                        if f2.get("workspace_id") == ws_id or f2.get("workspace_app_id") == wa_id:
                            url = r2.get_presigned_url(r2_key, expiration=3600)

                if url:
                    data["url"] = url
                    block["data"] = data
            except Exception as e:
                logger.debug(f"Skipping file URL enrichment due to error: {e}")
