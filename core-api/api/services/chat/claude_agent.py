"""
Chat agent streaming response handler using Claude (Anthropic).

Streams responses via NDJSON events, handles tool calling with parallel
execution, and image attachments.
"""

from typing import List, Dict, AsyncGenerator, Optional, Any
import json
import logging
import uuid
import base64
import asyncio
import time
import anthropic
from anthropic import APIStatusError
from api.config import settings
from api.services.chat.events import (
    content_event, action_event, display_event, ping_event,
    done_event, sources_event, status_event,
    tool_call_start_event, tool_call_end_event, tool_exchange_event
)
from api.services.chat.prompts import build_system_prompt
from lib.tools import ToolRegistry, ToolContext
from lib.r2_client import get_r2_client

# Import definitions to register all tools
import lib.tools.definitions  # noqa: F401

logger = logging.getLogger(__name__)

# Limits
TOOL_EXECUTION_TIMEOUT_S = 30
MAX_TOOL_RESULT_CHARS = 4000
MAX_CONTEXT_CHARS = 100000
CLAUDE_MAX_RETRIES = 3
CLAUDE_RETRY_DELAY_S = 1.0

# Transient HTTP status codes worth retrying
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 529}


def _truncate_tool_result(result_str: str, max_chars: int = MAX_TOOL_RESULT_CHARS) -> str:
    """Truncate a tool result string to avoid blowing the context window."""
    if len(result_str) <= max_chars:
        return result_str
    return result_str[:max_chars] + "... [truncated]"


# Initialize Anthropic client
_anthropic_client = None


def get_anthropic_client() -> anthropic.AsyncAnthropic:
    """Get or create the async Anthropic client."""
    global _anthropic_client
    if _anthropic_client is None:
        api_key = settings.anthropic_api_key
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not set in configuration")
        _anthropic_client = anthropic.AsyncAnthropic(api_key=api_key)
    return _anthropic_client


def _fetch_image_sync(r2_key: str) -> Optional[bytes]:
    """Synchronous helper to fetch image from R2 (runs in thread pool)."""
    try:
        r2_client = get_r2_client()
        response = r2_client.s3_client.get_object(
            Bucket=r2_client.bucket_name,
            Key=r2_key
        )
        return response['Body'].read()
    except Exception as e:
        logger.error(f"Failed to fetch image from R2: {r2_key}, error: {e}")
        return None


async def _fetch_image_as_base64(r2_key: str) -> Optional[str]:
    """Fetch an image from R2 and return as base64 string."""
    try:
        t0 = time.time()
        image_data = await asyncio.to_thread(_fetch_image_sync, r2_key)
        t1 = time.time()
        if image_data is None:
            return None
        encoded = base64.b64encode(image_data).decode('utf-8')
        t2 = time.time()
        size_kb = len(image_data) / 1024
        logger.info(f"⏱️ [TIMING] R2 fetch: {(t1-t0)*1000:.0f}ms, base64 encode: {(t2-t1)*1000:.0f}ms, size: {size_kb:.0f}KB")
        return encoded
    except Exception as e:
        logger.error(f"Failed to fetch/encode image: {r2_key}, error: {e}")
        return None


async def _prepare_image_attachments(
    attachments: List[Dict[str, Any]]
) -> tuple[List[Dict[str, Any]], List[str]]:
    """
    Fetch image attachments from R2 and prepare Claude content blocks.

    Returns:
        Tuple of (claude_image_blocks, base64_images)
    """
    image_attachments = [
        att for att in attachments
        if att.get("mime_type", "").startswith("image/")
    ]

    if not image_attachments:
        return [], []

    # Fetch ALL images in parallel
    t_start = time.time()
    logger.info(f"⏱️ [TIMING] Starting to fetch {len(image_attachments)} images in parallel...")
    fetch_tasks = [_fetch_image_as_base64(att["r2_key"]) for att in image_attachments]
    base64_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    t_end = time.time()
    logger.info(f"⏱️ [TIMING] All images fetched + encoded in {(t_end-t_start)*1000:.0f}ms total")

    image_blocks = []
    valid_base64_images = []
    for att, base64_result in zip(image_attachments, base64_results):
        if isinstance(base64_result, Exception):
            logger.error(f"Failed to fetch image {att.get('filename', 'unknown')}: {base64_result}")
            continue
        if base64_result is None:
            continue

        valid_base64_images.append(base64_result)
        # Claude image format
        image_blocks.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": att["mime_type"],
                "data": base64_result,
            }
        })
        logger.info(f"Added image to Claude message: {att.get('filename', 'unknown')}")

    return image_blocks, valid_base64_images


def _convert_messages_to_claude(messages: List[Dict]) -> List[Dict]:
    """
    Convert message history from OpenAI format to Claude format.

    OpenAI: [{"role": "system", ...}, {"role": "user", ...}, {"role": "assistant", ...}, {"role": "tool", ...}]
    Claude: system is separate param. Tool results go in user messages.
    """
    claude_messages = []

    for msg in messages:
        role = msg.get("role", "")

        if role == "system":
            # System messages are handled separately in Claude
            continue

        elif role == "user":
            content = msg.get("content", "")
            # Handle multimodal content (already in list format from image prep)
            if isinstance(content, list):
                claude_messages.append({"role": "user", "content": content})
            else:
                claude_messages.append({"role": "user", "content": content})

        elif role == "assistant":
            # Check if this has tool calls
            tool_calls = msg.get("tool_calls", [])
            if tool_calls:
                content_blocks = []
                # Add any text content first
                if msg.get("content"):
                    content_blocks.append({"type": "text", "text": msg["content"]})
                # Add tool_use blocks
                for tc in tool_calls:
                    func = tc.get("function", {})
                    try:
                        tool_input = json.loads(func.get("arguments", "{}"))
                    except json.JSONDecodeError:
                        tool_input = {}
                    content_blocks.append({
                        "type": "tool_use",
                        "id": tc.get("id", ""),
                        "name": func.get("name", ""),
                        "input": tool_input,
                    })
                claude_messages.append({"role": "assistant", "content": content_blocks})
            else:
                content = msg.get("content", "")
                if content:
                    claude_messages.append({"role": "assistant", "content": content})

        elif role == "tool":
            # Claude expects tool results in a user message
            claude_messages.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": msg.get("tool_call_id", ""),
                    "content": msg.get("content", ""),
                }]
            })

    return claude_messages


async def get_user_connections(user_id: str, user_jwt: str) -> List[str]:
    """Get list of connected providers for a user."""
    try:
        from lib.supabase_client import get_authenticated_async_client
        supabase = await get_authenticated_async_client(user_jwt)
        result = await supabase.table("ext_connections") \
            .select("provider") \
            .eq("user_id", user_id) \
            .eq("is_active", True) \
            .execute()
        providers = list({row["provider"] for row in (result.data or [])})
        if providers:
            return providers
        # Fallback: if no connections found, return empty list
        return []
    except Exception as e:
        logger.warning(f"[CHAT] Failed to fetch ext_connections for user {user_id}: {e}")
        return []


async def stream_chat_response(
    messages: List[Dict],
    user_id: str,
    user_jwt: str,
    context: Optional[Dict[str, Any]] = None,
    user_timezone: str = "UTC",
    attachments: Optional[List[Dict[str, Any]]] = None,
    workspace_ids: Optional[List[str]] = None,
    is_disconnected: Optional[Any] = None,
) -> AsyncGenerator[str, None]:
    """
    Stream chat response from Claude, handling tool calls.
    Yields NDJSON event chunks.

    Args:
        messages: Conversation history
        user_id: User ID
        user_jwt: User's JWT token
        context: Optional context dict with 'emails' and/or 'documents' lists
        user_timezone: User's timezone identifier (e.g., "Europe/Oslo")
        attachments: Optional list of attachment dicts for images
        workspace_ids: Optional list of workspace IDs to scope tool results
        is_disconnected: Optional async callable that returns True if client disconnected
    """
    client = get_anthropic_client()

    # Default to all user's workspaces if none specified
    if not workspace_ids:
        try:
            from lib.supabase_client import get_authenticated_async_client
            supabase = await get_authenticated_async_client(user_jwt)
            ws_result = await supabase.table("workspace_members").select("workspace_id").eq("user_id", user_id).execute()
            workspace_ids = [row["workspace_id"] for row in (ws_result.data or [])]
            logger.info(f"[CHAT] Defaulting to all {len(workspace_ids)} workspaces for user {user_id}")
        except Exception as e:
            logger.warning(f"[CHAT] Failed to fetch default workspaces: {e}")

    # Build system prompt with user preferences, context, and timezone
    system_prompt = await build_system_prompt(user_id, user_jwt, context, user_timezone, workspace_ids)

    # Sliding window: limit by character count (conservative proxy for tokens)
    total_chars = 0
    recent_messages = []

    for msg in reversed(messages):
        msg_chars = len(json.dumps(msg))
        if total_chars + msg_chars > MAX_CONTEXT_CHARS:
            break
        recent_messages.insert(0, msg)
        total_chars += msg_chars

    # Handle image attachments
    image_blocks: List[Dict[str, Any]] = []

    if attachments and recent_messages:
        image_blocks, _ = await _prepare_image_attachments(attachments)

        if image_blocks:
            yield status_event("Processing images...")

    # Build the last user message with images
    if image_blocks:
        for i in range(len(recent_messages) - 1, -1, -1):
            if recent_messages[i].get("role") == "user":
                original_text = recent_messages[i].get("content", "")
                content_parts = image_blocks.copy()
                if original_text:
                    content_parts.append({"type": "text", "text": original_text})
                recent_messages[i] = {
                    "role": "user",
                    "content": content_parts
                }
                break

    # Convert to Claude message format
    claude_messages = _convert_messages_to_claude(recent_messages)

    # Get user's connected services for tool filtering
    ext_connections = await get_user_connections(user_id, user_jwt)
    tools = ToolRegistry.get_claude_tools(ext_connections)

    tool_context = ToolContext(
        user_id=user_id,
        user_jwt=user_jwt,
        user_timezone=user_timezone,
        ext_connections=ext_connections,
        workspace_ids=workspace_ids,
    )

    pending_actions = []
    # Track display/sources events to emit after LLM text (preserve interleaving order)
    queued_display_events: List[str] = []
    queued_sources_events: List[str] = []

    while True:
        # Check if client disconnected before starting a new Claude API call
        if is_disconnected and await is_disconnected():
            logger.info("[CHAT] Client disconnected, aborting stream")
            return

        t_api_start = time.time()
        has_images = any(
            isinstance(m.get("content"), list) and
            any(b.get("type") == "image" for b in m["content"] if isinstance(b, dict))
            for m in claude_messages
        )
        logger.info(f"⏱️ [TIMING] Starting Claude API call (model: claude-opus-4-6, has_images: {has_images})")

        # Stream from Claude with retry on transient errors
        collected_text = ""
        collected_tool_uses: List[Dict[str, Any]] = []
        current_block_type = None
        current_tool_id = ""
        current_tool_name = ""
        current_tool_json = ""
        first_token_logged = False

        last_error: Optional[Exception] = None
        for attempt in range(1, CLAUDE_MAX_RETRIES + 1):
            try:
                async with client.messages.stream(
                    model="claude-opus-4-6",
                    max_tokens=25000,
                    system=system_prompt,
                    messages=claude_messages,
                    tools=tools if tools else [],
                ) as stream:
                    async for event in stream:
                        if not first_token_logged and event.type == "content_block_delta":
                            t_first = time.time()
                            logger.info(f"⏱️ [TIMING] Claude time-to-first-token: {(t_first-t_api_start)*1000:.0f}ms")
                            first_token_logged = True

                        if event.type == "content_block_start":
                            block = event.content_block
                            current_block_type = block.type
                            if block.type == "tool_use":
                                current_tool_id = block.id
                                current_tool_name = block.name
                                current_tool_json = ""

                        elif event.type == "content_block_delta":
                            delta = event.delta
                            if delta.type == "text_delta":
                                collected_text += delta.text
                                yield content_event(delta.text)
                            elif delta.type == "input_json_delta":
                                current_tool_json += delta.partial_json

                        elif event.type == "content_block_stop":
                            if current_block_type == "tool_use":
                                try:
                                    tool_input = json.loads(current_tool_json) if current_tool_json else {}
                                except json.JSONDecodeError as e:
                                    logger.error(f"Failed to parse tool JSON for {current_tool_name}: {e}")
                                    tool_input = {}
                                collected_tool_uses.append({
                                    "id": current_tool_id,
                                    "name": current_tool_name,
                                    "input": tool_input,
                                })
                            current_block_type = None

                last_error = None
                break  # Success — exit retry loop

            except APIStatusError as e:
                last_error = e
                is_transient = e.status_code in _TRANSIENT_STATUS_CODES
                # Catch mid-stream overload errors (SSE error on 200 response)
                if not is_transient:
                    try:
                        body = e.body if hasattr(e, 'body') else {}
                        if isinstance(body, dict):
                            error_type = body.get('error', {}).get('type', '')
                            is_transient = error_type in ('overloaded_error', 'api_error')
                    except Exception:
                        pass
                if is_transient and attempt < CLAUDE_MAX_RETRIES:
                    delay = CLAUDE_RETRY_DELAY_S * attempt
                    logger.warning(f"[CHAT] Claude API error (status={e.status_code}, body={e.body}), retrying in {delay}s (attempt {attempt}/{CLAUDE_MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    # Reset state for retry
                    collected_text = ""
                    collected_tool_uses = []
                    continue
                raise  # Non-transient or final attempt — propagate

        if last_error:
            raise last_error

        # Flush queued events from previous tool iteration
        for ev in queued_display_events:
            yield ev
        queued_display_events = []

        for ev in queued_sources_events:
            yield ev
        queued_sources_events = []

        # Process tool calls
        if collected_tool_uses:
            # Build assistant message with all content blocks
            assistant_content = []
            if collected_text:
                assistant_content.append({"type": "text", "text": collected_text})
            for tu in collected_tool_uses:
                assistant_content.append({
                    "type": "tool_use",
                    "id": tu["id"],
                    "name": tu["name"],
                    "input": tu["input"],
                })
            claude_messages.append({"role": "assistant", "content": assistant_content})

            # Emit tool_call start events for all tools
            for tu in collected_tool_uses:
                yield tool_call_start_event(tu["name"], tu["input"])

            yield ping_event()

            # Execute all tools in parallel with timeout
            async def _timed_execute(tu: Dict) -> tuple[Any, int]:
                t0 = time.time()
                try:
                    result = await asyncio.wait_for(
                        ToolRegistry.execute(tu["name"], tu["input"], tool_context),
                        timeout=TOOL_EXECUTION_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    from lib.tools.base import ToolResult as TR
                    result = TR(status="error", data={"error": f"Tool '{tu['name']}' timed out after {TOOL_EXECUTION_TIMEOUT_S}s"})
                duration_ms = int((time.time() - t0) * 1000)
                return result, duration_ms

            tasks = [_timed_execute(tu) for tu in collected_tool_uses]
            timed_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results and build tool_result messages
            tool_result_blocks = []
            for tu, timed_result in zip(collected_tool_uses, timed_results):
                if isinstance(timed_result, Exception):
                    logger.error(f"Tool {tu['name']} raised exception: {timed_result}")
                    tool_result_str = json.dumps({"status": "error", "error": str(timed_result)})
                    tool_result_blocks.append({
                        "type": "tool_result",
                        "tool_use_id": tu["id"],
                        "content": _truncate_tool_result(tool_result_str),
                    })
                    yield tool_call_end_event(tu["name"], 0, "error")
                    continue

                result, duration_ms = timed_result
                tool_result_str = result.to_json_string()

                if result.status == "error":
                    logger.warning(f"Tool {tu['name']} returned error: {result.data}")
                    yield tool_call_end_event(tu["name"], duration_ms, "error")
                elif result.status == "staged":
                    action_id = str(uuid.uuid4()).upper()
                    pending_actions.append(action_event(
                        action_id=action_id,
                        action=result.data.get("action", ""),
                        data=result.data,
                        description=result.description or ""
                    ))
                    yield tool_call_end_event(tu["name"], duration_ms, "staged")
                elif result.status == "success":
                    if result.display_type and result.display_items:
                        queued_display_events.append(display_event(
                            display_type=result.display_type,
                            items=result.display_items,
                            total_count=result.display_total or len(result.display_items)
                        ))
                    if result.sources:
                        queued_sources_events.append(sources_event(result.sources))
                    yield tool_call_end_event(tu["name"], duration_ms, "success")

                # Truncate tool result before adding to context
                truncated_result_str = _truncate_tool_result(tool_result_str)

                tool_result_blocks.append({
                    "type": "tool_result",
                    "tool_use_id": tu["id"],
                    "content": truncated_result_str,
                })

                # Yield full tool exchange for persistence (not truncated)
                yield tool_exchange_event(tu["id"], tu["name"], tu["input"], tool_result_str)

            # Add all tool results in a single user message
            claude_messages.append({"role": "user", "content": tool_result_blocks})

            # Reset for next iteration
            collected_text = ""
            continue

        else:
            # No tool calls — stream is complete
            for action in pending_actions:
                yield action

            yield done_event()
            break
