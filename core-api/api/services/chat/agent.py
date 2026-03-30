"""
Chat agent streaming response handler.

This module contains the main streaming logic for handling chat requests.
Uses the unified tool registry for tool definitions and execution.
"""

from typing import List, Dict, AsyncGenerator, Optional, Any
from enum import Enum
import json
import logging
import uuid
import base64
import asyncio
import time
from openai import AsyncOpenAI
from api.config import settings
from api.services.chat.events import (
    content_event, action_event, display_event, ping_event,
    done_event, sources_event, status_event
)
from api.services.chat.prompts import build_system_prompt
from lib.tools import ToolRegistry, ToolContext
from lib.r2_client import get_r2_client
from lib import ultramemory_client as memory

# Import definitions to register all tools
import lib.tools.definitions  # noqa: F401

logger = logging.getLogger(__name__)


class ContentPhase(str, Enum):
    """Phases of content generation for reasoning detection."""
    GROUNDED = "grounded"    # Before/during tool calls, may have sources
    RESULT = "result"        # Tool result display
    REASONING = "reasoning"  # After all tools complete, no more sources


class StreamState:
    """
    Tracks state during streaming for reasoning block detection.

    Text emitted after all tool calls complete (and after sources are emitted)
    is considered "reasoning" - synthesis and analysis rather than grounded facts.

    Also queues display/sources events for deferred emission to preserve
    correct interleaving order (text → widget, not widget → text).
    """

    def __init__(self) -> None:
        self.current_phase: ContentPhase = ContentPhase.GROUNDED
        self.has_emitted_sources: bool = False
        self.has_pending_sources: bool = False
        self.tool_calls_complete: bool = False
        # Queue for deferred events - emitted AFTER LLM text to preserve order
        self.queued_display_events: List[str] = []
        self.queued_sources_events: List[str] = []

    def mark_sources_emitted(self):
        """Called when sources are emitted."""
        self.has_emitted_sources = True
        self.has_pending_sources = False

    def mark_sources_pending(self):
        """Called when we know sources will be emitted."""
        self.has_pending_sources = True

    def transition_to_reasoning(self) -> bool:
        """
        Check if we should transition to reasoning phase.

        Returns True if transitioned, False if already in reasoning or can't transition.
        """
        if self.current_phase == ContentPhase.REASONING:
            return False

        # Transition when: no more tools AND (sources emitted OR no pending sources)
        if self.tool_calls_complete:
            if self.has_emitted_sources or not self.has_pending_sources:
                self.current_phase = ContentPhase.REASONING
                return True

        return False


# Initialize AsyncOpenAI client
openai_client = None


def get_openai_client() -> AsyncOpenAI:
    """Get or create the async OpenAI client."""
    global openai_client
    if openai_client is None:
        api_key = settings.openai_api_key
        if not api_key:
            raise ValueError("OPENAI_API_KEY not set in configuration")
        openai_client = AsyncOpenAI(api_key=api_key)
    return openai_client


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
    """
    Fetch an image from R2 and return as base64 string.

    Uses asyncio.to_thread() to avoid blocking the event loop.

    Args:
        r2_key: The R2 storage key

    Returns:
        Base64-encoded image string or None if fetch failed
    """
    try:
        # Run blocking boto3 call in thread pool
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


async def _add_images_to_last_message(
    messages: List[Dict],
    attachments: List[Dict[str, Any]]
) -> tuple[List[Dict], List[str]]:
    """
    Modify the last user message to include images for Vision API.

    The Vision API expects messages in this format:
    {
        "role": "user",
        "content": [
            {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,...", "detail": "auto"}},
            {"type": "text", "text": "What's in this image?"}
        ]
    }

    Args:
        messages: List of message dicts
        attachments: List of attachment dicts with r2_key and mime_type

    Returns:
        Tuple of (modified messages list, list of base64 encoded images)
    """
    # Find the last user message
    last_user_idx = None
    for i in range(len(messages) - 1, -1, -1):
        if messages[i].get("role") == "user":
            last_user_idx = i
            break

    if last_user_idx is None:
        return messages, []

    # Filter image attachments
    image_attachments = [
        att for att in attachments
        if att.get("mime_type", "").startswith("image/")
    ]

    if not image_attachments:
        return messages, []

    # Fetch ALL images in PARALLEL (saves 1-4s with multiple images)
    t_start = time.time()
    logger.info(f"⏱️ [TIMING] Starting to fetch {len(image_attachments)} images in parallel...")
    fetch_tasks = [
        _fetch_image_as_base64(att["r2_key"])
        for att in image_attachments
    ]
    base64_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    t_end = time.time()
    logger.info(f"⏱️ [TIMING] All images fetched + encoded in {(t_end-t_start)*1000:.0f}ms total")

    # Build content parts from fetched images
    content_parts = []
    valid_base64_images = []
    for att, base64_result in zip(image_attachments, base64_results):
        # Skip if fetch failed or returned exception
        if isinstance(base64_result, Exception):
            logger.error(f"Failed to fetch image {att.get('filename', 'unknown')}: {base64_result}")
            continue
        if base64_result is None:
            continue

        valid_base64_images.append(base64_result)
        content_parts.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:{att['mime_type']};base64,{base64_result}",
                "detail": "auto"  # Let OpenAI decide resolution
            }
        })
        logger.info(f"Added image to Vision API message: {att.get('filename', 'unknown')}")

    # Add the text content
    original_content = messages[last_user_idx].get("content", "")
    if original_content:
        content_parts.append({
            "type": "text",
            "text": original_content
        })

    # If we have images, update the message format
    if content_parts:
        messages = messages.copy()
        messages[last_user_idx] = {
            "role": "user",
            "content": content_parts
        }

    return messages, valid_base64_images


async def get_user_connections(user_id: str, user_jwt: str) -> List[str]:
    """
    Get list of connected providers for a user.

    Returns list like ["google"] based on active ext_connections.
    """
    # TODO: Query ext_connections table for user's active connections
    # For now, assume google is always connected (backward compat)
    return ["google"]


async def stream_chat_response(
    messages: List[Dict],
    user_id: str,
    user_jwt: str,
    context: Optional[Dict[str, Any]] = None,
    user_timezone: str = "UTC",
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> AsyncGenerator[str, None]:
    """
    Stream chat response from OpenAI, handling tool calls.
    Yields chunks of the response content.

    Args:
        messages: Conversation history
        user_id: User ID
        user_jwt: User's JWT token
        context: Optional context dict with 'emails' and/or 'documents' lists
        user_timezone: User's timezone identifier (e.g., "Europe/Oslo")
        attachments: Optional list of attachment dicts for Vision API (images)
    """
    client = get_openai_client()

    # Build system prompt with user preferences, context, and timezone
    system_prompt = await build_system_prompt(user_id, user_jwt, context, user_timezone)

    system_message = {
        "role": "system",
        "content": system_prompt
    }

    # Implement sliding window: limit by character count to avoid token limits
    MAX_CONTEXT_CHARS = 100000

    # Build context from most recent messages backwards until we hit the character limit
    total_chars = 0
    recent_messages = []

    for msg in reversed(messages):
        msg_chars = len(json.dumps(msg))
        if total_chars + msg_chars > MAX_CONTEXT_CHARS:
            break
        recent_messages.insert(0, msg)
        total_chars += msg_chars

    # If attachments present, modify the last user message for Vision API
    if attachments and recent_messages:
        recent_messages, _ = await _add_images_to_last_message(recent_messages, attachments)
        yield status_event("Processing images...")

    current_messages = [system_message] + recent_messages
    pending_actions = []

    # Get user's connected services for tool filtering
    ext_connections = await get_user_connections(user_id, user_jwt)

    # Get tools filtered by user's connections
    tools = ToolRegistry.get_openai_tools(ext_connections)

    # Create tool context for execution
    tool_context = ToolContext(
        user_id=user_id,
        user_jwt=user_jwt,
        user_timezone=user_timezone,
        ext_connections=ext_connections
    )

    # Stream state for phase tracking
    state = StreamState()

    while True:
        # Call OpenAI API with streaming for final response
        t_api_start = time.time()
        logger.info(f"⏱️ [TIMING] Starting OpenAI API call (model: gpt-5.1, has_images: {any(isinstance(m.get('content'), list) for m in current_messages)})")
        stream = await client.chat.completions.create(
            model="gpt-5.1",
            messages=current_messages,
            tools=tools if tools else None,
            tool_choice="auto" if tools else None,
            stream=True
        )

        # Collect the response and check for tool calls
        collected_messages = []
        collected_tool_calls = []
        first_chunk_logged = False

        async for chunk in stream:
            if not first_chunk_logged:
                t_first_token = time.time()
                logger.info(f"⏱️ [TIMING] OpenAI time-to-first-token: {(t_first_token-t_api_start)*1000:.0f}ms")
                first_chunk_logged = True
            delta = chunk.choices[0].delta if chunk.choices else None
            if not delta:
                continue

            # Collect tool calls if present
            if delta.tool_calls:
                for tool_call_chunk in delta.tool_calls:
                    # Extend the collected tool calls
                    if tool_call_chunk.index >= len(collected_tool_calls):
                        collected_tool_calls.append({
                            "id": tool_call_chunk.id or "",
                            "type": "function",
                            "function": {
                                "name": tool_call_chunk.function.name or "",
                                "arguments": tool_call_chunk.function.arguments or ""
                            }
                        })
                    else:
                        # Append to existing tool call
                        if tool_call_chunk.function.name:
                            collected_tool_calls[tool_call_chunk.index]["function"]["name"] += tool_call_chunk.function.name
                        if tool_call_chunk.function.arguments:
                            collected_tool_calls[tool_call_chunk.index]["function"]["arguments"] += tool_call_chunk.function.arguments

            # Yield content immediately as it arrives (wrapped in NDJSON event)
            if delta.content:
                collected_messages.append(delta.content)
                yield content_event(delta.content)

        # After streaming content, flush any queued events from previous tool call
        # This ensures text → widget order (LLM says "Here's your calendar" THEN widget appears)
        for event in state.queued_display_events:
            yield event
        state.queued_display_events = []

        for event in state.queued_sources_events:
            yield event
            state.mark_sources_emitted()
        state.queued_sources_events = []

        # Check if we have tool calls
        if collected_tool_calls:
            # SEQUENTIAL EXECUTION: Process only ONE tool per iteration
            # This gives the LLM a chance to discuss each tool result before moving to the next
            tc = collected_tool_calls[0]
            function_name = tc["function"]["name"]
            try:
                function_args = json.loads(tc["function"]["arguments"])
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse tool arguments for {function_name}: {e}")
                function_args = {}

            # Emit status indicator (dynamic for smart_search based on types)
            if function_name == "smart_search":
                types_str = function_args.get("types", "emails,calendar,todos,documents")
                types = [t.strip() for t in types_str.split(",")] if isinstance(types_str, str) else types_str
                type_names = {'emails': 'Emails', 'calendar': 'Calendar', 'todos': 'Tasks', 'documents': 'Notes'}
                names = [type_names.get(t, t.title()) for t in types[:2] if t in type_names]
                status_msg = ", ".join(names) + ("..." if len(types) > 2 else "") if names else "Searching..."
            else:
                status_msg = ToolRegistry.get_status_message(function_name) or f"Working on {function_name}..."
            yield status_event(status_msg)

            # Send ping to keep connection alive during tool execution
            yield ping_event()

            # Execute the tool using registry
            tool_result = await ToolRegistry.execute(function_name, function_args, tool_context)

            # Convert result to JSON string for LLM context
            tool_result_str = tool_result.to_json_string()

            # Handle tool result (display events, staged actions, sources)
            if tool_result.status == "error":
                # Log error internally, emit status to client
                logger.warning(f"Tool {function_name} returned error: {tool_result.data}")
                yield status_event(f"Unable to complete {function_name.replace('_', ' ')}")
            elif tool_result.status == "staged":
                # Generate ID upfront so streaming event and storage use the same ID
                action_id = str(uuid.uuid4()).upper()  # Uppercase for consistency with iOS UUIDs
                pending_actions.append(action_event(
                    action_id=action_id,
                    action=tool_result.data.get("action", ""),
                    data=tool_result.data,
                    description=tool_result.description or ""
                ))
            elif tool_result.status == "success":
                # QUEUE display events for emission AFTER next LLM text
                # This preserves correct interleaving order: text → widget
                if tool_result.display_type and tool_result.display_items:
                    state.queued_display_events.append(display_event(
                        display_type=tool_result.display_type,
                        items=tool_result.display_items,
                        total_count=tool_result.display_total or len(tool_result.display_items)
                    ))

                # QUEUE sources for emission AFTER next LLM text
                if tool_result.sources:
                    state.mark_sources_pending()
                    state.queued_sources_events.append(sources_event(tool_result.sources))

            # Add to conversation history (only this ONE tool call)
            current_messages.append({
                "role": "assistant",
                "content": None,
                "tool_calls": [tc]  # Only the one we processed
            })
            current_messages.append({
                "tool_call_id": tc["id"],
                "role": "tool",
                "name": function_name,
                "content": tool_result_str,
            })

            # Loop back - LLM will see this ONE result and respond about it
            # Then it can decide to call another tool or finish
            continue

        else:
            # No tool calls - mark tools complete and check for reasoning transition
            state.tool_calls_complete = True
            state.transition_to_reasoning()

            # Yield actions (staged items requiring confirmation)
            for action in pending_actions:
                yield action

            # ── Auto-ingest conversation into Ultramemory ──────────
            # Schedule before final yield so the task is created even
            # if the client disconnects after receiving done_event.
            if settings.ultramemory_url:
                try:
                    user_message = ""
                    for msg in reversed(messages):
                        if msg.get("role") == "user":
                            user_message = msg.get("content", "")
                            if isinstance(user_message, list):
                                # Vision API format — extract text parts
                                user_message = " ".join(
                                    p.get("text", "") for p in user_message
                                    if p.get("type") == "text"
                                )
                            break

                    full_response = "".join(collected_messages)
                    if user_message and full_response:
                        conversation_text = (
                            f"User: {user_message}\n"
                            f"Assistant: {full_response[:2000]}"
                        )

                        async def _safe_ingest():
                            try:
                                await memory.ingest(
                                    user_id=user_id,
                                    content=conversation_text,
                                    category="conversation",
                                )
                            except Exception as exc:
                                logger.warning(f"Memory auto-ingest background task failed: {exc}")

                        asyncio.create_task(_safe_ingest())
                except Exception as e:
                    logger.warning(f"Memory auto-ingest failed (non-fatal): {e}")

            # Signal stream completion
            yield done_event()

            break
