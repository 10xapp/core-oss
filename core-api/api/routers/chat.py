from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from zoneinfo import ZoneInfo
from uuid import UUID
from api.dependencies import get_current_user_id, get_current_user_jwt
from api.config import settings
from api.rate_limit import limiter
from lib.supabase_client import get_authenticated_async_client, get_async_service_role_client
from lib.r2_client import get_r2_client
from api.services.chat.claude_agent import stream_chat_response
from api.services.chat.events import error_event, done_event
from api.services.chat.content_builder import ContentBuilder, create_attachment_part
from api.services.chat.title_generator import generate_and_update_title
import json
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

router = APIRouter(prefix="/api/chat", tags=["chat"])
logger = logging.getLogger(__name__)

CHAT_STREAM_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "X-Accel-Buffering": "no",
}


# ============================================================================
# Response Models
# ============================================================================

class ConversationResponse(BaseModel):
    """Response model for a single conversation."""
    id: str
    user_id: str
    title: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        extra = "allow"


class ConversationDeleteResponse(BaseModel):
    """Response model for delete conversation."""
    status: str
    id: str


class MessageItemResponse(BaseModel):
    """Response model for a single chat message."""
    id: str
    conversation_id: str
    role: str
    content: Optional[str] = None
    content_parts: Optional[List[Dict[str, Any]]] = None
    created_at: Optional[str] = None

    class Config:
        extra = "allow"


class ActionExecuteResponse(BaseModel):
    """Response model for action execution."""
    success: bool
    action_id: str
    status: str


# ============================================================================
# Dependencies
# ============================================================================

async def get_current_user(
    user_id: str = Depends(get_current_user_id),
    jwt: str = Depends(get_current_user_jwt)
) -> Dict[str, str]:
    return {"id": user_id, "jwt": jwt}

class CreateConversationRequest(BaseModel):
    title: Optional[str] = "New Conversation"

class UpdateConversationRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=100)

class EmailContext(BaseModel):
    """Email context to include in chat"""
    external_id: str
    thread_id: Optional[str] = None
    subject: Optional[str] = None
    sender: Optional[str] = None  # 'from' is a reserved keyword
    to: Optional[str] = None
    snippet: Optional[str] = None
    body: Optional[str] = None
    received_at: Optional[str] = None

class DocumentContext(BaseModel):
    """Document context to include in chat"""
    id: str
    title: Optional[str] = None
    content: Optional[str] = None
    is_folder: Optional[bool] = False

class ChatContext(BaseModel):
    """Context to include in chat message"""
    emails: Optional[List[EmailContext]] = None
    documents: Optional[List[DocumentContext]] = None

class SendMessageRequest(BaseModel):
    content: str
    attachment_ids: Optional[List[str]] = None  # Image attachment IDs (max 3)
    timezone: Optional[str] = "UTC"  # User's timezone identifier (e.g., "Europe/Oslo")
    context: Optional[ChatContext] = None
    workspace_id: Optional[str] = None  # Deprecated: use workspace_ids
    workspace_ids: Optional[List[str]] = None  # Workspace IDs to scope tool results (defaults to all)

    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> str:
        """Validate timezone is a valid IANA timezone identifier."""
        if v is None:
            return "UTC"
        try:
            ZoneInfo(v)
            return v
        except Exception:
            # Invalid timezone, fall back to UTC
            return "UTC"

    @field_validator('attachment_ids')
    @classmethod
    def validate_attachment_ids(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate attachment_ids count."""
        if v and len(v) > settings.chat_attachment_max_per_message:
            raise ValueError(f"Maximum {settings.chat_attachment_max_per_message} attachments per message")
        return v

@router.get("/conversations", response_model=List[ConversationResponse])
async def list_conversations(
    user: Dict = Depends(get_current_user)
):
    """List all conversations for the current user"""
    supabase = await get_authenticated_async_client(user['jwt'])
    
    logger.info(f"💬 [CHAT SEARCH] User {user['id']} listing conversations")
    
    response = await supabase.table("conversations")\
        .select("*")\
        .eq("user_id", user['id'])\
        .order("updated_at", desc=True)\
        .execute()
    
    conversation_count = len(response.data) if response.data else 0
    logger.info(f"💬 [CHAT SEARCH] Found {conversation_count} conversations for user {user['id']}")
        
    return response.data

@router.post("/conversations", response_model=ConversationResponse)
async def create_conversation(
    request: CreateConversationRequest,
    user: Dict = Depends(get_current_user)
):
    """Create a new conversation"""
    supabase = await get_authenticated_async_client(user['jwt'])
    
    response = await supabase.table("conversations")\
        .insert({
            "user_id": user['id'],
            "title": request.title
        })\
        .execute()
        
    if not response.data:
        raise HTTPException(status_code=500, detail="Failed to create conversation")

    return response.data[0]

@router.patch("/conversations/{conversation_id}", response_model=ConversationResponse)
async def update_conversation(
    conversation_id: str,
    request: UpdateConversationRequest,
    user: Dict = Depends(get_current_user)
):
    """Update a conversation (rename)"""
    supabase = await get_authenticated_async_client(user['jwt'])

    # Atomic update with ownership check
    response = await supabase.table("conversations")\
        .update({"title": request.title})\
        .eq("id", conversation_id)\
        .eq("user_id", user['id'])\
        .execute()

    if not response.data:
        raise HTTPException(status_code=404, detail="Conversation not found")

    logger.info(f"💬 [CHAT] User {user['id']} renamed conversation {conversation_id} to '{request.title}'")
    return response.data[0]

@router.delete("/conversations/{conversation_id}", response_model=ConversationDeleteResponse)
async def delete_conversation(
    conversation_id: str,
    user: Dict = Depends(get_current_user)
):
    """Delete a conversation, all its messages, and clean up R2 attachments"""
    supabase = await get_authenticated_async_client(user['jwt'])
    r2_client = get_r2_client()

    # Verify ownership
    conv_check = await supabase.table("conversations")\
        .select("id")\
        .eq("id", conversation_id)\
        .eq("user_id", user['id'])\
        .execute()

    if not conv_check.data:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Get all attachment R2 keys for this conversation (before cascade delete)
    attachments_result = await supabase.table("chat_attachments")\
        .select("r2_key, thumbnail_r2_key")\
        .eq("conversation_id", conversation_id)\
        .execute()

    r2_keys_to_delete = []
    for att in (attachments_result.data or []):
        if att.get("r2_key"):
            r2_keys_to_delete.append(att["r2_key"])
        if att.get("thumbnail_r2_key"):
            r2_keys_to_delete.append(att["thumbnail_r2_key"])

    # Delete messages first (foreign key constraint)
    await supabase.table("messages")\
        .delete()\
        .eq("conversation_id", conversation_id)\
        .execute()

    # Delete conversation (cascades to chat_attachments)
    await supabase.table("conversations")\
        .delete()\
        .eq("id", conversation_id)\
        .execute()

    # Clean up R2 files (after DB delete to ensure consistency)
    for r2_key in r2_keys_to_delete:
        try:
            r2_client.delete_file(r2_key)
        except Exception as e:
            # Log but don't fail - orphaned files will be cleaned by cron
            logger.warning(f"Failed to delete R2 file {r2_key}: {e}")

    logger.info(f"💬 [CHAT] User {user['id']} deleted conversation {conversation_id}")
    return {"status": "deleted", "id": conversation_id}

@router.get("/conversations/{conversation_id}/messages", response_model=List[MessageItemResponse])
async def get_messages(
    conversation_id: str,
    user: Dict = Depends(get_current_user)
):
    """Get messages for a conversation"""
    supabase = await get_authenticated_async_client(user['jwt'])
    
    # Verify ownership
    conv_check = await supabase.table("conversations")\
        .select("id")\
        .eq("id", conversation_id)\
        .eq("user_id", user['id'])\
        .execute()
        
    if not conv_check.data:
        raise HTTPException(status_code=404, detail="Conversation not found")
    
    logger.info(f"💬 [CHAT SEARCH] User {user['id']} retrieving messages for conversation {conversation_id}")
        
    response = await supabase.table("messages")\
        .select("*")\
        .eq("conversation_id", conversation_id)\
        .order("created_at", desc=False)\
        .execute()
    
    message_count = len(response.data) if response.data else 0
    logger.info(f"💬 [CHAT SEARCH] Found {message_count} messages in conversation {conversation_id} for user {user['id']}")
        
    return response.data

@router.post("/conversations/{conversation_id}/messages", responses={
    200: {
        "description": "NDJSON streaming response with chat events",
        "content": {
            "application/x-ndjson": {
                "schema": {"type": "string", "description": "Newline-delimited JSON stream of chat events"}
            },
        },
    },
})
@limiter.limit("10/minute;500/day")
async def send_message(
    request: Request,
    response: Response,
    conversation_id: str,
    body: SendMessageRequest,
    user: Dict = Depends(get_current_user)
):
    """Send a message and get a streaming response"""
    supabase = await get_authenticated_async_client(user['jwt'])

    # Verify ownership
    conv_check = await supabase.table("conversations")\
        .select("id")\
        .eq("id", conversation_id)\
        .eq("user_id", user['id'])\
        .execute()

    if not conv_check.data:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Fetch and validate attachments if provided
    attachments = []
    if body.attachment_ids:
        att_result = await supabase.table("chat_attachments")\
            .select("*")\
            .in_("id", body.attachment_ids)\
            .eq("user_id", user['id'])\
            .eq("conversation_id", conversation_id)\
            .eq("status", "uploaded")\
            .execute()

        if not att_result.data or len(att_result.data) != len(body.attachment_ids):
            raise HTTPException(
                status_code=400,
                detail="One or more attachments not found or not uploaded"
            )

        attachments = att_result.data

    # Build user message content_parts (attachments first, then text)
    user_content_parts = []
    for att in attachments:
        user_content_parts.append(create_attachment_part(
            attachment_id=att["id"],
            filename=att["filename"],
            mime_type=att["mime_type"],
            file_size=att["file_size"],
            r2_key=att["r2_key"],
            thumbnail_r2_key=att.get("thumbnail_r2_key"),
            width=att.get("width"),
            height=att.get("height"),
        ))

    # Add text part if content provided
    if body.content:
        user_content_parts.append({
            "id": str(UUID(int=0).hex),  # Simple ID for text
            "type": "text",
            "data": {"content": body.content}
        })

    # Save user message with content_parts
    user_message_result = await supabase.table("messages")\
        .insert({
            "conversation_id": conversation_id,
            "role": "user",
            "content": body.content,
            "content_parts": user_content_parts if user_content_parts else None,
        })\
        .execute()

    user_message_id = user_message_result.data[0]["id"] if user_message_result.data else None

    # Link attachments to the message
    if attachments and user_message_id:
        for att in attachments:
            await supabase.table("chat_attachments")\
                .update({"message_id": user_message_id})\
                .eq("id", att["id"])\
                .execute()
        
    # Get conversation history for context
    history_response = await supabase.table("messages")\
        .select("role, content, content_parts")\
        .eq("conversation_id", conversation_id)\
        .order("created_at", desc=False)\
        .execute()
        
    history = history_response.data or []

    # Format history for Claude API (reconstruct tool_use/tool_result pairs from stored content_parts)
    formatted_history = []
    for msg in history:
        content_parts = msg.get("content_parts") or []
        text_content = msg["content"] or ""

        # Check if this assistant message has tool_call parts (new format)
        tool_calls = [p for p in content_parts if p.get("type") == "tool_call"]

        if msg["role"] == "assistant" and tool_calls:
            # Reconstruct Claude-format multi-block assistant message with tool_use blocks
            blocks = []

            # Add the text content block (if any)
            if text_content.strip():
                blocks.append({"type": "text", "text": text_content})

            # Add tool_use blocks
            for tc in tool_calls:
                tc_data = tc.get("data", {})
                blocks.append({
                    "type": "tool_use",
                    "id": tc_data.get("tool_use_id", ""),
                    "name": tc_data.get("name", ""),
                    "input": tc_data.get("args", {}),
                })

            formatted_history.append({"role": "assistant", "content": blocks})

            # Add corresponding tool_result messages (Claude requires them after tool_use)
            tool_result_blocks = []
            for tc in tool_calls:
                tc_data = tc.get("data", {})
                result_str = tc_data.get("result", "")
                # Truncate large results to avoid context overflow
                if len(result_str) > 4000:
                    result_str = result_str[:4000] + "... [truncated]"
                tool_result_blocks.append({
                    "type": "tool_result",
                    "tool_use_id": tc_data.get("tool_use_id", ""),
                    "content": result_str,
                })

            formatted_history.append({"role": "user", "content": tool_result_blocks})

        else:
            # Simple text message (user or assistant without tool calls)
            formatted_history.append({
                "role": msg["role"],
                "content": text_content
            })
        
    # Build context dict if provided
    context_dict = None
    if body.context:
        context_dict = {
            "emails": [email.model_dump() for email in body.context.emails] if body.context.emails else None,
            "documents": [doc.model_dump() for doc in body.context.documents] if body.context.documents else None
        }
    
    async def generate():
        """
        Stream NDJSON events to client while building content_parts for database.

        CRITICAL: Each chunk from agent is now an NDJSON line like:
            {"type": "content", "delta": "Hello"}\n
            {"type": "display", ...}\n  (emitted immediately, preserving order)
            {"type": "action", ...}\n
            {"type": "sources", ...}\n
            {"type": "done"}\n

        We must:
        1. Yield raw NDJSON to client immediately
        2. Use ContentBuilder to track content_parts with proper interleaving
        3. Dual-write: save both content (for search/fallback) and content_parts (source of truth)
        """
        content_builder = ContentBuilder()  # Tracks content parts with proper interleaving
        text_content = ""  # Also track full text for content column (search/fallback)
        sources = []  # Web search sources (accumulates across multiple searches)
        display_events = []  # Keep for legacy display_content column

        try:
            logger.info(f"[CHAT] User {user['id']} sending message in conversation {conversation_id} (timezone: {body.timezone}, attachments: {len(attachments)})")
            # Resolve workspace_ids: prefer new array field, fall back to legacy single field
            effective_workspace_ids = body.workspace_ids or ([body.workspace_id] if body.workspace_id else None)
            async for chunk in stream_chat_response(formatted_history, user['id'], user['jwt'], context_dict, body.timezone, attachments, effective_workspace_ids, is_disconnected=request.is_disconnected):
                # Parse NDJSON to build blocks with proper interleaving
                stripped_chunk = chunk.strip()
                if stripped_chunk:
                    try:
                        event = json.loads(stripped_chunk)
                        event_type = event.get("type")

                        if event_type == "content":
                            delta = event.get("delta", "")
                            text_content += delta
                            content_builder.append_text(delta)
                            # Yield content events immediately
                            yield chunk

                        elif event_type == "display":
                            # Add display content part (flushes text first to preserve order)
                            display_type = event.get("display_type")
                            items = event.get("items", [])
                            total_count = event.get("total_count", 0)
                            content_builder.add_display(display_type, items, total_count)
                            # Also track for legacy column
                            display_events.append({
                                "display_type": display_type,
                                "items": items,
                                "total_count": total_count
                            })
                            # Yield display events immediately
                            yield chunk

                        elif event_type == "action":
                            # Add action content part (flushes text first to preserve order)
                            # Use the ID from the event for consistency between stream and storage
                            content_builder.add_action(
                                action=event.get("action", ""),
                                data=event.get("data", {}),
                                description=event.get("description", ""),
                                action_id=event.get("id")  # Use pre-generated ID from stream
                            )
                            # Yield action events immediately
                            yield chunk

                        elif event_type == "sources":
                            # Add sources inline via content builder (flushes text first to preserve order)
                            new_sources = event.get("sources", [])
                            content_builder.add_sources(new_sources)
                            sources.extend(new_sources)  # Also track for debugging
                            # Yield sources events immediately
                            yield chunk

                        elif event_type == "tool_exchange":
                            # Persist tool call context (not streamed to client)
                            content_builder.add_tool_call(
                                tool_use_id=event.get("tool_use_id", ""),
                                name=event.get("name", ""),
                                args=event.get("args", {}),
                                result_json=event.get("result", ""),
                            )

                        elif event_type == "done":
                            # Don't yield the agent's done event - we'll send our own with message_id
                            pass

                        else:
                            # Pass through other events (ping, status, error, etc.)
                            yield chunk

                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse NDJSON chunk: {stripped_chunk[:100]}")
                        yield chunk  # Pass through unparseable chunks
                else:
                    yield chunk  # Pass through empty/whitespace chunks

            # Finalize content_parts (flushes remaining text; sources already added inline via add_sources)
            content_parts = content_builder.finalize()

            # Save assistant message after stream completes
            assistant_message_id = ""
            if text_content or content_parts:
                message_data = {
                    "conversation_id": conversation_id,
                    "role": "assistant",
                    "content": text_content,  # Keep for search/fallback
                    "content_parts": content_parts  # Source of truth with proper interleaving
                }

                result = await supabase.table("messages")\
                    .insert(message_data)\
                    .execute()

                # Extract the message ID for the done event
                if result.data and len(result.data) > 0:
                    assistant_message_id = result.data[0].get("id", "")

                # Touch conversation to trigger updated_at via DB trigger
                await supabase.table("conversations")\
                    .update({"id": conversation_id})\
                    .eq("id", conversation_id)\
                    .execute()

                # Auto-generate title after first exchange (fire-and-forget)
                # history has 1 entry = just the user message we saved = first exchange
                if len(history) == 1 and body.content:
                    asyncio.create_task(
                        generate_and_update_title(
                            conversation_id=conversation_id,
                            user_message=body.content,
                            supabase_client=supabase
                        )
                    )

            # Yield done event with message_id (for action persistence)
            yield done_event(assistant_message_id)

        except Exception as e:
            logger.error(f"Error in chat stream: {e}", exc_info=True)
            yield error_event(f"Error: {type(e).__name__}: {e}")
        finally:
            # Clean up async client to prevent connection pool leaks on stream cancel
            try:
                await supabase.aclose()
            except Exception:
                pass  # Ignore cleanup errors

    return StreamingResponse(generate(), media_type="application/x-ndjson", headers=CHAT_STREAM_HEADERS)


class RegenerateRequest(BaseModel):
    timezone: Optional[str] = "UTC"
    workspace_ids: Optional[List[str]] = None

    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> str:
        if v is None:
            return "UTC"
        try:
            ZoneInfo(v)
            return v
        except Exception:
            return "UTC"


@router.post("/conversations/{conversation_id}/messages/{message_id}/regenerate", responses={
    200: {
        "description": "NDJSON streaming response with regenerated chat events",
        "content": {"application/x-ndjson": {"schema": {"type": "string"}}},
    },
})
async def regenerate_message(
    conversation_id: str,
    message_id: str,
    body: RegenerateRequest,
    raw_request: Request,
    user: Dict = Depends(get_current_user)
):
    """Delete the assistant message and regenerate a new response from the preceding context."""
    supabase = await get_authenticated_async_client(user['jwt'])

    # Verify ownership
    conv_check = await supabase.table("conversations") \
        .select("id") \
        .eq("id", conversation_id) \
        .eq("user_id", user['id']) \
        .execute()
    if not conv_check.data:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Verify the target message exists and is an assistant message
    msg_check = await supabase.table("messages") \
        .select("id, role, created_at") \
        .eq("id", message_id) \
        .eq("conversation_id", conversation_id) \
        .execute()
    if not msg_check.data:
        raise HTTPException(status_code=404, detail="Message not found")
    if msg_check.data[0]["role"] != "assistant":
        raise HTTPException(status_code=400, detail="Can only regenerate assistant messages")

    target_created_at = msg_check.data[0]["created_at"]

    # Delete the target message and any messages after it
    await supabase.table("messages") \
        .delete() \
        .eq("conversation_id", conversation_id) \
        .gte("created_at", target_created_at) \
        .execute()

    # Get remaining conversation history
    history_response = await supabase.table("messages") \
        .select("role, content, content_parts") \
        .eq("conversation_id", conversation_id) \
        .order("created_at", desc=False) \
        .execute()

    history = history_response.data or []
    if not history:
        raise HTTPException(status_code=400, detail="No messages left to regenerate from")

    # Reconstruct formatted history (same logic as send_message)
    formatted_history = []
    for msg in history:
        content_parts = msg.get("content_parts") or []
        text_content = msg["content"] or ""
        tool_calls = [p for p in content_parts if p.get("type") == "tool_call"]

        if msg["role"] == "assistant" and tool_calls:
            blocks = []
            if text_content.strip():
                blocks.append({"type": "text", "text": text_content})
            for tc in tool_calls:
                tc_data = tc.get("data", {})
                blocks.append({
                    "type": "tool_use",
                    "id": tc_data.get("tool_use_id", ""),
                    "name": tc_data.get("name", ""),
                    "input": tc_data.get("args", {}),
                })
            formatted_history.append({"role": "assistant", "content": blocks})
            tool_result_blocks = []
            for tc in tool_calls:
                tc_data = tc.get("data", {})
                result_str = tc_data.get("result", "")
                if len(result_str) > 4000:
                    result_str = result_str[:4000] + "... [truncated]"
                tool_result_blocks.append({
                    "type": "tool_result",
                    "tool_use_id": tc_data.get("tool_use_id", ""),
                    "content": result_str,
                })
            formatted_history.append({"role": "user", "content": tool_result_blocks})
        else:
            formatted_history.append({"role": msg["role"], "content": text_content})

    effective_workspace_ids = body.workspace_ids

    async def generate():
        content_builder = ContentBuilder()
        text_content = ""
        sources = []
        display_events = []

        try:
            logger.info(f"[CHAT] User {user['id']} regenerating message in conversation {conversation_id}")
            async for chunk in stream_chat_response(formatted_history, user['id'], user['jwt'], None, body.timezone, None, None, effective_workspace_ids, is_disconnected=raw_request.is_disconnected):
                stripped_chunk = chunk.strip()
                if stripped_chunk:
                    try:
                        event = json.loads(stripped_chunk)
                        event_type = event.get("type")

                        if event_type == "content":
                            delta = event.get("delta", "")
                            text_content += delta
                            content_builder.append_text(delta)
                            yield chunk
                        elif event_type == "display":
                            content_builder.add_display(event.get("display_type"), event.get("items", []), event.get("total_count", 0))
                            display_events.append(event)
                            yield chunk
                        elif event_type == "action":
                            content_builder.add_action(
                                action=event.get("action", ""),
                                data=event.get("data", {}),
                                description=event.get("description", ""),
                                action_id=event.get("id")
                            )
                            yield chunk
                        elif event_type == "sources":
                            new_sources = event.get("sources", [])
                            content_builder.add_sources(new_sources)
                            sources.extend(new_sources)
                            yield chunk
                        elif event_type == "tool_exchange":
                            content_builder.add_tool_call(
                                tool_use_id=event.get("tool_use_id", ""),
                                name=event.get("name", ""),
                                args=event.get("args", {}),
                                result_json=event.get("result", ""),
                            )
                        elif event_type == "done":
                            pass  # We'll send our own done event
                        else:
                            yield chunk
                    except (json.JSONDecodeError, TypeError):
                        yield chunk
                else:
                    yield chunk

            content_parts = content_builder.finalize()

            assistant_message_id = ""
            if text_content or content_parts:
                result = await supabase.table("messages") \
                    .insert({
                        "conversation_id": conversation_id,
                        "role": "assistant",
                        "content": text_content,
                        "content_parts": content_parts,
                    }) \
                    .execute()
                if result.data and len(result.data) > 0:
                    assistant_message_id = result.data[0].get("id", "")

            yield done_event(assistant_message_id)

        except Exception as e:
            logger.error(f"Error in regenerate stream: {e}", exc_info=True)
            yield error_event(f"Error: {type(e).__name__}: {e}")
        finally:
            try:
                await supabase.aclose()
            except Exception:
                pass

    return StreamingResponse(generate(), media_type="application/x-ndjson", headers=CHAT_STREAM_HEADERS)


@router.patch("/messages/{message_id}/actions/{action_id}/execute", response_model=ActionExecuteResponse)
async def execute_action(
    message_id: str,
    action_id: str,
    user: Dict = Depends(get_current_user)
):
    """
    Execute a staged action and update its status in the message's content_parts.

    Dispatches to the appropriate service based on the action type:
    - create_calendar_event → creates event in DB + external calendar
    - send_email → sends email via Gmail/Microsoft
    - update_calendar_event → updates event in DB + external calendar
    - delete_calendar_event → deletes event from DB + external calendar
    - create_project_issue → creates a project card/issue
    """
    # Validate message_id is a valid UUID
    try:
        UUID(message_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid message ID format")

    # Use service role client since we manually verify ownership below
    # This bypasses RLS which may differ between prod/dev environments
    supabase = await get_async_service_role_client()

    # Verify message exists and belongs to user's conversation
    message_check = await supabase.table("messages")\
        .select("id, conversation_id")\
        .eq("id", message_id)\
        .execute()

    if not message_check.data:
        raise HTTPException(status_code=404, detail="Message not found")

    conversation_id = message_check.data[0]["conversation_id"]

    # Verify user owns the conversation
    conv_check = await supabase.table("conversations")\
        .select("id")\
        .eq("id", conversation_id)\
        .eq("user_id", user['id'])\
        .execute()

    if not conv_check.data:
        raise HTTPException(status_code=403, detail="Access denied")

    # Get current content_parts
    message_data = await supabase.table("messages")\
        .select("content_parts")\
        .eq("id", message_id)\
        .execute()

    if not message_data.data or not message_data.data[0].get("content_parts"):
        raise HTTPException(status_code=400, detail="Message has no content_parts")

    content_parts = message_data.data[0]["content_parts"]

    # Find the action part
    action_part = None
    for part in content_parts:
        if part.get("id") == action_id and part.get("type") == "action":
            action_part = part
            break

    if not action_part:
        raise HTTPException(status_code=404, detail="Action not found in message")

    action_data = action_part.get("data", {})
    action_type = action_data.get("action", "")
    # Tool args are nested in data.data (see content_builder.create_action_part)
    tool_args = action_data.get("data", action_data)

    # Execute the actual action
    result_data = None
    try:
        result_data = await _execute_action(action_type, tool_args, user['id'], user['jwt'])
        action_part["data"]["status"] = "executed"
        if result_data:
            action_part["data"]["result"] = result_data
        logger.info(f"✅ [ACTION] User {user['id']} executed {action_type} (action {action_id})")
    except Exception as e:
        action_part["data"]["status"] = "error"
        action_part["data"]["error"] = str(e)
        logger.error(f"❌ [ACTION] Failed to execute {action_type} for user {user['id']}: {e}")
        # Save the error status, then raise
        await supabase.table("messages")\
            .update({"content_parts": content_parts})\
            .eq("id", message_id)\
            .execute()
        raise HTTPException(status_code=500, detail=f"Action failed: {str(e)}")

    # Save updated content_parts
    await supabase.table("messages")\
        .update({"content_parts": content_parts})\
        .eq("id", message_id)\
        .execute()

    response = {"success": True, "action_id": action_id, "status": "executed"}
    if result_data:
        response["result"] = result_data
    return response


# Thread pool for running sync service functions (create_event, send_email, etc.)
_action_executor = ThreadPoolExecutor(max_workers=4)


async def _resolve_workspace_app(user_id: str, user_jwt: str, app_type: str, workspace_id: str = None):
    """Find a workspace app by type, using the given workspace or falling back to default."""
    from api.services.workspaces.crud import get_default_workspace
    from api.services.workspaces.apps import get_workspace_app_by_type

    ws_id = workspace_id
    if not ws_id:
        default_ws = await get_default_workspace(user_id, user_jwt)
        if not default_ws:
            raise ValueError("No default workspace found for user")
        ws_id = default_ws["id"]

    app = await get_workspace_app_by_type(ws_id, app_type, user_jwt)
    if not app:
        raise ValueError(f"No {app_type} app found in workspace")
    return app


async def _run_action_in_executor(callback):
    """Run a synchronous action service call in the shared executor."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_action_executor, callback)


async def _execute_create_calendar_event_action(data: Dict[str, Any], user_id: str, user_jwt: str):
    from api.services.calendar.create_event import create_event

    user_timezone = data.get("user_timezone")
    event_data = {
        "title": data.get("summary", ""),
        "start_time": data.get("start_time"),
        "end_time": data.get("end_time"),
        "description": data.get("description"),
        "location": data.get("location"),
        "attendees": data.get("attendees", []),
        "is_all_day": data.get("is_all_day", False),
    }
    result = await _run_action_in_executor(
        lambda: create_event(user_id, event_data, user_jwt, user_timezone=user_timezone)
    )
    return {
        "synced_to_external": result.get("synced_to_external", False),
        "provider": result.get("provider"),
        "sync_error": result.get("sync_error"),
        "account_email": result.get("event", {}).get("ext_connection_id"),
    }


async def _execute_send_email_action(data: Dict[str, Any], user_id: str, user_jwt: str):
    from api.services.email.send_email import send_email

    await _run_action_in_executor(
        lambda: send_email(
            user_id=user_id,
            user_jwt=user_jwt,
            to=data.get("to", ""),
            subject=data.get("subject", ""),
            body=data.get("body", ""),
        )
    )


async def _execute_update_calendar_event_action(data: Dict[str, Any], user_id: str, user_jwt: str):
    from api.services.calendar.update_event import update_event

    event_id = data.get("event_id")
    user_timezone = data.get("user_timezone")
    if not event_id:
        raise ValueError("event_id is required for update_calendar_event")

    event_data = {
        key: value for key, value in data.items()
        if key in ("summary", "start_time", "end_time", "description", "location")
        and value is not None
    }
    if "summary" in event_data:
        event_data["title"] = event_data.pop("summary")

    await _run_action_in_executor(
        lambda: update_event(event_id, event_data, user_id, user_jwt, user_timezone=user_timezone)
    )


async def _execute_delete_calendar_event_action(data: Dict[str, Any], user_id: str, user_jwt: str):
    from api.services.calendar.delete_event import delete_event

    event_id = data.get("event_id")
    if not event_id:
        raise ValueError("event_id is required for delete_calendar_event")

    await _run_action_in_executor(lambda: delete_event(event_id, user_id, user_jwt))


async def _execute_create_document_action(data: Dict[str, Any], user_id: str, user_jwt: str):
    from api.services.documents.create_document import create_document

    files_app = await _resolve_workspace_app(user_id, user_jwt, "files", data.get("workspace_id"))

    doc = await create_document(
        user_id=user_id,
        user_jwt=user_jwt,
        workspace_app_id=files_app["id"],
        title=data.get("title", "Untitled"),
        content=data.get("content", ""),
        parent_id=data.get("parent_id"),
    )
    return {"document_id": doc["id"], "workspace_id": doc.get("workspace_id")}


def _parse_action_priority(raw_value: Any) -> int | None:
    """Parse project issue priority and raise a user-facing validation error when invalid."""
    if raw_value is None:
        return None

    try:
        priority = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("priority must be an integer between 0 and 4") from exc

    if not 0 <= priority <= 4:
        raise ValueError("priority must be an integer between 0 and 4")

    return priority


def _parse_action_due_at(raw_value: Any) -> datetime | None:
    """Parse an optional due date in ISO 8601 format."""
    if raw_value is None:
        return None

    try:
        return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("due_at must be a valid ISO 8601 datetime") from exc


async def _execute_create_project_issue_action(data: Dict[str, Any], user_id: str, user_jwt: str):
    from api.services.projects import create_issue

    board_id = data.get("board_id")
    state_id = data.get("state_id")
    title = (data.get("title") or "").strip()

    if not board_id:
        raise ValueError("board_id is required for create_project_issue")
    if not state_id:
        raise ValueError("state_id is required for create_project_issue")
    if not title:
        raise ValueError("title is required for create_project_issue")

    priority = _parse_action_priority(data.get("priority"))
    due_at = _parse_action_due_at(data.get("due_at"))

    issue = await create_issue(
        user_id=user_id,
        user_jwt=user_jwt,
        board_id=board_id,
        state_id=state_id,
        title=title,
        description=data.get("description"),
        priority=priority if priority is not None else 0,
        due_at=due_at,
    )
    return {
        "issue_id": issue["id"],
        "board_id": issue.get("board_id"),
        "workspace_id": issue.get("workspace_id"),
        "number": issue.get("number"),
        "title": issue.get("title"),
    }


async def _execute_action(action_type: str, data: Dict[str, Any], user_id: str, user_jwt: str):
    """Dispatch a staged action to the appropriate service function."""
    handlers = {
        "create_calendar_event": _execute_create_calendar_event_action,
        "send_email": _execute_send_email_action,
        "update_calendar_event": _execute_update_calendar_event_action,
        "delete_calendar_event": _execute_delete_calendar_event_action,
        "create_document": _execute_create_document_action,
        "create_project_issue": _execute_create_project_issue_action,
    }

    handler = handlers.get(action_type)
    if not handler:
        raise ValueError(f"Unknown action type: {action_type}")

    return await handler(data, user_id, user_jwt)
