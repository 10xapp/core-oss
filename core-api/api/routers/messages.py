"""
Messages API router for workspace team messaging.

Provides endpoints for:
- Channel management (list, create, update, delete)
- Channel membership (for private channels)
- Message operations (list, create, update, delete)
- Reactions (add, remove)
- Thread replies
"""

from fastapi import APIRouter, HTTPException, Depends, Query, Request, Response
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import logging

from api.dependencies import get_current_user_id, get_current_user_jwt
from api.rate_limit import limiter
from api.services.messages import (
    # Channels
    get_channels,
    get_channel,
    create_channel,
    update_channel,
    delete_channel,
    add_channel_member,
    remove_channel_member,
    get_channel_members,
    # DMs
    get_or_create_dm,
    get_user_dms,
    # Unread
    get_unread_counts,
    mark_channel_read,
    # Messages
    get_messages,
    get_message,
    create_message,
    update_message,
    delete_message,
    add_reaction,
    remove_reaction,
    get_thread_replies,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["messages"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class CreateChannelRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    description: Optional[str] = Field(None, max_length=500)
    is_private: bool = False


class UpdateChannelRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=80)
    description: Optional[str] = Field(None, max_length=500)


class AddChannelMemberRequest(BaseModel):
    user_id: str
    role: str = "member"


class ContentBlock(BaseModel):
    """A content block in a message."""
    type: str  # text, mention, file, link_preview, code, quote, embed
    data: Dict[str, Any]


class CreateMessageRequest(BaseModel):
    blocks: List[ContentBlock]
    thread_parent_id: Optional[str] = None


class UpdateMessageRequest(BaseModel):
    blocks: List[ContentBlock]


class AddReactionRequest(BaseModel):
    emoji: str = Field(..., min_length=1, max_length=32)


class CreateDMRequest(BaseModel):
    participant_ids: List[str] = Field(..., min_length=1, description="User IDs to start DM with (excluding current user)")


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class ChannelResponse(BaseModel):
    """A channel object."""
    id: str
    name: Optional[str] = None
    description: Optional[str] = None
    is_private: Optional[bool] = None
    is_dm: Optional[bool] = None
    workspace_app_id: Optional[str] = None
    created_by: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        extra = "allow"


class ChannelListResponse(BaseModel):
    """Response for listing channels."""
    channels: List[ChannelResponse]
    count: int


class SingleChannelResponse(BaseModel):
    """Response for single channel operations."""
    channel: ChannelResponse


class ChannelMemberResponse(BaseModel):
    """A channel member object."""
    id: Optional[str] = None
    user_id: str
    channel_id: Optional[str] = None
    role: Optional[str] = None
    joined_at: Optional[str] = None

    class Config:
        extra = "allow"


class ChannelMemberListResponse(BaseModel):
    """Response for listing channel members."""
    members: List[ChannelMemberResponse]
    count: int


class SingleChannelMemberResponse(BaseModel):
    """Response for single member operations."""
    member: ChannelMemberResponse


class DMResponse(BaseModel):
    """A DM channel object."""
    id: str
    name: Optional[str] = None
    is_dm: Optional[bool] = True
    workspace_app_id: Optional[str] = None
    created_at: Optional[str] = None
    participants: Optional[List[Dict[str, Any]]] = None

    class Config:
        extra = "allow"


class DMListResponse(BaseModel):
    """Response for listing DMs."""
    dms: List[DMResponse]
    count: int


class SingleDMResponse(BaseModel):
    """Response for single DM operations."""
    dm: DMResponse


class UnreadCountsResponse(BaseModel):
    """Response for unread counts."""
    unread_counts: Dict[str, int]


class MessageResponse(BaseModel):
    """A message object."""
    id: str
    channel_id: Optional[str] = None
    user_id: Optional[str] = None
    blocks: Optional[List[Dict[str, Any]]] = None
    thread_parent_id: Optional[str] = None
    reply_count: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        extra = "allow"


class MessageListResponse(BaseModel):
    """Response for listing messages."""
    messages: List[MessageResponse]
    count: int
    has_more: bool = False


class SingleMessageResponse(BaseModel):
    """Response for single message operations."""
    message: MessageResponse


class ThreadRepliesResponse(BaseModel):
    """Response for thread replies."""
    replies: List[MessageResponse]
    count: int


class ReactionResponse(BaseModel):
    """A reaction object."""
    id: Optional[str] = None
    message_id: Optional[str] = None
    user_id: Optional[str] = None
    emoji: str
    created_at: Optional[str] = None

    class Config:
        extra = "allow"


class SingleReactionResponse(BaseModel):
    """Response for reaction operations."""
    reaction: ReactionResponse


class SuccessResponse(BaseModel):
    """Simple success response."""
    success: bool


# =============================================================================
# CHANNEL ENDPOINTS
# =============================================================================

@router.get("/workspaces/apps/{workspace_app_id}/channels", response_model=ChannelListResponse)
async def list_channels(
    workspace_app_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get all channels in a workspace app."""
    try:
        channels = await get_channels(workspace_app_id, user_jwt)
        return {"channels": channels, "count": len(channels)}
    except Exception as e:
        logger.error(f"Error listing channels: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/workspaces/apps/{workspace_app_id}/channels", response_model=SingleChannelResponse)
@limiter.limit("5/minute;50/day")
async def create_new_channel(
    request: Request,
    response: Response,
    workspace_app_id: str,
    body: CreateChannelRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Create a new channel in a workspace app."""
    try:
        channel = await create_channel(
            workspace_app_id=workspace_app_id,
            user_id=user_id,
            user_jwt=user_jwt,
            name=body.name,
            description=body.description,
            is_private=body.is_private,
        )
        return {"channel": channel}
    except Exception as e:
        logger.error(f"Error creating channel: {e}")
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail="Channel name already exists")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/channels/{channel_id}", response_model=SingleChannelResponse)
async def get_channel_by_id(
    channel_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get a channel by ID."""
    try:
        channel = await get_channel(channel_id, user_jwt)
        if not channel:
            raise HTTPException(status_code=404, detail="Channel not found")
        return {"channel": channel}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting channel: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/channels/{channel_id}", response_model=SingleChannelResponse)
async def update_channel_by_id(
    channel_id: str,
    request: UpdateChannelRequest,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Update a channel."""
    try:
        channel = await update_channel(
            channel_id=channel_id,
            user_jwt=user_jwt,
            name=request.name,
            description=request.description,
        )
        return {"channel": channel}
    except Exception as e:
        logger.error(f"Error updating channel: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/channels/{channel_id}", response_model=SuccessResponse)
async def delete_channel_by_id(
    channel_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Delete a channel."""
    try:
        await delete_channel(channel_id, user_jwt)
        return {"success": True}
    except Exception as e:
        logger.error(f"Error deleting channel: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# CHANNEL MEMBERS ENDPOINTS
# =============================================================================

@router.get("/channels/{channel_id}/members", response_model=ChannelMemberListResponse)
async def list_channel_members(
    channel_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get members of a channel."""
    try:
        members = await get_channel_members(channel_id, user_jwt)
        return {"members": members, "count": len(members)}
    except Exception as e:
        logger.error(f"Error listing channel members: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/channels/{channel_id}/members", response_model=SingleChannelMemberResponse)
async def add_member_to_channel(
    channel_id: str,
    request: AddChannelMemberRequest,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Add a member to a private channel."""
    try:
        member = await add_channel_member(
            channel_id=channel_id,
            member_user_id=request.user_id,
            user_jwt=user_jwt,
            role=request.role,
        )
        return {"member": member}
    except ValueError as e:
        detail = str(e)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail)
    except Exception as e:
        logger.error(f"Error adding channel member: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/channels/{channel_id}/members/{user_id}", response_model=SuccessResponse)
async def remove_member_from_channel(
    channel_id: str,
    user_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Remove a member from a private channel."""
    try:
        await remove_channel_member(channel_id, user_id, user_jwt)
        return {"success": True}
    except Exception as e:
        logger.error(f"Error removing channel member: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# DIRECT MESSAGE (DM) ENDPOINTS
# =============================================================================

@router.get("/workspaces/apps/{workspace_app_id}/dms", response_model=DMListResponse)
async def list_user_dms(
    workspace_app_id: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get all DM channels for the current user in a workspace app."""
    try:
        dms = await get_user_dms(workspace_app_id, user_id, user_jwt)
        return {"dms": dms, "count": len(dms)}
    except Exception as e:
        logger.error(f"Error listing DMs: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/workspaces/apps/{workspace_app_id}/dms", response_model=SingleDMResponse)
@limiter.limit("10/minute;100/day")
async def create_or_get_dm(
    request: Request,
    response: Response,
    workspace_app_id: str,
    body: CreateDMRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get or create a DM channel with specified participants."""
    try:
        dm = await get_or_create_dm(
            workspace_app_id=workspace_app_id,
            user_id=user_id,
            user_jwt=user_jwt,
            participant_ids=body.participant_ids,
        )
        return {"dm": dm}
    except Exception as e:
        logger.error(f"Error creating/getting DM: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# UNREAD INDICATORS ENDPOINTS
# =============================================================================

@router.get("/workspaces/apps/{workspace_app_id}/unread-counts", response_model=UnreadCountsResponse)
async def get_workspace_unread_counts(
    workspace_app_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get unread message counts for all channels in a workspace app."""
    try:
        counts = await get_unread_counts(workspace_app_id, user_jwt)
        return {"unread_counts": counts}
    except Exception as e:
        logger.error(f"Error getting unread counts: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/channels/{channel_id}/read", response_model=SuccessResponse)
async def mark_channel_as_read(
    channel_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Mark a channel as read for the current user."""
    try:
        await mark_channel_read(channel_id, user_jwt)
        return {"success": True}
    except Exception as e:
        logger.error(f"Error marking channel read: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# MESSAGE ENDPOINTS
# =============================================================================

@router.get("/channels/{channel_id}/messages", response_model=MessageListResponse)
async def list_messages(
    channel_id: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    before_id: Optional[str] = None,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get messages from a channel."""
    try:
        messages = await get_messages(
            channel_id=channel_id,
            user_jwt=user_jwt,
            limit=limit + 1,
            offset=offset,
            before_id=before_id,
        )
        has_more = len(messages) > limit
        if has_more:
            messages = messages[1:]  # Drop oldest; keep `limit` most recent
        return {"messages": messages, "count": len(messages), "has_more": has_more}
    except Exception as e:
        logger.error(f"Error listing messages: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/channels/{channel_id}/messages", response_model=SingleMessageResponse)
@limiter.limit("30/minute;1000/day")
async def send_message(
    request: Request,
    response: Response,
    channel_id: str,
    body: CreateMessageRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Send a message to a channel."""
    try:
        # Convert Pydantic models to dicts
        blocks = [block.model_dump() for block in body.blocks]

        message = await create_message(
            channel_id=channel_id,
            user_id=user_id,
            user_jwt=user_jwt,
            blocks=blocks,
            thread_parent_id=body.thread_parent_id,
        )
        return {"message": message}
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/messages/{message_id}", response_model=SingleMessageResponse)
async def get_message_by_id(
    message_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get a message by ID."""
    try:
        message = await get_message(message_id, user_jwt)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
        return {"message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting message: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/messages/{message_id}", response_model=SingleMessageResponse)
async def edit_message(
    message_id: str,
    request: UpdateMessageRequest,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Edit a message."""
    try:
        blocks = [block.model_dump() for block in request.blocks]
        message = await update_message(message_id, user_jwt, blocks)
        return {"message": message}
    except Exception as e:
        logger.error(f"Error editing message: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/messages/{message_id}", response_model=SuccessResponse)
async def delete_message_by_id(
    message_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Delete a message."""
    try:
        await delete_message(message_id, user_jwt)
        return {"success": True}
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# THREAD ENDPOINTS
# =============================================================================

@router.get("/messages/{message_id}/replies", response_model=ThreadRepliesResponse)
async def list_thread_replies(
    message_id: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Get replies to a message thread."""
    try:
        replies = await get_thread_replies(
            parent_message_id=message_id,
            user_jwt=user_jwt,
            limit=limit,
            offset=offset,
        )
        return {"replies": replies, "count": len(replies)}
    except Exception as e:
        logger.error(f"Error listing thread replies: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# REACTION ENDPOINTS
# =============================================================================

@router.post("/messages/{message_id}/reactions", response_model=SingleReactionResponse)
@limiter.limit("60/minute")
async def add_reaction_to_message(
    request: Request,
    response: Response,
    message_id: str,
    body: AddReactionRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Add a reaction to a message."""
    try:
        reaction = await add_reaction(
            message_id=message_id,
            user_id=user_id,
            user_jwt=user_jwt,
            emoji=body.emoji,
        )
        return {"reaction": reaction}
    except Exception as e:
        logger.error(f"Error adding reaction: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/messages/{message_id}/reactions/{emoji}", response_model=SuccessResponse)
async def remove_reaction_from_message(
    message_id: str,
    emoji: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Remove a reaction from a message."""
    try:
        await remove_reaction(message_id, user_id, user_jwt, emoji)
        return {"success": True}
    except Exception as e:
        logger.error(f"Error removing reaction: {e}")
        raise HTTPException(status_code=400, detail=str(e))
