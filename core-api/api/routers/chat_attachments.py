"""
Chat attachments router - HTTP endpoints for image uploads in chat messages.

Supports presigned URL uploads for images (JPEG, PNG) with client-side thumbnail generation.
"""
from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
import asyncio
import logging
import uuid

from api.dependencies import get_current_user_jwt, get_current_user_id
from api.config import settings
from lib.r2_client import get_r2_client
from lib.supabase_client import get_authenticated_async_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/chat/attachments", tags=["chat-attachments"])

# Allowed MIME types for chat attachments (images only for now)
CHAT_ATTACHMENT_ALLOWED_TYPES = {"image/jpeg", "image/png"}


# ============================================================================
# Request/Response Models
# ============================================================================

class PresignedURLInfo(BaseModel):
    """Presigned URL details for upload."""
    upload_url: str
    r2_key: str


class AttachmentUploadRequest(BaseModel):
    """Request for presigned upload URLs (original + thumbnail)."""
    conversation_id: str = Field(..., description="Conversation to attach to")
    filename: str = Field(..., min_length=1, max_length=255)
    content_type: str = Field(..., description="MIME type (image/jpeg or image/png)")
    file_size: int = Field(..., gt=0, description="Original file size in bytes")
    thumbnail_size: int = Field(..., gt=0, description="Thumbnail file size in bytes")
    width: Optional[int] = Field(None, gt=0, description="Image width in pixels")
    height: Optional[int] = Field(None, gt=0, description="Image height in pixels")

    @field_validator('content_type')
    @classmethod
    def validate_content_type(cls, v: str) -> str:
        if v not in CHAT_ATTACHMENT_ALLOWED_TYPES:
            raise ValueError(f"Unsupported content type. Allowed: {', '.join(CHAT_ATTACHMENT_ALLOWED_TYPES)}")
        return v

    @field_validator('file_size')
    @classmethod
    def validate_file_size(cls, v: int) -> int:
        if v > settings.chat_attachment_max_size:
            raise ValueError(f"File size exceeds maximum ({settings.chat_attachment_max_size} bytes)")
        return v


class AttachmentUploadResponse(BaseModel):
    """Response with presigned URLs for original and thumbnail."""
    attachment_id: str
    original: PresignedURLInfo
    thumbnail: PresignedURLInfo
    expires_at: str


class AttachmentMetadata(BaseModel):
    """Attachment metadata returned after confirmation."""
    id: str
    conversation_id: str
    filename: str
    mime_type: str
    file_size: int
    width: Optional[int] = None
    height: Optional[int] = None
    r2_key: str
    thumbnail_r2_key: Optional[str] = None
    status: str
    created_at: str


class AttachmentConfirmResponse(BaseModel):
    """Response after confirming upload."""
    attachment: AttachmentMetadata


class AttachmentURLResponse(BaseModel):
    """Response with presigned download URL."""
    url: str
    expires_in: int


# ============================================================================
# Helper Functions
# ============================================================================

def generate_chat_attachment_key(user_id: str, conversation_id: str, filename: str, is_thumbnail: bool = False) -> str:
    """
    Generate R2 key for chat attachment.
    Format: chat-attachments/{user_id}/{conversation_id}/{uuid}[_thumb].{ext}
    """
    import os
    ext = os.path.splitext(filename)[1].lower() if '.' in filename else '.jpg'
    unique_id = str(uuid.uuid4())
    suffix = "_thumb" if is_thumbnail else ""
    return f"chat-attachments/{user_id}/{conversation_id}/{unique_id}{suffix}{ext}"


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/upload-url", response_model=AttachmentUploadResponse)
async def get_presigned_upload_urls(
    request: AttachmentUploadRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Get presigned URLs for uploading image and thumbnail to R2.

    Flow:
    1. Client calls this with file metadata
    2. Server returns presigned PUT URLs for original + thumbnail
    3. Client uploads both directly to R2 using PUT
    4. Client calls POST /api/chat/attachments/{attachment_id}/confirm

    The presigned URL bypasses the backend for efficient uploads.
    """
    supabase = await get_authenticated_async_client(user_jwt)
    r2_client = get_r2_client()

    # Verify conversation exists and belongs to user
    conv_check = await supabase.table("conversations")\
        .select("id")\
        .eq("id", request.conversation_id)\
        .eq("user_id", user_id)\
        .execute()

    if not conv_check.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    # Check pending uploads count for this conversation (prevent abuse)
    pending_check = await supabase.table("chat_attachments")\
        .select("id", count="exact")\
        .eq("conversation_id", request.conversation_id)\
        .eq("status", "uploading")\
        .execute()

    pending_count = pending_check.count or 0
    if pending_count >= settings.chat_attachment_max_per_message * 2:  # Allow some buffer
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many pending uploads. Please complete or cancel existing uploads."
        )

    try:
        # Generate R2 keys
        original_key = generate_chat_attachment_key(user_id, request.conversation_id, request.filename, is_thumbnail=False)
        thumbnail_key = generate_chat_attachment_key(user_id, request.conversation_id, request.filename, is_thumbnail=True)

        # Generate presigned PUT URLs
        expiry = settings.chat_attachment_upload_expiry
        original_presigned = r2_client.generate_presigned_put_url(original_key, request.content_type, expiry)
        thumbnail_presigned = r2_client.generate_presigned_put_url(thumbnail_key, "image/jpeg", expiry)

        # Create attachment record with status='uploading'
        attachment_id = str(uuid.uuid4())
        insert_result = await supabase.table("chat_attachments")\
            .insert({
                "id": attachment_id,
                "user_id": user_id,
                "conversation_id": request.conversation_id,
                "filename": request.filename,
                "mime_type": request.content_type,
                "file_size": request.file_size,
                "width": request.width,
                "height": request.height,
                "r2_key": original_key,
                "thumbnail_r2_key": thumbnail_key,
                "status": "uploading",
            })\
            .execute()

        if not insert_result.data:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create attachment record"
            )

        logger.info(f"📤 Chat attachment upload initiated: {attachment_id} for user {user_id}")

        return AttachmentUploadResponse(
            attachment_id=attachment_id,
            original=PresignedURLInfo(
                upload_url=original_presigned["url"],
                r2_key=original_key,
            ),
            thumbnail=PresignedURLInfo(
                upload_url=thumbnail_presigned["url"],
                r2_key=thumbnail_key,
            ),
            expires_at=original_presigned["expires_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        error_str = str(e)
        logger.error(f"❌ Error generating chat attachment URLs: {error_str}")

        if 'JWT expired' in error_str or 'PGRST303' in error_str:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Your session has expired. Please sign in again."
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate upload URLs: {error_str}"
        )


@router.post("/{attachment_id}/confirm", response_model=AttachmentConfirmResponse)
async def confirm_attachment_upload(
    attachment_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Confirm image was uploaded successfully to R2.

    This endpoint:
    1. Verifies original file exists in R2
    2. Verifies thumbnail file exists in R2
    3. Updates status to 'uploaded'

    Call this after successfully uploading both files to their presigned URLs.
    """
    supabase = await get_authenticated_async_client(user_jwt)
    r2_client = get_r2_client()

    # Get attachment record
    attachment_result = await supabase.table("chat_attachments")\
        .select("*")\
        .eq("id", attachment_id)\
        .eq("user_id", user_id)\
        .execute()

    if not attachment_result.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Attachment not found"
        )

    attachment = attachment_result.data[0]

    if attachment["status"] == "uploaded":
        # Already confirmed, return current state
        return AttachmentConfirmResponse(
            attachment=AttachmentMetadata(
                id=attachment["id"],
                conversation_id=attachment["conversation_id"],
                filename=attachment["filename"],
                mime_type=attachment["mime_type"],
                file_size=attachment["file_size"],
                width=attachment.get("width"),
                height=attachment.get("height"),
                r2_key=attachment["r2_key"],
                thumbnail_r2_key=attachment.get("thumbnail_r2_key"),
                status=attachment["status"],
                created_at=attachment["created_at"],
            )
        )

    try:
        # Get actual file size from R2 (also verifies file exists)
        # We trust presigned URL uploads - if client says it uploaded, we just verify with metadata
        # This is 1 HEAD request instead of 3, saving ~200-400ms
        metadata = await asyncio.to_thread(r2_client.get_object_metadata, attachment["r2_key"])

        if not metadata:
            # Original file doesn't exist - mark as error
            await supabase.table("chat_attachments")\
                .update({"status": "error"})\
                .eq("id", attachment_id)\
                .execute()

            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Original file not found in storage. Please re-upload."
            )

        actual_size = metadata.get("content_length", attachment["file_size"])

        # Update status to uploaded
        update_result = await supabase.table("chat_attachments")\
            .update({
                "status": "uploaded",
                "file_size": actual_size,
            })\
            .eq("id", attachment_id)\
            .execute()

        updated_attachment = update_result.data[0]

        logger.info(f"✅ Chat attachment confirmed: {attachment_id}")

        return AttachmentConfirmResponse(
            attachment=AttachmentMetadata(
                id=updated_attachment["id"],
                conversation_id=updated_attachment["conversation_id"],
                filename=updated_attachment["filename"],
                mime_type=updated_attachment["mime_type"],
                file_size=updated_attachment["file_size"],
                width=updated_attachment.get("width"),
                height=updated_attachment.get("height"),
                r2_key=updated_attachment["r2_key"],
                thumbnail_r2_key=updated_attachment.get("thumbnail_r2_key"),
                status=updated_attachment["status"],
                created_at=updated_attachment["created_at"],
            )
        )

    except HTTPException:
        raise
    except Exception as e:
        error_str = str(e)
        logger.error(f"❌ Error confirming chat attachment: {error_str}")

        if 'JWT expired' in error_str or 'PGRST303' in error_str:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Your session has expired. Please sign in again."
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to confirm upload: {error_str}"
        )


@router.get("/{attachment_id}/url", response_model=AttachmentURLResponse)
async def get_attachment_download_url(
    attachment_id: str,
    thumbnail: bool = False,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Get a presigned URL for viewing an attachment.

    Args:
        attachment_id: The attachment ID
        thumbnail: If true, return URL for thumbnail instead of original

    The URL expires after 1 hour by default.
    """
    supabase = await get_authenticated_async_client(user_jwt)
    r2_client = get_r2_client()

    # Get attachment record
    attachment_result = await supabase.table("chat_attachments")\
        .select("r2_key, thumbnail_r2_key, status, user_id")\
        .eq("id", attachment_id)\
        .eq("user_id", user_id)\
        .execute()

    if not attachment_result.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Attachment not found"
        )

    attachment = attachment_result.data[0]

    if attachment["status"] != "uploaded":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Attachment not yet uploaded"
        )

    try:
        # Select appropriate R2 key
        r2_key = attachment["thumbnail_r2_key"] if thumbnail and attachment.get("thumbnail_r2_key") else attachment["r2_key"]

        expiry = settings.chat_attachment_download_expiry
        url = r2_client.get_presigned_url(r2_key, expiry)

        logger.info(f"🔗 Generated download URL for attachment {attachment_id} (thumbnail={thumbnail})")

        return AttachmentURLResponse(
            url=url,
            expires_in=expiry,
        )

    except Exception as e:
        error_str = str(e)
        logger.error(f"❌ Error getting attachment URL: {error_str}")

        if 'JWT expired' in error_str or 'PGRST303' in error_str:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Your session has expired. Please sign in again."
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get download URL: {error_str}"
        )


@router.delete("/{attachment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_attachment(
    attachment_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Delete an attachment and its files from R2.

    Used for:
    - Canceling an upload before sending
    - Removing an attachment from a staged message
    """
    supabase = await get_authenticated_async_client(user_jwt)
    r2_client = get_r2_client()

    # Get attachment record
    attachment_result = await supabase.table("chat_attachments")\
        .select("r2_key, thumbnail_r2_key, message_id")\
        .eq("id", attachment_id)\
        .eq("user_id", user_id)\
        .execute()

    if not attachment_result.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Attachment not found"
        )

    attachment = attachment_result.data[0]

    # Don't allow deleting if already attached to a message
    if attachment.get("message_id"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete attachment that is already part of a message"
        )

    try:
        # Delete from R2 (ignore if not found)
        try:
            await asyncio.to_thread(r2_client.delete_file, attachment["r2_key"])
        except Exception:
            pass

        if attachment.get("thumbnail_r2_key"):
            try:
                await asyncio.to_thread(r2_client.delete_file, attachment["thumbnail_r2_key"])
            except Exception:
                pass

        # Delete from database
        await supabase.table("chat_attachments")\
            .delete()\
            .eq("id", attachment_id)\
            .execute()

        logger.info(f"🗑️ Deleted chat attachment: {attachment_id}")
        return None

    except HTTPException:
        raise
    except Exception as e:
        error_str = str(e)
        logger.error(f"❌ Error deleting attachment: {error_str}")

        if 'JWT expired' in error_str or 'PGRST303' in error_str:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Your session has expired. Please sign in again."
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete attachment: {error_str}"
        )


@router.get("/conversation/{conversation_id}", response_model=List[AttachmentMetadata])
async def list_conversation_attachments(
    conversation_id: str,
    status_filter: Optional[str] = None,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    List all attachments for a conversation.

    Args:
        conversation_id: The conversation ID
        status_filter: Filter by status ('uploading', 'uploaded', 'error')

    Returns attachments ordered by created_at desc.
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Verify conversation belongs to user
    conv_check = await supabase.table("conversations")\
        .select("id")\
        .eq("id", conversation_id)\
        .eq("user_id", user_id)\
        .execute()

    if not conv_check.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    try:
        query = supabase.table("chat_attachments")\
            .select("*")\
            .eq("conversation_id", conversation_id)\
            .order("created_at", desc=True)

        if status_filter:
            query = query.eq("status", status_filter)

        result = await query.execute()

        return [
            AttachmentMetadata(
                id=att["id"],
                conversation_id=att["conversation_id"],
                filename=att["filename"],
                mime_type=att["mime_type"],
                file_size=att["file_size"],
                width=att.get("width"),
                height=att.get("height"),
                r2_key=att["r2_key"],
                thumbnail_r2_key=att.get("thumbnail_r2_key"),
                status=att["status"],
                created_at=att["created_at"],
            )
            for att in (result.data or [])
        ]

    except Exception as e:
        error_str = str(e)
        logger.error(f"❌ Error listing attachments: {error_str}")

        if 'JWT expired' in error_str or 'PGRST303' in error_str:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Your session has expired. Please sign in again."
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list attachments: {error_str}"
        )
