"""
Files router - HTTP endpoints for file upload/download operations
"""
from fastapi import APIRouter, HTTPException, Request, status, Depends, UploadFile, File, Query, Response
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import logging
import time

from api.services.files import (
    upload_file,
    delete_file,
    get_file,
    get_presigned_url,
    list_files,
)
from api.dependencies import get_current_user_jwt, get_current_user_id
from api.exceptions import handle_api_exception
from api.config import settings
from api.rate_limit import limiter
from lib.filename_utils import sanitize_filename
from lib.r2_client import get_r2_client
from lib.image_proxy import generate_file_url, is_image_type, get_signed_url_expiration
from lib.supabase_client import get_authenticated_supabase_client
from lib.presigned_upload import (
    PresignedUploadManager,
    PresignedUploadError,
    InvalidMimeTypeError,
    FileSizeExceededError,
    FileNotFoundInStorageError,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/files", tags=["files"])


# ============================================================================
# Presigned Upload Models
# ============================================================================

class PresignedUploadRequest(BaseModel):
    """Request for presigned upload URL."""
    workspace_app_id: Optional[str] = Field(None, description="Workspace app ID (files app)")
    workspace_id: Optional[str] = Field(None, description="Workspace ID (for workspace-level uploads like icons)")
    filename: str = Field(..., min_length=1, max_length=255)
    content_type: str = Field(..., description="MIME type")
    file_size: int = Field(..., gt=0, le=52428800, description="Size in bytes (max 50MB)")
    parent_id: Optional[str] = Field(None, description="Parent folder ID")
    tags: Optional[List[str]] = Field(None, description="Tags to apply")
    create_document: bool = Field(False, description="Create document entry on confirm")


class PresignedUploadResponse(BaseModel):
    """Response with presigned URL."""
    file_id: str
    upload_url: str
    r2_key: str
    public_url: str
    expires_at: str
    headers: Dict[str, str]
    # Echo back for client convenience
    workspace_app_id: Optional[str] = None
    workspace_id: Optional[str] = None
    parent_id: Optional[str] = None
    tags: Optional[List[str]] = None
    create_document: bool = False


class ConfirmUploadRequest(BaseModel):
    """Request to confirm upload completion."""
    workspace_app_id: Optional[str] = Field(None, description="Workspace app ID (files app)")
    parent_id: Optional[str] = Field(None, description="Parent folder ID")
    tags: Optional[List[str]] = Field(None, description="Tags to apply")
    create_document: bool = Field(False, description="Create document entry")


class ConfirmUploadResponse(BaseModel):
    """Response from confirm upload."""
    file: Dict[str, Any]
    document: Optional[Dict[str, Any]] = None


class FileItemResponse(BaseModel):
    """Response model for a single file."""
    id: str
    user_id: Optional[str] = None
    filename: Optional[str] = None
    content_type: Optional[str] = None
    file_size: Optional[int] = None
    r2_key: Optional[str] = None
    public_url: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        extra = "allow"


class FileListResponse(BaseModel):
    """Response model for file list."""
    files: List[FileItemResponse]
    count: int


class FileUploadResponse(BaseModel):
    """Response model for legacy file upload."""
    file: FileItemResponse
    document: Optional[Dict[str, Any]] = None

    class Config:
        extra = "allow"


class FileURLResponse(BaseModel):
    """Response model for presigned download URL."""
    url: str
    expires_in: Optional[int] = None

    class Config:
        extra = "allow"


# ============================================================================
# Presigned Upload Endpoints
# ============================================================================

@router.post("/upload-url", response_model=PresignedUploadResponse)
@limiter.limit("30/minute;500/day")
async def get_presigned_upload_url(
    request: Request,
    response: Response,
    body: PresignedUploadRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Get a presigned URL for direct upload to R2.

    Flow:
    1. Client calls this with file metadata
    2. Server returns presigned PUT URL (valid 5 minutes)
    3. Client uploads directly to R2 using PUT
    4. Client calls POST /api/files/{file_id}/confirm

    The presigned URL bypasses the backend, allowing:
    - Larger files without timeout
    - Upload progress tracking
    - Reduced backend bandwidth
    """
    manager = PresignedUploadManager(
        r2_client=get_r2_client(),
        supabase_client=get_authenticated_supabase_client(user_jwt),
    )

    try:
        # Validate that at least one of workspace_app_id or workspace_id is provided
        if not body.workspace_app_id and not body.workspace_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Either workspace_app_id or workspace_id must be provided"
            )

        safe_filename = sanitize_filename(body.filename)

        result = manager.initiate_upload(
            user_id=user_id,
            workspace_app_id=body.workspace_app_id,
            workspace_id=body.workspace_id,
            filename=safe_filename,
            content_type=body.content_type,
            file_size=body.file_size,
            parent_id=body.parent_id,
            tags=body.tags,
            create_document=body.create_document,
        )

        logger.info(f"📤 Presigned upload URL generated for user {user_id}: {result.file_id}")

        return PresignedUploadResponse(
            file_id=result.file_id,
            upload_url=result.upload_url,
            r2_key=result.r2_key,
            public_url=result.public_url,
            expires_at=result.expires_at,
            headers=result.headers,
            # Echo back for client
            workspace_app_id=body.workspace_app_id,
            workspace_id=body.workspace_id,
            parent_id=body.parent_id,
            tags=body.tags,
            create_document=body.create_document,
        )

    except InvalidMimeTypeError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except FileSizeExceededError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        handle_api_exception(e, "Failed to generate upload URL", logger)


@router.post("/{file_id}/confirm", response_model=ConfirmUploadResponse)
async def confirm_upload(
    file_id: str,
    request: Optional[ConfirmUploadRequest] = None,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Confirm file was uploaded successfully to R2.

    This endpoint marks a pending upload as complete and optionally creates
    the corresponding document row.

    Call this after successfully uploading to the presigned URL.
    """
    # Use defaults if no request body provided
    if request is None:
        request = ConfirmUploadRequest(workspace_app_id=None, parent_id=None, tags=None, create_document=False)

    manager = PresignedUploadManager(
        r2_client=get_r2_client(),
        supabase_client=get_authenticated_supabase_client(user_jwt),
    )

    try:
        result = manager.confirm_upload(
            file_id=file_id,
            user_id=user_id,
            parent_id=request.parent_id,
            tags=request.tags,
            create_document=request.create_document,
        )

        logger.info(f"✅ Upload confirmed for file {file_id}")

        return ConfirmUploadResponse(
            file=result.file,
            document=result.document,
        )

    except FileNotFoundInStorageError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except PresignedUploadError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ValueError as e:
        detail = str(e)
        if "already confirmed" in detail.lower():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    except Exception as e:
        handle_api_exception(e, "Failed to confirm upload", logger)


# ============================================================================
# Existing File Upload Endpoints
# ============================================================================


@router.post("/upload", response_model=FileUploadResponse, status_code=status.HTTP_201_CREATED)
@limiter.limit("30/minute;500/day")
async def upload_file_endpoint(
    request: Request,
    response: Response,
    file: UploadFile = File(...),
    workspace_app_id: str = Query(..., description="Workspace app ID (files app)"),
    parent_id: Optional[str] = Query(None, description="Parent folder ID in documents tree"),
    create_document: bool = Query(True, description="Create a document entry for the file"),
    tags: Optional[List[str]] = Query(None, description="Tags to assign to the file (can be repeated)"),
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Upload a file to R2 storage.

    - Files are stored in Cloudflare R2
    - Metadata is stored in the database
    - Optionally creates a document entry for the file in the documents tree
    - Tags can be assigned during upload (e.g., ?tags=Work&tags=Important)

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        # Validate file size by reading in chunks (memory efficient)
        # This avoids loading the entire file into memory
        file_size = 0
        chunk_size = 65536  # 64KB chunks

        while chunk := await file.read(chunk_size):
            file_size += len(chunk)
            # Fail early if file exceeds max size
            if file_size > settings.r2_max_file_size:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=f"File size exceeds maximum allowed ({settings.r2_max_file_size} bytes)"
                )

        if file_size == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot upload empty file"
            )

        # Reset file position for upload
        await file.seek(0)

        safe_filename = sanitize_filename(file.filename or "unnamed")

        logger.info(f"📤 Uploading file '{safe_filename}' ({file_size} bytes) for user {user_id}")

        result = await upload_file(
            user_id=user_id,
            user_jwt=user_jwt,
            workspace_app_id=workspace_app_id,
            file_data=file.file,
            filename=safe_filename,
            content_type=file.content_type or "application/octet-stream",
            file_size=file_size,
            parent_id=parent_id,
            create_document=create_document,
            tags=tags,
        )

        logger.info(f"✅ File uploaded: {result['file']['id']}")
        return result

    except HTTPException:
        raise
    except ValueError as e:
        # R2 client validation errors
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to upload file", logger)


@router.get("", response_model=FileListResponse)
async def list_files_endpoint(
    response: Response,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
    workspace_app_id: Optional[str] = Query(None, description="Filter by workspace app ID"),
    file_type: Optional[str] = Query(None, description="Filter by MIME type (e.g., 'image/png' or 'image/' for all images)"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of files to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    List files for a user with optional filtering.

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📋 Listing files for user {user_id}")

        files = await list_files(
            user_id=user_id,
            user_jwt=user_jwt,
            workspace_id=workspace_id,
            workspace_app_id=workspace_app_id,
            file_type=file_type,
            limit=limit,
            offset=offset,
        )

        # Add cache headers for edge caching
        response.headers["Cache-Control"] = "s-maxage=60, stale-while-revalidate=300"

        logger.info(f"✅ Found {len(files)} files")
        return {"files": files, "count": len(files)}

    except Exception as e:
        handle_api_exception(e, "Failed to list files", logger)


@router.get("/{file_id}", response_model=FileItemResponse)
async def get_file_endpoint(
    file_id: str,
    response: Response,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Get file metadata by ID.

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📄 Getting file {file_id} for user {user_id}")

        file = await get_file(
            user_id=user_id,
            user_jwt=user_jwt,
            file_id=file_id,
        )

        # Add cache headers
        response.headers["Cache-Control"] = "s-maxage=300, stale-while-revalidate=600"

        logger.info(f"✅ Found file: {file_id}")
        return file

    except Exception as e:
        handle_api_exception(e, "Failed to get file", logger, check_not_found=True)


@router.get("/{file_id}/url", response_model=FileURLResponse)
async def get_file_url_endpoint(
    file_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
    expiration: int = Query(3600, ge=60, le=604800, description="URL expiration in seconds (1 min to 7 days)"),
):
    """
    Get a presigned URL for downloading a file.

    The URL is temporary and expires after the specified time.
    Default expiration is 1 hour (3600 seconds).

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"🔗 Getting download URL for file {file_id}")

        result = await get_presigned_url(
            user_id=user_id,
            user_jwt=user_jwt,
            file_id=file_id,
            expiration=expiration,
        )

        # If image proxy is configured, return HMAC URL for image files only.
        # Non-images keep presigned URLs (Worker may not support range/streaming).
        file_record = result.get("file", {})
        mime_type = file_record.get("file_type", "")
        r2_key = file_record.get("r2_key", "")

        if r2_key and is_image_type(mime_type) and settings.image_proxy_url and settings.image_proxy_secret:
            proxy_url = generate_file_url(r2_key, mime_type, "preview")
            if proxy_url:
                # Match proxy URL signing window/validity.
                expires_in = max(0, get_signed_url_expiration() - int(time.time()))
                logger.info(f"✅ Generated proxy URL for: {file_id}")
                return {"url": proxy_url, "expires_in": expires_in}

        logger.info(f"✅ Generated presigned URL for: {file_id}")
        return result

    except Exception as e:
        handle_api_exception(e, "Failed to get download URL", logger, check_not_found=True)


@router.delete("/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_file_endpoint(
    file_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Delete a file from R2 and the database.
    Also removes any document entries that reference this file.

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"🗑️ Deleting file {file_id} for user {user_id}")

        await delete_file(
            user_id=user_id,
            user_jwt=user_jwt,
            file_id=file_id,
        )

        logger.info(f"✅ Deleted file: {file_id}")
        return None

    except Exception as e:
        handle_api_exception(e, "Failed to delete file", logger, check_not_found=True)
