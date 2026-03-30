"""Permissions router - sharing and access requests."""
from __future__ import annotations

from typing import List, Dict, Any, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, EmailStr, Field, field_validator

from api.config import settings
from api.dependencies import get_current_user_id, get_current_user_jwt
from api.exceptions import handle_api_exception
from api.request_types import AccessRequestStatus, PermissionLevel, ResourceType
from api.rate_limit import limiter
from api.services.permissions import (
    share_resource,
    batch_share_resource,
    revoke_share,
    update_share,
    get_resource_shares,
    get_shared_with_me,
    create_share_link,
    revoke_share_link,
    get_resource_links,
    resolve_share_link,
    update_share_link_slug,
    check_share_link_slug_availability,
    create_access_request,
    resolve_access_request,
    list_pending_access_requests,
)
from api.services.users import search_users_by_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["permissions"])


# ==========================================================================
# Request/Response Models
# ==========================================================================

class ShareResourceRequest(BaseModel):
    resource_type: ResourceType
    resource_id: str
    grantee_email: EmailStr
    permission: PermissionLevel = Field(default=PermissionLevel.READ, description="read|write|admin")

    @field_validator("resource_type", "permission", mode="before")
    @classmethod
    def normalize_values(cls, value: ResourceType | PermissionLevel | str) -> ResourceType | PermissionLevel | str:
        return value.strip().lower() if isinstance(value, str) else value


class BatchShareGrant(BaseModel):
    email: EmailStr
    permission: PermissionLevel = Field(default=PermissionLevel.READ, description="read|write|admin")

    @field_validator("permission", mode="before")
    @classmethod
    def normalize_permission(cls, value: PermissionLevel | str) -> PermissionLevel | str:
        return value.strip().lower() if isinstance(value, str) else value


class BatchShareRequest(BaseModel):
    resource_type: ResourceType
    resource_id: str
    grants: List[BatchShareGrant]

    @field_validator("resource_type", mode="before")
    @classmethod
    def normalize_resource_type(cls, value: ResourceType | str) -> ResourceType | str:
        return value.strip().lower() if isinstance(value, str) else value


class UpdateShareRequest(BaseModel):
    permission: PermissionLevel

    @field_validator("permission", mode="before")
    @classmethod
    def normalize_permission(cls, value: PermissionLevel | str) -> PermissionLevel | str:
        return value.strip().lower() if isinstance(value, str) else value


class AccessRequestCreate(BaseModel):
    resource_type: ResourceType
    resource_id: str
    message: Optional[str] = None

    @field_validator("resource_type", mode="before")
    @classmethod
    def normalize_resource_type(cls, value: ResourceType | str) -> ResourceType | str:
        return value.strip().lower() if isinstance(value, str) else value


class AccessRequestResolve(BaseModel):
    status: AccessRequestStatus
    permission: Optional[PermissionLevel] = PermissionLevel.READ

    @field_validator("status", "permission", mode="before")
    @classmethod
    def normalize_values(cls, value: AccessRequestStatus | PermissionLevel | str | None) -> AccessRequestStatus | PermissionLevel | str | None:
        if isinstance(value, str):
            return value.strip().lower()
        return value


class PermissionItemResponse(BaseModel):
    id: str
    resource_type: str
    resource_id: str
    workspace_id: Optional[str] = None
    grantee_type: str
    grantee_id: Optional[str] = None
    permission: str
    link_token: Optional[str] = None
    granted_by: Optional[str] = None
    created_at: Optional[str] = None
    expires_at: Optional[str] = None
    grantee: Optional[Dict[str, Any]] = None

    class Config:
        extra = "allow"


class ResourceSharesResponse(BaseModel):
    shares: List[PermissionItemResponse]
    members: List[Dict[str, Any]]


class CreateLinkRequest(BaseModel):
    resource_type: ResourceType
    resource_id: str
    permission: PermissionLevel = Field(default=PermissionLevel.READ, description="read|write|admin")
    slug: Optional[str] = Field(default=None, description="Optional custom slug for the link")

    @field_validator("resource_type", "permission", mode="before")
    @classmethod
    def normalize_values(cls, value: ResourceType | PermissionLevel | str) -> ResourceType | PermissionLevel | str:
        return value.strip().lower() if isinstance(value, str) else value


class UpdateLinkSlugRequest(BaseModel):
    slug: Optional[str] = Field(default=None, description="Set null to clear and revert to token URL")


class ShareLinkResponse(BaseModel):
    id: str
    link_token: str
    link_slug: Optional[str] = None
    resource_type: str
    resource_id: str
    permission: str
    granted_by: Optional[str] = None
    created_at: Optional[str] = None
    expires_at: Optional[str] = None
    url: str

    class Config:
        extra = "allow"


class ShareLinksListResponse(BaseModel):
    links: List[ShareLinkResponse]


class SlugAvailabilityResponse(BaseModel):
    slug: str
    available: bool
    reason: Optional[str] = None


class ResolvedLinkResponse(BaseModel):
    resource_type: str
    resource_id: str
    workspace_id: Optional[str] = None
    workspace_app_id: Optional[str] = None
    app_type: Optional[str] = None
    title: Optional[str] = None
    permission: str

    class Config:
        extra = "allow"


class SharedWithMeItemResponse(BaseModel):
    permission_id: str
    permission: str
    resource_type: str
    resource_id: str
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    title: Optional[str] = None
    workspace_app_id: Optional[str] = None
    app_type: Optional[str] = None
    created_at: Optional[str] = None

    class Config:
        extra = "allow"


class SharedWithMeResponse(BaseModel):
    items: List[SharedWithMeItemResponse]
    count: int


class AccessRequestResponse(BaseModel):
    id: str
    resource_type: str
    resource_id: str
    workspace_id: Optional[str] = None
    requester_id: str
    status: str
    message: Optional[str] = None
    reviewed_by: Optional[str] = None
    created_at: Optional[str] = None
    resolved_at: Optional[str] = None
    requester: Optional[Dict[str, Any]] = None
    resource_title: Optional[str] = None


class UserSearchItem(BaseModel):
    id: str
    email: Optional[str] = None
    name: Optional[str] = None
    avatar_url: Optional[str] = None

    class Config:
        extra = "allow"


class UserSearchResponse(BaseModel):
    users: List[UserSearchItem]
    count: int


# ==========================================================================
# Sharing Endpoints
# ==========================================================================

@router.post("/permissions/share", response_model=PermissionItemResponse, status_code=status.HTTP_201_CREATED)
async def share_resource_endpoint(
    request: ShareResourceRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Share a resource with a user by email."""
    try:
        permission = await share_resource(
            user_id=user_id,
            user_jwt=user_jwt,
            resource_type=request.resource_type,
            resource_id=request.resource_id,
            grantee_email=request.grantee_email,
            permission=request.permission,
        )
        return permission
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to share resource", logger)


@router.post("/permissions/share/batch", response_model=List[PermissionItemResponse])
async def batch_share_endpoint(
    request: BatchShareRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Share a resource with multiple users."""
    try:
        grants = [grant.model_dump() for grant in request.grants]
        permissions = await batch_share_resource(
            user_id=user_id,
            user_jwt=user_jwt,
            resource_type=request.resource_type,
            resource_id=request.resource_id,
            grants=grants,
        )
        return permissions
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to batch share resource", logger)


@router.delete("/permissions/share/{permission_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_share_endpoint(
    permission_id: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Revoke a share by permission ID."""
    try:
        await revoke_share(user_id=user_id, user_jwt=user_jwt, permission_id=permission_id)
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to revoke share", logger)


@router.patch("/permissions/share/{permission_id}", response_model=PermissionItemResponse)
async def update_share_endpoint(
    permission_id: str,
    request: UpdateShareRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Update a share's permission level."""
    try:
        permission = await update_share(
            user_id=user_id,
            user_jwt=user_jwt,
            permission_id=permission_id,
            new_permission=request.permission,
        )
        return permission
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to update share", logger)


@router.get("/permissions/resource/{resource_type}/{resource_id}", response_model=ResourceSharesResponse)
async def get_resource_shares_endpoint(
    resource_type: str,
    resource_id: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List all shares for a resource."""
    try:
        return await get_resource_shares(user_id, user_jwt, resource_type, resource_id)
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to get resource shares", logger)


def _link_with_url(row: Dict[str, Any]) -> Dict[str, Any]:
    identifier = row.get("link_slug") or row.get("link_token")
    return {
        **row,
        "url": f"{settings.frontend_url}/s/{identifier}" if identifier else None,
    }


@router.post("/permissions/link", response_model=ShareLinkResponse, status_code=status.HTTP_201_CREATED)
async def create_share_link_endpoint(
    request: CreateLinkRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Create a share link for a resource."""
    try:
        link = await create_share_link(
            user_id=user_id,
            user_jwt=user_jwt,
            resource_type=request.resource_type,
            resource_id=request.resource_id,
            permission=request.permission,
            slug=request.slug,
        )
        return _link_with_url(link)
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to create share link", logger)


@router.get("/permissions/link/slug-availability", response_model=SlugAvailabilityResponse)
async def check_share_link_slug_availability_endpoint(
    slug: str = Query(..., min_length=1, max_length=64),
    user_id: str = Depends(get_current_user_id),  # auth gate
):
    """Check whether a custom share-link slug is available globally."""
    try:
        logger.info(f"User {user_id} checking link slug availability")
        return await check_share_link_slug_availability(slug)
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to check share link slug availability", logger)


@router.get("/permissions/links/{resource_type}/{resource_id}", response_model=ShareLinksListResponse)
async def list_resource_links_endpoint(
    resource_type: str,
    resource_id: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List share links for a resource."""
    try:
        links = await get_resource_links(
            user_id=user_id,
            user_jwt=user_jwt,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        return {"links": [_link_with_url(link) for link in links]}
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to list share links", logger)


@router.patch("/permissions/link/{link_id}/slug", response_model=ShareLinkResponse)
async def update_share_link_slug_endpoint(
    link_id: str,
    request: UpdateLinkSlugRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Set, update, or clear a custom slug for an existing share link."""
    try:
        link = await update_share_link_slug(
            user_id=user_id,
            user_jwt=user_jwt,
            link_id=link_id,
            slug=request.slug,
        )
        return _link_with_url(link)
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to update share link slug", logger)


@router.delete("/permissions/link/{token}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_share_link_endpoint(
    token: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Revoke a share link by token."""
    try:
        await revoke_share_link(user_id=user_id, user_jwt=user_jwt, link_token=token)
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to revoke share link", logger)


@router.post("/permissions/resolve-link/{token}", response_model=ResolvedLinkResponse)
async def resolve_share_link_endpoint(
    token: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Resolve a share link and grant access."""
    try:
        return await resolve_share_link(user_id=user_id, user_jwt=user_jwt, token=token)
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to resolve share link", logger)

@router.get("/permissions/shared-with-me", response_model=SharedWithMeResponse)
async def shared_with_me_endpoint(
    workspace_id: Optional[str] = Query(default=None),
    resource_type: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List resources shared with the current user."""
    try:
        return await get_shared_with_me(
            user_id=user_id,
            user_jwt=user_jwt,
            workspace_id=workspace_id,
            resource_type=resource_type,
            limit=limit,
            offset=offset,
        )
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to get shared resources", logger)


@router.get("/permissions/users/search", response_model=UserSearchResponse)
@limiter.limit("30/minute")
async def search_users_for_sharing_endpoint(
    request: Request,
    response: Response,
    q: str = Query(..., min_length=3),
    user_id: str = Depends(get_current_user_id),
):
    """Search users globally by email for sharing."""
    try:
        logger.info(f"User {user_id} searching users for sharing")
        users = await search_users_by_email(q, limit=10)
        return {"users": users, "count": len(users)}
    except Exception as e:
        handle_api_exception(e, "Failed to search users", logger)


# ==========================================================================
# Access Requests Endpoints
# ==========================================================================

@router.post("/access-requests", response_model=AccessRequestResponse, status_code=status.HTTP_201_CREATED)
async def create_access_request_endpoint(
    request: AccessRequestCreate,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Create a new access request."""
    try:
        return await create_access_request(
            user_id=user_id,
            user_jwt=user_jwt,
            resource_type=request.resource_type,
            resource_id=request.resource_id,
            message=request.message,
        )
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to create access request", logger)


@router.get("/access-requests/pending", response_model=List[AccessRequestResponse])
async def list_pending_access_requests_endpoint(
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List pending access requests for resources the user can manage."""
    try:
        return await list_pending_access_requests(user_id, user_jwt)
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to list pending access requests", logger)


@router.patch("/access-requests/{request_id}", response_model=AccessRequestResponse)
async def resolve_access_request_endpoint(
    request_id: str,
    request: AccessRequestResolve,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Approve or deny an access request."""
    try:
        return await resolve_access_request(
            user_id=user_id,
            user_jwt=user_jwt,
            request_id=request_id,
            status_value=request.status,
            permission=request.permission or "read",
        )
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to resolve access request", logger)
