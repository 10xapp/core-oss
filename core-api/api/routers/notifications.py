"""
Notifications router - HTTP endpoints for the notification system.
"""
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from typing import Optional, List
from api.services.notifications import (
    get_notifications,
    get_unread_count,
    mark_as_read,
    mark_all_as_read,
    archive_notification,
    get_preferences,
    update_preference,
    subscribe,
    unsubscribe,
    is_subscribed,
)
from api.services.documents import assert_document_access
from api.services.permissions.helpers import normalize_resource_type
from api.dependencies import get_current_user_jwt, get_current_user_id
from api.exceptions import handle_api_exception
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/notifications", tags=["notifications"])


# ============================================================================
# Request Models
# ============================================================================

class MarkAllReadRequest(BaseModel):
    """Request model for marking all notifications as read."""
    workspace_id: Optional[str] = None


class UpdatePreferenceRequest(BaseModel):
    """Request model for updating notification preferences."""
    category: str = Field(..., description="Category: projects, messages, calendar, etc.")
    workspace_id: Optional[str] = None
    in_app: Optional[bool] = None
    push: Optional[bool] = None
    email_digest: Optional[bool] = None
    muted_until: Optional[str] = None


# ============================================================================
# Response Models
# ============================================================================

class NotificationResponse(BaseModel):
    """Response model for a single notification."""
    id: str
    user_id: str
    workspace_id: Optional[str] = None
    type: str
    title: str
    body: Optional[str] = None
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    group_key: Optional[str] = None
    actor_id: Optional[str] = None
    read: bool = False
    seen: bool = False
    archived: bool = False
    data: dict = Field(default_factory=dict)
    created_at: str

    class Config:
        extra = "allow"


class NotificationListResponse(BaseModel):
    """Response model for notification list."""
    notifications: List[NotificationResponse]
    count: int


class UnreadCountResponse(BaseModel):
    """Response model for unread count."""
    count: int


class PreferenceResponse(BaseModel):
    """Response model for a notification preference."""
    id: str
    user_id: str
    workspace_id: Optional[str] = None
    category: str
    in_app: bool = True
    push: bool = True
    email_digest: bool = False
    muted_until: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        extra = "allow"


class PreferenceListResponse(BaseModel):
    """Response model for preference list."""
    preferences: List[PreferenceResponse]
    count: int


class SubscriptionStatusResponse(BaseModel):
    """Response model for subscription check."""
    subscribed: bool


class StatusResponse(BaseModel):
    """Generic status response."""
    status: str

    class Config:
        extra = "allow"


# ============================================================================
# Notification Feed Endpoints
# ============================================================================

@router.get("", response_model=NotificationListResponse)
async def get_notifications_endpoint(
    workspace_id: Optional[str] = Query(None, description="Filter by workspace"),
    unread_only: bool = Query(False, description="Only return unread"),
    limit: int = Query(30, ge=1, le=100, description="Max notifications"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """Get paginated notification feed."""
    try:
        result = await get_notifications(
            user_id=user_id,
            user_jwt=user_jwt,
            workspace_id=workspace_id,
            unread_only=unread_only,
            limit=limit,
            offset=offset,
        )
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to fetch notifications", logger)


@router.get("/unread-count", response_model=UnreadCountResponse)
async def get_unread_count_endpoint(
    workspace_id: Optional[str] = Query(None, description="Filter by workspace"),
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """Get unread notification count (for badge)."""
    try:
        count = await get_unread_count(
            user_id=user_id,
            user_jwt=user_jwt,
            workspace_id=workspace_id,
        )
        return {"count": count}
    except Exception as e:
        handle_api_exception(e, "Failed to fetch unread count", logger)


# ============================================================================
# Notification State Endpoints
# ============================================================================

@router.patch("/{notification_id}/read", response_model=NotificationResponse)
async def mark_as_read_endpoint(
    notification_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Mark a single notification as read."""
    try:
        result = await mark_as_read(user_jwt, notification_id)
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to mark notification as read", logger)


@router.post("/mark-all-read", response_model=StatusResponse)
async def mark_all_as_read_endpoint(
    request: MarkAllReadRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """Mark all unread notifications as read."""
    try:
        count = await mark_all_as_read(
            user_id=user_id,
            user_jwt=user_jwt,
            workspace_id=request.workspace_id,
        )
        return {"status": "ok", "updated_count": count}
    except Exception as e:
        handle_api_exception(e, "Failed to mark all as read", logger)


@router.delete("/{notification_id}", response_model=StatusResponse)
async def archive_notification_endpoint(
    notification_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Archive (soft-delete) a notification."""
    try:
        await archive_notification(user_jwt, notification_id)
        return {"status": "archived"}
    except Exception as e:
        handle_api_exception(e, "Failed to archive notification", logger)


# ============================================================================
# Preference Endpoints
# ============================================================================

@router.get("/preferences", response_model=PreferenceListResponse)
async def get_preferences_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """Get all notification preferences for current user."""
    try:
        prefs = await get_preferences(user_id, user_jwt)
        return {"preferences": prefs, "count": len(prefs)}
    except Exception as e:
        handle_api_exception(e, "Failed to fetch preferences", logger)


@router.patch("/preferences", response_model=PreferenceResponse)
async def update_preference_endpoint(
    request: UpdatePreferenceRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """Create or update a notification preference."""
    try:
        pref = await update_preference(
            user_id=user_id,
            user_jwt=user_jwt,
            category=request.category,
            workspace_id=request.workspace_id,
            in_app=request.in_app,
            push=request.push,
            email_digest=request.email_digest,
            muted_until=request.muted_until,
        )
        return pref
    except Exception as e:
        handle_api_exception(e, "Failed to update preference", logger)


# ============================================================================
# Subscription Endpoints
# ============================================================================

@router.post("/subscribe/{resource_type}/{resource_id}", response_model=StatusResponse)
async def subscribe_endpoint(
    resource_type: str,
    resource_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """Manually subscribe to notifications for a resource."""
    try:
        normalized_resource_type = normalize_resource_type(resource_type)
        if normalized_resource_type in {"document", "folder"}:
            await assert_document_access(
                user_id=user_id,
                user_jwt=user_jwt,
                document_id=resource_id,
            )
        await subscribe(
            user_id=user_id,
            resource_type=normalized_resource_type,
            resource_id=resource_id,
            reason="manual",
        )
        return {"status": "subscribed"}
    except Exception as e:
        handle_api_exception(e, "Failed to subscribe", logger)


@router.delete("/subscribe/{resource_type}/{resource_id}", response_model=StatusResponse)
async def unsubscribe_endpoint(
    resource_type: str,
    resource_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Unsubscribe from notifications for a resource."""
    try:
        await unsubscribe(
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        return {"status": "unsubscribed"}
    except Exception as e:
        handle_api_exception(e, "Failed to unsubscribe", logger)


@router.get("/subscriptions/{resource_type}/{resource_id}", response_model=SubscriptionStatusResponse)
async def check_subscription_endpoint(
    resource_type: str,
    resource_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Check if current user is subscribed to a resource."""
    try:
        subscribed = await is_subscribed(
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        return {"subscribed": subscribed}
    except Exception as e:
        handle_api_exception(e, "Failed to check subscription", logger)
