"""
Core notification creation — any service calls these functions.

Uses async service role client for inserts (bypasses RLS) since we're writing
notifications on behalf of other users (fan-out).
"""
from typing import Optional, List, Dict, Any
import logging
from api.services.notifications.subscriptions import get_subscribers
from api.services.notifications.preferences import should_notify
from lib.supabase_client import get_async_service_role_client

logger = logging.getLogger(__name__)


class NotificationType:
    """Notification type constants."""
    TASK_ASSIGNED = "task_assigned"
    TASK_COMPLETED = "task_completed"
    TASK_UPDATED = "task_updated"
    COMMENT_ADDED = "comment_added"
    MENTIONED = "mentioned"
    WORKSPACE_INVITE = "workspace_invite"
    PERMISSION_GRANTED = "permission_granted"
    PERMISSION_REVOKED = "permission_revoked"
    ACCESS_REQUESTED = "access_requested"
    ACCESS_APPROVED = "access_approved"
    ACCESS_DENIED = "access_denied"
    MESSAGE_RECEIVED = "message_received"
    EMAIL_RECEIVED = "email_received"
    FILE_SHARED = "file_shared"
    FILE_EDITED = "file_edited"
    CALENDAR_INVITE = "calendar_invite"


# Map notification types to preference categories
TYPE_TO_CATEGORY: Dict[str, str] = {
    "task_assigned": "projects",
    "task_completed": "projects",
    "task_updated": "projects",
    "comment_added": "projects",
    "mentioned": "projects",
    "workspace_invite": "workspaces",
    "permission_granted": "sharing",
    "permission_revoked": "sharing",
    "access_requested": "sharing",
    "access_approved": "sharing",
    "access_denied": "sharing",
    "message_received": "messages",
    "email_received": "email",
    "file_shared": "files",
    "file_edited": "files",
    "calendar_invite": "calendar",
}


async def create_notification(
    recipients: List[str],
    type: str,
    title: str,
    body: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    actor_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fan-out: creates one notification row per recipient.

    Skips the actor (don't notify yourself), checks preferences,
    and uses service role client (bypasses RLS). Supabase Realtime
    auto-delivers to connected clients.

    Args:
        recipients: User IDs to notify
        type: NotificationType constant
        title: Notification title
        body: Optional longer description
        resource_type: 'issue', 'channel_message', etc.
        resource_id: UUID of the entity
        actor_id: Who triggered it
        workspace_id: Workspace context
        data: Extra context for rendering

    Returns:
        List of created notification rows
    """
    logger.info(f"[Notify] create_notification called: type={type}, recipients={recipients}, actor_id={actor_id}")

    if not recipients:
        logger.info("[Notify] No recipients, skipping")
        return []

    # Filter out actor (don't notify yourself)
    filtered = [uid for uid in recipients if uid != actor_id]
    if not filtered:
        logger.info("[Notify] All recipients filtered (actor only), skipping")
        return []

    # Check preferences for each recipient
    category = TYPE_TO_CATEGORY.get(type, "general")
    notifiable: List[str] = []
    for uid in filtered:
        if await should_notify(uid, category, workspace_id, channel="in_app"):
            notifiable.append(uid)

    if not notifiable:
        logger.info("[Notify] All recipients opted out via preferences, skipping")
        return []

    logger.info(f"[Notify] Inserting {len(notifiable)} notification(s) for type={type}")

    # Build rows
    rows = [
        {
            "user_id": uid,
            "workspace_id": workspace_id,
            "type": type,
            "title": title,
            "body": body,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "actor_id": actor_id,
            "data": data or {},
        }
        for uid in notifiable
    ]

    try:
        client = await get_async_service_role_client()
        result = await client.table("notifications").insert(rows).execute()
        logger.info(f"Created {len(result.data)} notifications (type={type})")
        return result.data
    except Exception as e:
        logger.error(f"Failed to create notifications: {e}")
        return []


async def notify_subscribers(
    resource_type: str,
    resource_id: str,
    type: str,
    title: str,
    body: Optional[str] = None,
    actor_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Convenience: looks up subscribers for a resource, then creates notifications.

    Args:
        resource_type: 'issue', 'board', etc.
        resource_id: UUID of the entity
        type: NotificationType constant
        title: Notification title
        body: Optional longer text
        actor_id: Who triggered it
        workspace_id: Workspace context
        data: Extra context for rendering

    Returns:
        List of created notification rows
    """
    subscribers = await get_subscribers(resource_type, resource_id)
    recipient_ids = [sub["user_id"] for sub in subscribers]

    return await create_notification(
        recipients=recipient_ids,
        type=type,
        title=title,
        body=body,
        resource_type=resource_type,
        resource_id=resource_id,
        actor_id=actor_id,
        workspace_id=workspace_id,
        data=data,
    )
