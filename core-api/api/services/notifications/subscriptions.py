"""
Notification subscriptions — manage who is subscribed to what resource.

Uses async service role client for subscribe/unsubscribe (called by backend services
on behalf of users) and for get_subscribers (cross-user lookup).
"""
from typing import List, Dict, Any
import logging
from lib.supabase_client import get_async_service_role_client

logger = logging.getLogger(__name__)


async def subscribe(
    user_id: str,
    resource_type: str,
    resource_id: str,
    reason: str = "manual",
) -> Dict[str, Any]:
    """Subscribe a user to notifications for a resource. Idempotent (upsert).

    Args:
        user_id: User to subscribe
        resource_type: 'issue', 'board', 'channel', etc.
        resource_id: UUID of the entity
        reason: Why subscribed — 'creator', 'assignee', 'commenter', 'manual'

    Returns:
        Subscription row
    """
    client = await get_async_service_role_client()
    result = await client.table("notification_subscriptions") \
        .upsert(
            {
                "user_id": user_id,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "reason": reason,
            },
            on_conflict="user_id,resource_type,resource_id",
        ) \
        .execute()
    return result.data[0] if result.data else {}


async def ensure_subscription(
    user_id: str,
    resource_type: str,
    resource_id: str,
    reason: str = "manual",
) -> Dict[str, Any]:
    """Create a subscription only when one does not already exist.

    Unlike ``subscribe()``, this preserves an existing row's reason so manual
    watches do not get overwritten by auto-subscription flows.
    """
    client = await get_async_service_role_client()
    existing = await client.table("notification_subscriptions") \
        .select("*") \
        .eq("user_id", user_id) \
        .eq("resource_type", resource_type) \
        .eq("resource_id", resource_id) \
        .maybe_single() \
        .execute()

    if existing is not None and existing.data:
        return existing.data

    return await subscribe(
        user_id=user_id,
        resource_type=resource_type,
        resource_id=resource_id,
        reason=reason,
    )


async def unsubscribe(
    user_id: str,
    resource_type: str,
    resource_id: str,
) -> None:
    """Unsubscribe a user from a resource.

    Args:
        user_id: User to unsubscribe
        resource_type: 'issue', 'board', etc.
        resource_id: UUID of the entity
    """
    client = await get_async_service_role_client()
    await client.table("notification_subscriptions") \
        .delete() \
        .eq("user_id", user_id) \
        .eq("resource_type", resource_type) \
        .eq("resource_id", resource_id) \
        .execute()


async def get_subscribers(
    resource_type: str,
    resource_id: str,
) -> List[Dict[str, Any]]:
    """Get all subscribers for a resource.

    Args:
        resource_type: 'issue', 'board', etc.
        resource_id: UUID of the entity

    Returns:
        List of dicts with user_id and reason
    """
    client = await get_async_service_role_client()
    result = await client.table("notification_subscriptions") \
        .select("user_id, reason") \
        .eq("resource_type", resource_type) \
        .eq("resource_id", resource_id) \
        .execute()
    return result.data if result else []


async def is_subscribed(
    user_id: str,
    resource_type: str,
    resource_id: str,
) -> bool:
    """Check if a user is subscribed to a resource.

    Args:
        user_id: User to check
        resource_type: 'issue', 'board', etc.
        resource_id: UUID of the entity

    Returns:
        True if subscribed
    """
    client = await get_async_service_role_client()
    result = await client.table("notification_subscriptions") \
        .select("id") \
        .eq("user_id", user_id) \
        .eq("resource_type", resource_type) \
        .eq("resource_id", resource_id) \
        .maybe_single() \
        .execute()
    return result is not None and result.data is not None
