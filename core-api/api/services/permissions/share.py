"""Sharing services: create, update, revoke permissions."""
from __future__ import annotations

from typing import Dict, Any, List
import logging

from fastapi import HTTPException, status

from lib.supabase_client import get_authenticated_async_client
from api.services.users import get_auth_user_by_email as get_user_by_email
from api.services.notifications.create import create_notification, NotificationType
from api.services.notifications.helpers import get_actor_info
from api.services.permissions.helpers import (
    normalize_permission_level,
    normalize_resource_type,
    resolve_resource_context,
    assert_can_manage_shares,
)

logger = logging.getLogger(__name__)

FILE_SHARED_RESOURCE_TYPES = {"document", "file"}
PERMISSION_LABELS = {
    "read": "Can view",
    "write": "Can edit",
    "admin": "Full access",
}


def _get_share_notification_type(resource_type: str) -> str:
    return (
        NotificationType.FILE_SHARED
        if resource_type in FILE_SHARED_RESOURCE_TYPES
        else NotificationType.PERMISSION_GRANTED
    )


def _format_shared_resource_label(resource_title: str | None, resource_type: str) -> str:
    if resource_title:
        return f'"{resource_title}"'
    if resource_type == "document":
        return "a document"
    if resource_type == "file":
        return "a file"
    return "a resource"


async def _notify_share_granted(
    *,
    actor_user_id: str,
    recipient_ids: List[str],
    permission: str,
    context: Dict[str, Any],
    resource_type: str,
    resource_id: str,
) -> None:
    actor = await get_actor_info(actor_user_id)
    resource_label = _format_shared_resource_label(context.get("title"), resource_type)
    notification_type = _get_share_notification_type(resource_type)

    title = f"{actor['actor_name']} shared {resource_label} with you"
    body = PERMISSION_LABELS.get(permission, "Access granted")

    await create_notification(
        recipients=recipient_ids,
        type=notification_type,
        title=title,
        body=body,
        resource_type=resource_type,
        resource_id=resource_id,
        actor_id=actor_user_id,
        workspace_id=context.get("workspace_id"),
        data={
            "permission": permission,
            "resource_title": context.get("title"),
            "workspace_id": context.get("workspace_id"),
            "document_id": context.get("document_id"),
            **actor,
        },
    )


async def share_resource(
    user_id: str,
    user_jwt: str,
    resource_type: str,
    resource_id: str,
    grantee_email: str,
    permission: str,
) -> Dict[str, Any]:
    """Share a resource with a user by email."""
    normalized_permission = normalize_permission_level(permission)
    normalized_resource_type = normalize_resource_type(resource_type)

    await assert_can_manage_shares(user_id, user_jwt, normalized_resource_type, resource_id)

    grantee = await get_user_by_email(grantee_email.strip().lower())
    if not grantee:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    grantee_id: str = grantee.get("id", "")
    if not grantee_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if grantee_id == user_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot share with yourself")

    context = await resolve_resource_context(normalized_resource_type, resource_id)
    normalized_resource_type = context.get("resource_type", normalized_resource_type)

    supabase = await get_authenticated_async_client(user_jwt)
    insert_data = {
        "workspace_id": context.get("workspace_id"),
        "resource_type": normalized_resource_type,
        "resource_id": resource_id,
        "grantee_type": "user",
        "grantee_id": grantee_id,
        "permission": normalized_permission,
        "granted_by": user_id,
    }

    result = await supabase.table("permissions") \
        .upsert(insert_data, on_conflict="resource_type,resource_id,grantee_id") \
        .execute()

    if not result.data:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to share resource")

    permission_row = result.data[0]

    try:
        await _notify_share_granted(
            actor_user_id=user_id,
            recipient_ids=[grantee_id],
            permission=normalized_permission,
            context=context,
            resource_type=normalized_resource_type,
            resource_id=resource_id,
        )
    except Exception as e:
        logger.warning(f"Permission granted notification failed: {e}")

    return permission_row


async def batch_share_resource(
    user_id: str,
    user_jwt: str,
    resource_type: str,
    resource_id: str,
    grants: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """Share a resource with multiple users."""
    normalized_resource_type = normalize_resource_type(resource_type)
    await assert_can_manage_shares(user_id, user_jwt, normalized_resource_type, resource_id)

    context = await resolve_resource_context(normalized_resource_type, resource_id)
    normalized_resource_type = context.get("resource_type", normalized_resource_type)

    supabase = await get_authenticated_async_client(user_jwt)
    created: List[Dict[str, Any]] = []

    for grant in grants:
        email = (grant.get("email") or "").lower().strip()
        permission = normalize_permission_level(grant.get("permission") or "read")
        if not email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid email in batch share")

        grantee = await get_user_by_email(email)
        if not grantee:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User not found: {email}")

        grantee_id: str = grantee.get("id", "")
        if not grantee_id or grantee_id == user_id:
            continue

        insert_data = {
            "workspace_id": context.get("workspace_id"),
            "resource_type": normalized_resource_type,
            "resource_id": resource_id,
            "grantee_type": "user",
            "grantee_id": grantee_id,
            "permission": permission,
            "granted_by": user_id,
        }

        result = await supabase.table("permissions") \
            .upsert(insert_data, on_conflict="resource_type,resource_id,grantee_id") \
            .execute()

        if result.data:
            created_row = result.data[0]
            created.append(created_row)

            try:
                await _notify_share_granted(
                    actor_user_id=user_id,
                    recipient_ids=[grantee_id],
                    permission=permission,
                    context=context,
                    resource_type=normalized_resource_type,
                    resource_id=resource_id,
                )
            except Exception as e:
                logger.warning(f"Permission granted notification failed: {e}")

    return created


async def revoke_share(
    user_id: str,
    user_jwt: str,
    permission_id: str,
) -> None:
    """Revoke a share by permission ID."""
    supabase = await get_authenticated_async_client(user_jwt)

    existing = await supabase.table("permissions") \
        .select("*") \
        .eq("id", permission_id) \
        .maybe_single() \
        .execute()

    if not existing.data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Permission not found")

    permission_row = existing.data

    await supabase.table("permissions") \
        .delete() \
        .eq("id", permission_id) \
        .execute()

    try:
        if permission_row.get("grantee_type") == "user":
            grantee_id = permission_row.get("grantee_id")
            context = await resolve_resource_context(
                permission_row.get("resource_type"),
                permission_row.get("resource_id"),
            )
            actor = await get_actor_info(user_id)
            title = f"{actor['actor_name']} revoked your access to {context.get('title') or 'a resource'}"
            await create_notification(
                recipients=[grantee_id],
                type=NotificationType.PERMISSION_REVOKED,
                title=title,
                resource_type=permission_row.get("resource_type"),
                resource_id=permission_row.get("resource_id"),
                actor_id=user_id,
                workspace_id=context.get("workspace_id"),
                data={
                    "resource_title": context.get("title"),
                    **actor,
                },
            )
    except Exception as e:
        logger.warning(f"Permission revoked notification failed: {e}")


async def update_share(
    user_id: str,
    user_jwt: str,
    permission_id: str,
    new_permission: str,
) -> Dict[str, Any]:
    """Update permission level for a share."""
    normalized_permission = normalize_permission_level(new_permission)
    supabase = await get_authenticated_async_client(user_jwt)

    existing = await supabase.table("permissions") \
        .select("*") \
        .eq("id", permission_id) \
        .maybe_single() \
        .execute()

    if not existing.data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Permission not found")

    previous = existing.data

    result = await supabase.table("permissions") \
        .update({"permission": normalized_permission}) \
        .eq("id", permission_id) \
        .execute()

    if not result.data:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to update permission")

    updated = result.data[0]

    try:
        if previous.get("grantee_type") == "user" and previous.get("permission") != normalized_permission:
            grantee_id = previous.get("grantee_id")
            context = await resolve_resource_context(
                previous.get("resource_type"),
                previous.get("resource_id"),
            )
            actor = await get_actor_info(user_id)
            title = f"{actor['actor_name']} updated your access to {context.get('title') or 'a resource'}"
            await create_notification(
                recipients=[grantee_id],
                type=NotificationType.PERMISSION_GRANTED,
                title=title,
                resource_type=previous.get("resource_type"),
                resource_id=previous.get("resource_id"),
                actor_id=user_id,
                workspace_id=context.get("workspace_id"),
                data={
                    "permission": normalized_permission,
                    "resource_title": context.get("title"),
                    **actor,
                },
            )
    except Exception as e:
        logger.warning(f"Permission update notification failed: {e}")

    return updated
