"""Grouped file-edit notifications for shared documents."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from api.services.notifications.create import NotificationType
from api.services.notifications.grouped import upsert_grouped_notification
from api.services.notifications.helpers import get_actor_info
from api.services.notifications.subscriptions import ensure_subscription, get_subscribers
from api.services.permissions.helpers import filter_active_permissions
from lib.supabase_client import get_async_service_role_client

logger = logging.getLogger(__name__)

_AUTO_SUBSCRIPTION_REASONS = frozenset({"creator", "share_recipient"})


def get_file_edit_group_key(document_id: str) -> str:
    """Return the grouped dedupe key for document-edit notifications."""
    return f"document:{document_id}"


def _format_document_target(document_title: Optional[str]) -> str:
    normalized = (document_title or "").strip()
    if normalized:
        return f'"{normalized}"'
    return "a document"


async def _get_direct_share_recipient_ids(
    *,
    document_id: str,
    file_id: Optional[str] = None,
) -> Set[str]:
    client = await get_async_service_role_client()
    recipient_ids: Set[str] = set()

    targets = [("document", document_id)]
    if file_id:
        targets.append(("file", file_id))

    for resource_type, resource_id in targets:
        result = await client.table("permissions") \
            .select("grantee_id, expires_at") \
            .eq("resource_type", resource_type) \
            .eq("resource_id", resource_id) \
            .eq("grantee_type", "user") \
            .execute()

        active_rows = filter_active_permissions(result.data or [])
        for row in active_rows:
            grantee_id = row.get("grantee_id")
            if isinstance(grantee_id, str) and grantee_id:
                recipient_ids.add(grantee_id)

    return recipient_ids


async def _ensure_auto_subscriptions(
    *,
    document_id: str,
    owner_id: Optional[str],
    share_recipient_ids: Set[str],
) -> None:
    if owner_id:
        await ensure_subscription(
            user_id=owner_id,
            resource_type="document",
            resource_id=document_id,
            reason="creator",
        )

    for user_id in share_recipient_ids:
        await ensure_subscription(
            user_id=user_id,
            resource_type="document",
            resource_id=document_id,
            reason="share_recipient",
        )


async def emit_document_edited_notification(
    *,
    document_id: str,
    document_title: Optional[str],
    editor_user_id: str,
    workspace_id: Optional[str],
    owner_id: Optional[str],
    file_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Upsert grouped edit notifications for a document's interested users."""
    if not document_id:
        raise ValueError("document_id is required")
    if not editor_user_id:
        raise ValueError("editor_user_id is required")

    share_recipient_ids = await _get_direct_share_recipient_ids(
        document_id=document_id,
        file_id=file_id,
    )
    await _ensure_auto_subscriptions(
        document_id=document_id,
        owner_id=owner_id,
        share_recipient_ids=share_recipient_ids,
    )

    subscriptions = await get_subscribers("document", document_id)
    manual_subscriber_ids = {
        row["user_id"]
        for row in subscriptions
        if isinstance(row.get("user_id"), str)
        and row.get("reason") not in _AUTO_SUBSCRIPTION_REASONS
    }

    recipient_ids = set(share_recipient_ids)
    recipient_ids.update(manual_subscriber_ids)
    if owner_id:
        recipient_ids.add(owner_id)

    if not recipient_ids:
        return []

    actor = await get_actor_info(editor_user_id)
    title = f"{actor['actor_name']} made edits to {_format_document_target(document_title)}"
    payload = {
        "document_id": document_id,
        "document_title": document_title,
        "editor_name": actor["actor_name"],
        "workspace_id": workspace_id,
        "resource_title": document_title,
        **actor,
    }

    notifications: List[Dict[str, Any]] = []
    group_key = get_file_edit_group_key(document_id)
    for user_id in recipient_ids:
        notification = await upsert_grouped_notification(
            user_id=user_id,
            type=NotificationType.FILE_EDITED,
            group_key=group_key,
            workspace_id=workspace_id,
            resource_type="document",
            resource_id=document_id,
            actor_id=editor_user_id,
            title=title,
            data=payload,
        )
        if notification is not None:
            notifications.append(notification)

    logger.info(
        "Emitted file_edited notifications for document=%s recipients=%d actor=%s",
        document_id,
        len(notifications),
        editor_user_id,
    )
    return notifications
