"""
Grouped notification primitives.

Phase 1 introduces the shared upsert and resolution helpers used by grouped
notification emitters. No emitters are wired up here yet.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import logging
from typing import Any, Dict, List, Optional

from api.services.notifications.create import NotificationType, TYPE_TO_CATEGORY
from api.services.notifications.preferences import should_notify
from lib.supabase_client import get_async_service_role_client

logger = logging.getLogger(__name__)

_UNSET = object()


class GroupedNotificationResolution(str, Enum):
    """How a grouped notification should resolve when its source clears."""

    ARCHIVE = "archive"
    MARK_READ = "mark_read"


GROUPED_NOTIFICATION_RESOLUTION_BY_TYPE: Dict[str, GroupedNotificationResolution] = {
    NotificationType.MESSAGE_RECEIVED: GroupedNotificationResolution.ARCHIVE,
    NotificationType.EMAIL_RECEIVED: GroupedNotificationResolution.ARCHIVE,
    NotificationType.CALENDAR_INVITE: GroupedNotificationResolution.ARCHIVE,
    NotificationType.FILE_EDITED: GroupedNotificationResolution.MARK_READ,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_grouped_resolution(notification_type: str) -> GroupedNotificationResolution:
    """Return the explicit resolution mode for a grouped notification type."""
    resolution = GROUPED_NOTIFICATION_RESOLUTION_BY_TYPE.get(notification_type)
    if resolution is None:
        raise ValueError(
            f"Grouped notification type '{notification_type}' is missing an explicit resolution mode"
        )
    return resolution


def _with_optional_field(payload: Dict[str, Any], field: str, value: Any) -> None:
    if value is not _UNSET:
        payload[field] = value


async def upsert_grouped_notification(
    *,
    user_id: str,
    type: str,
    group_key: str,
    workspace_id: Any = _UNSET,
    resource_type: Any = _UNSET,
    resource_id: Any = _UNSET,
    actor_id: Any = _UNSET,
    title: Any = _UNSET,
    body: Any = _UNSET,
    data: Any = _UNSET,
) -> Optional[Dict[str, Any]]:
    """Insert or refresh a grouped notification row.

    Required keys match the grouped uniqueness contract: ``user_id``, ``type``,
    and ``group_key``. Optional fields only overwrite existing values when they
    are provided. The helper also enforces the standard in-app preference check
    and skips actor-self notifications when ``actor_id`` matches ``user_id``.

    Because ``notifications.title`` is non-nullable, callers must provide
    ``title`` whenever the upsert may need to create a new row.
    """
    if not user_id:
        raise ValueError("user_id is required")
    if not type:
        raise ValueError("type is required")
    if not group_key:
        raise ValueError("group_key is required")
    if actor_id is not _UNSET and actor_id == user_id:
        logger.info(
            "Skipping grouped notification for actor self-notify type=%s group_key=%s user_id=%s",
            type,
            group_key,
            user_id,
        )
        return None

    category = TYPE_TO_CATEGORY.get(type)
    if category is None:
        raise ValueError(
            f"Grouped notification type '{type}' is missing a preference category mapping"
        )

    workspace_scope = None if workspace_id is _UNSET else workspace_id
    if not await should_notify(user_id, category, workspace_scope, channel="in_app"):
        logger.info(
            "Skipping grouped notification due to preferences type=%s group_key=%s user_id=%s",
            type,
            group_key,
            user_id,
        )
        return None

    client = await get_async_service_role_client()
    now_iso = _now_iso()

    update_payload: Dict[str, Any] = {
        "read": False,
        "seen": False,
        "archived": False,
        "created_at": now_iso,
    }
    _with_optional_field(update_payload, "workspace_id", workspace_id)
    _with_optional_field(update_payload, "resource_type", resource_type)
    _with_optional_field(update_payload, "resource_id", resource_id)
    _with_optional_field(update_payload, "actor_id", actor_id)
    _with_optional_field(update_payload, "title", title)
    _with_optional_field(update_payload, "body", body)
    _with_optional_field(update_payload, "data", data)

    updated = await client.table("notifications") \
        .update(update_payload) \
        .eq("user_id", user_id) \
        .eq("type", type) \
        .eq("group_key", group_key) \
        .eq("archived", False) \
        .execute()

    if updated.data:
        return updated.data[0]

    if title in (_UNSET, None):
        raise ValueError("title is required when inserting a grouped notification")

    insert_payload: Dict[str, Any] = {
        "user_id": user_id,
        "type": type,
        "group_key": group_key,
        "title": title,
        "read": False,
        "seen": False,
        "archived": False,
        "created_at": now_iso,
    }
    _with_optional_field(insert_payload, "workspace_id", workspace_id)
    _with_optional_field(insert_payload, "resource_type", resource_type)
    _with_optional_field(insert_payload, "resource_id", resource_id)
    _with_optional_field(insert_payload, "actor_id", actor_id)
    _with_optional_field(insert_payload, "body", body)
    if data is _UNSET:
        insert_payload["data"] = {}
    else:
        insert_payload["data"] = data

    try:
        inserted = await client.table("notifications").insert(insert_payload).execute()
        return inserted.data[0] if inserted.data else {}
    except Exception as err:
        if "duplicate key value" not in str(err).lower():
            raise

        logger.info(
            "Grouped notification insert raced with another writer; retrying update "
            "for type=%s group_key=%s user_id=%s",
            type,
            group_key,
            user_id,
        )
        retried = await client.table("notifications") \
            .update(update_payload) \
            .eq("user_id", user_id) \
            .eq("type", type) \
            .eq("group_key", group_key) \
            .eq("archived", False) \
            .execute()
        return retried.data[0] if retried.data else {}


async def resolve_grouped_notification(
    *,
    user_id: str,
    type: str,
    group_key: str,
    resolution: Optional[GroupedNotificationResolution] = None,
) -> List[Dict[str, Any]]:
    """Resolve an active grouped notification row for a user."""
    if not user_id:
        raise ValueError("user_id is required")
    if not type:
        raise ValueError("type is required")
    if not group_key:
        raise ValueError("group_key is required")

    resolution_mode = resolution or get_grouped_resolution(type)
    payload: Dict[str, Any] = {"read": True, "seen": True}
    if resolution_mode == GroupedNotificationResolution.ARCHIVE:
        payload["archived"] = True

    client = await get_async_service_role_client()
    result = await client.table("notifications") \
        .update(payload) \
        .eq("user_id", user_id) \
        .eq("type", type) \
        .eq("group_key", group_key) \
        .eq("archived", False) \
        .execute()
    return result.data or []
