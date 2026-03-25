"""Helpers for handling invalid external connection state during sync."""

from datetime import datetime, timezone
import logging
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

_ORPHANED_USER_ERROR_PATTERNS = (
    "entities_user_id_fkey",
    "emails_user_id_fkey",
    "calendar_events_user_id_fkey",
    'key (user_id)=(',
    'is not present in table "users"',
)


def _normalize_error(error: Any) -> str:
    return str(error).lower() if error is not None else ""


def is_orphaned_user_error(error: Any) -> bool:
    """
    Return True when a sync write failed because the referenced user no longer exists.

    This commonly surfaces from entity/email/calendar FK errors after an auth user
    was removed while a stale ext_connection still exists.
    """
    error_str = _normalize_error(error)
    return all(pattern in error_str for pattern in _ORPHANED_USER_ERROR_PATTERNS[-2:]) or any(
        pattern in error_str for pattern in _ORPHANED_USER_ERROR_PATTERNS[:-2]
    )


def batch_has_orphaned_user_error(errors: Iterable[Any]) -> bool:
    """Return True when any batch error indicates an orphaned user/connection."""
    return any(is_orphaned_user_error(error) for error in errors)


def deactivate_connection_with_subscriptions(
    service_supabase: Any,
    connection_id: str,
    *,
    reason: Optional[str] = None,
) -> None:
    """
    Deactivate a broken external connection and any active push subscriptions.

    Used to quarantine permanently-invalid connections so background workers stop
    retrying them on every cron run.
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    service_supabase.table("ext_connections")\
        .update({
            "is_active": False,
            "updated_at": timestamp,
        })\
        .eq("id", connection_id)\
        .execute()

    service_supabase.table("push_subscriptions")\
        .update({
            "is_active": False,
            "updated_at": timestamp,
        })\
        .eq("ext_connection_id", connection_id)\
        .eq("is_active", True)\
        .execute()

    if reason:
        logger.warning(
            f"🚫 Deactivated connection {connection_id[:8]}... and subscriptions: {reason}"
        )
