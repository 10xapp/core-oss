"""Failure policy helpers for quarantining permanently-broken sync streams."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from api.services.syncs.connection_state import deactivate_connection_with_subscriptions
from api.services.syncs.google_error_utils import is_permanent_google_oauth_error
from api.services.syncs.sync_state_store import (
    complete_connection_sync_lease,
    get_max_failure_retry_count,
)

logger = logging.getLogger(__name__)


_PERMANENT_MICROSOFT_AUTH_PATTERNS = (
    "refresh token is invalid",
    "user must re-authenticate",
    "token has been revoked",
    "interaction_required",
    "invalid_grant",
)


def _is_permanent_microsoft_auth_error(error: Any) -> bool:
    error_str = str(error).lower() if error else ""
    return any(p in error_str for p in _PERMANENT_MICROSOFT_AUTH_PATTERNS)


def _coerce_retry_count(retry_count: Optional[int]) -> int:
    try:
        return max(int(retry_count or 0), 0)
    except (TypeError, ValueError):
        return 0


def get_failed_sync_quarantine_reason(
    *,
    provider: Optional[str],
    error: Any,
    retry_count: Optional[int],
) -> Optional[str]:
    """Return a deactivation reason when a failed sync should be quarantined."""
    error_message = str(error or "sync failed")

    if provider == "google" and is_permanent_google_oauth_error(error_message):
        return f"Permanent Google OAuth failure: {error_message}"

    if provider == "microsoft" and _is_permanent_microsoft_auth_error(error_message):
        return f"Permanent Microsoft auth failure: {error_message}"

    next_retry_count = _coerce_retry_count(retry_count) + 1
    max_retry_count = get_max_failure_retry_count()
    if next_retry_count >= max_retry_count:
        return (
            f"Sync failed {next_retry_count} times and hit the retry cap "
            f"({max_retry_count}); user must reconnect"
        )

    return None


def maybe_quarantine_failed_connection(
    service_supabase: Any,
    *,
    connection_id: str,
    sync_kind: str,
    worker_id: str,
    provider: Optional[str],
    error: Any,
    retry_count: Optional[int],
) -> Optional[Dict[str, Any]]:
    """
    Deactivate a claimed connection when a failure is known-permanent or has retried too often.
    """
    reason = get_failed_sync_quarantine_reason(
        provider=provider,
        error=error,
        retry_count=retry_count,
    )
    if reason is None:
        return None

    deactivate_connection_with_subscriptions(
        service_supabase,
        connection_id,
        reason=reason,
    )
    complete_connection_sync_lease(
        service_supabase,
        connection_id,
        sync_kind,
        worker_id,
    )
    logger.warning(
        "[SyncFailurePolicy] quarantined %s/%s: %s",
        connection_id[:8],
        sync_kind,
        reason,
    )
    return {
        "status": "quarantined",
        "message": reason,
    }
