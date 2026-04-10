"""Helpers for the lease-based connection sync control plane."""

from __future__ import annotations

from datetime import datetime
import os
from threading import Event, Thread
from typing import Any, Dict, Optional, Tuple

SYNC_KIND_EMAIL = "email"
SYNC_KIND_CALENDAR = "calendar"
VALID_SYNC_KINDS = {SYNC_KIND_EMAIL, SYNC_KIND_CALENDAR}
CLAIM_MODE_ANY = "any"
CLAIM_MODE_DIRTY_ONLY = "dirty_only"
CLAIM_MODE_RECONCILE_ONLY = "reconcile_only"
VALID_CLAIM_MODES = {
    CLAIM_MODE_ANY,
    CLAIM_MODE_DIRTY_ONLY,
    CLAIM_MODE_RECONCILE_ONLY,
}
DEFAULT_RECONCILE_INTERVAL_SECONDS = 21600
DEFAULT_MAX_FAILURE_RETRY_COUNT = 5
FAILURE_RETRY_SCHEDULE_SECONDS = (60, 300, 900, 1800, 3600, 21600)
MAX_FAILURE_RETRY_SECONDS = 86400


def _normalize_rpc_data(data: Any) -> Any:
    if isinstance(data, list):
        if not data:
            return None
        return data[0]
    return data


def _require_sync_kind(sync_kind: str) -> str:
    if sync_kind not in VALID_SYNC_KINDS:
        raise ValueError(f"Unsupported sync_kind: {sync_kind}")
    return sync_kind


def _require_claim_mode(claim_mode: str) -> str:
    if claim_mode not in VALID_CLAIM_MODES:
        raise ValueError(f"Unsupported claim_mode: {claim_mode}")
    return claim_mode


def get_reconcile_interval_seconds() -> int:
    """Return the clean-stream safety-net reconcile interval in seconds."""
    value = os.getenv(
        "RECONCILE_INTERVAL_SECONDS",
        str(DEFAULT_RECONCILE_INTERVAL_SECONDS),
    ).strip()
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return DEFAULT_RECONCILE_INTERVAL_SECONDS

    return max(300, min(seconds, 7 * 24 * 3600))


def get_failure_retry_seconds(previous_retry_count: Optional[int]) -> int:
    """Return stepped retry backoff to prevent dead connections from hogging workers."""
    try:
        retry_count = int(previous_retry_count or 0)
    except (TypeError, ValueError):
        retry_count = 0

    retry_count = max(retry_count, 0)
    if retry_count < len(FAILURE_RETRY_SCHEDULE_SECONDS):
        return FAILURE_RETRY_SCHEDULE_SECONDS[retry_count]
    return MAX_FAILURE_RETRY_SECONDS


def get_max_failure_retry_count() -> int:
    """Return the retry cap after which a broken connection is auto-deactivated."""
    value = os.getenv(
        "MAX_FAILURE_RETRY_COUNT",
        str(DEFAULT_MAX_FAILURE_RETRY_COUNT),
    ).strip()
    try:
        retry_count = int(value)
    except (TypeError, ValueError):
        return DEFAULT_MAX_FAILURE_RETRY_COUNT

    return max(1, min(retry_count, 1000))


def mark_connection_sync_dirty(
    service_supabase: Any,
    connection_id: str,
    sync_kind: str,
    *,
    latest_seen_cursor: Optional[str] = None,
    priority: int = 0,
    provider_event_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Mark a sync stream dirty, inserting the state row if needed."""
    result = service_supabase.rpc(
        "mark_connection_sync_dirty",
        {
            "p_connection_id": connection_id,
            "p_sync_kind": _require_sync_kind(sync_kind),
            "p_latest_seen_cursor": latest_seen_cursor,
            "p_priority": priority,
            "p_provider_event_at": provider_event_at.isoformat() if provider_event_at else None,
            "p_metadata": metadata or {},
        },
    ).execute()

    normalized = _normalize_rpc_data(getattr(result, "data", None))
    return bool(normalized)


def get_connection_sync_state(
    service_supabase: Any,
    connection_id: str,
    sync_kind: str,
) -> Optional[Dict[str, Any]]:
    """Fetch the current control-plane row for a single sync stream."""
    result = (
        service_supabase.table("connection_sync_state")
        .select(
            "connection_id, provider, sync_kind, dirty, dirty_generation, "
            "lease_generation, latest_seen_cursor, last_synced_cursor, "
            "priority, retry_count, metadata, lease_owner, lease_expires_at, "
            "last_sync_finished_at, next_reconcile_at"
        )
        .eq("connection_id", connection_id)
        .eq("sync_kind", _require_sync_kind(sync_kind))
        .maybe_single()
        .execute()
    )

    data = getattr(result, "data", None)
    return dict(data) if data else None


def claim_connection_sync_lease(
    service_supabase: Any,
    worker_id: str,
    *,
    lease_seconds: int = 120,
    provider: Optional[str] = None,
    sync_kind: Optional[str] = None,
    connection_id: Optional[str] = None,
    claim_mode: str = CLAIM_MODE_ANY,
) -> Optional[Dict[str, Any]]:
    """Claim the next dirty or due-for-reconcile sync stream lease, if available."""
    if sync_kind is not None:
        _require_sync_kind(sync_kind)
    _require_claim_mode(claim_mode)

    result = service_supabase.rpc(
        "claim_connection_sync_lease",
        {
            "p_worker_id": worker_id,
            "p_lease_seconds": lease_seconds,
            "p_provider": provider,
            "p_sync_kind": sync_kind,
            "p_connection_id": connection_id,
            "p_claim_mode": claim_mode,
        },
    ).execute()

    normalized = _normalize_rpc_data(getattr(result, "data", None))
    return dict(normalized) if normalized else None


def heartbeat_connection_sync_lease(
    service_supabase: Any,
    connection_id: str,
    sync_kind: str,
    worker_id: str,
    *,
    lease_seconds: int = 120,
) -> bool:
    """Extend an existing sync lease held by the given worker."""
    result = service_supabase.rpc(
        "heartbeat_connection_sync_lease",
        {
            "p_connection_id": connection_id,
            "p_sync_kind": _require_sync_kind(sync_kind),
            "p_worker_id": worker_id,
            "p_lease_seconds": lease_seconds,
        },
    ).execute()

    normalized = _normalize_rpc_data(getattr(result, "data", None))
    return bool(normalized)


def start_connection_sync_lease_heartbeat(
    service_supabase: Any,
    connection_id: str,
    sync_kind: str,
    worker_id: str,
    *,
    lease_seconds: int = 120,
    logger: Optional[Any] = None,
) -> Tuple[Event, Thread]:
    """
    Start a daemon thread that keeps a claimed lease alive during long sync runs.

    The caller is responsible for stopping the heartbeat via
    stop_connection_sync_lease_heartbeat().
    """
    interval_seconds = max(5, min(max(lease_seconds // 3, 1), 30))
    stop_event = Event()

    def _heartbeat_loop() -> None:
        while not stop_event.wait(interval_seconds):
            try:
                alive = heartbeat_connection_sync_lease(
                    service_supabase,
                    connection_id,
                    sync_kind,
                    worker_id,
                    lease_seconds=lease_seconds,
                )
                if not alive:
                    if logger is not None:
                        logger.warning(
                            "[SyncLease] heartbeat stopped for %s/%s because the lease is gone",
                            connection_id[:8],
                            sync_kind,
                        )
                    return
            except Exception as exc:
                if logger is not None:
                    logger.warning(
                        "[SyncLease] heartbeat failed for %s/%s: %s",
                        connection_id[:8],
                        sync_kind,
                        exc,
                    )

    thread = Thread(
        target=_heartbeat_loop,
        name=f"sync-lease-heartbeat-{sync_kind}-{connection_id[:8]}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def stop_connection_sync_lease_heartbeat(
    stop_event: Event,
    thread: Thread,
    *,
    join_timeout_seconds: float = 1.0,
) -> None:
    """Stop a heartbeat thread started by start_connection_sync_lease_heartbeat()."""
    stop_event.set()
    thread.join(timeout=join_timeout_seconds)


def complete_connection_sync_lease(
    service_supabase: Any,
    connection_id: str,
    sync_kind: str,
    worker_id: str,
    *,
    last_synced_cursor: Optional[str] = None,
    latest_seen_cursor: Optional[str] = None,
    keep_dirty: bool = False,
    reconcile_interval_seconds: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Release a sync lease after a successful worker run."""
    result = service_supabase.rpc(
        "complete_connection_sync_lease",
        {
            "p_connection_id": connection_id,
            "p_sync_kind": _require_sync_kind(sync_kind),
            "p_worker_id": worker_id,
            "p_last_synced_cursor": last_synced_cursor,
            "p_latest_seen_cursor": latest_seen_cursor,
            "p_keep_dirty": keep_dirty,
            "p_reconcile_interval_seconds": (
                reconcile_interval_seconds
                if reconcile_interval_seconds is not None
                else get_reconcile_interval_seconds()
            ),
        },
    ).execute()

    normalized = _normalize_rpc_data(getattr(result, "data", None))
    return dict(normalized) if normalized else None


def fail_connection_sync_lease(
    service_supabase: Any,
    connection_id: str,
    sync_kind: str,
    worker_id: str,
    error: str,
    *,
    retry_seconds: int = 60,
) -> Optional[Dict[str, Any]]:
    """Release a sync lease after failure and schedule a retry."""
    result = service_supabase.rpc(
        "fail_connection_sync_lease",
        {
            "p_connection_id": connection_id,
            "p_sync_kind": _require_sync_kind(sync_kind),
            "p_worker_id": worker_id,
            "p_error": error,
            "p_retry_seconds": retry_seconds,
        },
    ).execute()

    normalized = _normalize_rpc_data(getattr(result, "data", None))
    return dict(normalized) if normalized else None
