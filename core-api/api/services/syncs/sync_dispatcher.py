"""Shared helpers for dispatching sync work through the control plane."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from api.services.syncs.sync_state_store import (
    SYNC_KIND_CALENDAR,
    SYNC_KIND_EMAIL,
    mark_connection_sync_dirty,
)


JOB_TYPE_BY_STREAM = {
    ("google", SYNC_KIND_EMAIL): "sync-gmail",
    ("google", SYNC_KIND_CALENDAR): "sync-calendar",
    ("microsoft", SYNC_KIND_EMAIL): "sync-outlook",
    ("microsoft", SYNC_KIND_CALENDAR): "sync-outlook-calendar",
}

MANUAL_SYNC_PRIORITY = 200


def resolve_stream_job_type(provider: str, sync_kind: str) -> str:
    try:
        return JOB_TYPE_BY_STREAM[(provider, sync_kind)]
    except KeyError as exc:
        raise ValueError(f"Unsupported provider/sync_kind combination: {provider}/{sync_kind}") from exc


def build_stream_dedup_id(provider: str, sync_kind: str, connection_id: str) -> str:
    return f"stream-{provider}-{sync_kind}-{connection_id}"


def mark_stream_dirty(
    service_supabase: Any,
    connection_id: str,
    provider: str,
    sync_kind: str,
    *,
    latest_seen_cursor: Optional[str] = None,
    priority: int = 0,
    provider_event_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Mark a stream dirty after validating the provider/sync_kind mapping."""
    resolve_stream_job_type(provider, sync_kind)
    return mark_connection_sync_dirty(
        service_supabase,
        connection_id,
        sync_kind,
        latest_seen_cursor=latest_seen_cursor,
        priority=priority,
        provider_event_at=provider_event_at,
        metadata=metadata,
    )


def mark_and_enqueue_stream(
    service_supabase: Any,
    queue_client: Any,
    connection_id: str,
    provider: str,
    sync_kind: str,
    *,
    extra: Optional[Dict[str, Any]] = None,
    dedup_id: Optional[str] = None,
    latest_seen_cursor: Optional[str] = None,
    priority: int = 0,
    provider_event_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Mark a stream dirty and enqueue the existing provider-specific worker."""
    job_type = resolve_stream_job_type(provider, sync_kind)
    if not mark_stream_dirty(
        service_supabase,
        connection_id,
        provider,
        sync_kind,
        latest_seen_cursor=latest_seen_cursor,
        priority=priority,
        provider_event_at=provider_event_at,
        metadata=metadata,
    ):
        return False

    return queue_client.enqueue_sync_for_connection(
        connection_id,
        job_type,
        extra=extra,
        dedup_id=dedup_id or build_stream_dedup_id(provider, sync_kind, connection_id),
    )
