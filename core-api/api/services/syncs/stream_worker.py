"""Railway worker loop for lease-based sync execution."""

from __future__ import annotations

import logging
import os
import socket
import time
from typing import Any, Dict, Optional
from uuid import uuid4

from api.routers import workers as worker_router
from api.services.syncs.failure_policy import maybe_quarantine_failed_connection
from api.services.syncs.sync_state_store import (
    CLAIM_MODE_ANY,
    CLAIM_MODE_DIRTY_ONLY,
    CLAIM_MODE_RECONCILE_ONLY,
    SYNC_KIND_CALENDAR,
    SYNC_KIND_EMAIL,
    claim_connection_sync_lease,
    complete_connection_sync_lease,
    fail_connection_sync_lease,
    get_failure_retry_seconds,
    get_connection_sync_state,
    start_connection_sync_lease_heartbeat,
    stop_connection_sync_lease_heartbeat,
)
from lib.supabase_client import get_service_role_client

logger = logging.getLogger(__name__)


def build_worker_id() -> str:
    configured = os.getenv("SYNC_WORKER_ID")
    if configured:
        return configured
    return f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"


def resolve_worker_claim_mode(configured_mode: Optional[str] = None) -> str:
    """
    Map human-friendly worker modes onto queue claim modes.

    Defaults to the legacy mixed behavior when SYNC_WORKER_MODE is unset.
    """
    mode = (configured_mode or os.getenv("SYNC_WORKER_MODE") or "").strip().lower()
    if mode in {"", "any", "mixed", "all"}:
        return CLAIM_MODE_ANY
    if mode in {"realtime", "dirty", CLAIM_MODE_DIRTY_ONLY}:
        return CLAIM_MODE_DIRTY_ONLY
    if mode in {"reconcile", CLAIM_MODE_RECONCILE_ONLY}:
        return CLAIM_MODE_RECONCILE_ONLY
    raise ValueError(f"Unsupported SYNC_WORKER_MODE: {configured_mode or mode}")


def _build_payload(
    connection_id: str,
    claim: Dict[str, Any],
    state: Optional[Dict[str, Any]],
) -> worker_router.SyncPayload:
    metadata = (state or {}).get("metadata") or {}
    source = metadata.get("source")
    history_id = None
    if source == "gmail-webhook":
        raw_history_id = metadata.get("history_id") or claim.get("latest_seen_cursor") or (state or {}).get("latest_seen_cursor")
        history_id = str(raw_history_id) if raw_history_id is not None else None

    return worker_router.SyncPayload(
        connection_id=connection_id,
        history_id=history_id,
        channel_id=metadata.get("channel_id"),
        resource_state=metadata.get("resource_state"),
        message_number=metadata.get("message_number"),
        initial_sync=bool(metadata.get("initial_sync")),
        days_back=metadata.get("days_back"),
        days_past=metadata.get("days_past"),
        days_future=metadata.get("days_future"),
        max_results=metadata.get("max_results"),
    )


def _resolve_active_google_calendar_channel(
    service_supabase: Any,
    connection_id: str,
) -> Optional[str]:
    result = (
        service_supabase.table("push_subscriptions")
        .select("channel_id")
        .eq("ext_connection_id", connection_id)
        .eq("provider", "calendar")
        .eq("is_active", True)
        .limit(1)
        .execute()
    )
    if not result.data:
        return None
    return result.data[0].get("channel_id")


def _dispatch_claimed_stream(
    claim: Dict[str, Any],
    *,
    service_supabase: Optional[Any] = None,
    state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    connection_id = claim["connection_id"]
    provider = claim["provider"]
    sync_kind = claim["sync_kind"]
    metadata = (state or {}).get("metadata") or {}
    payload = _build_payload(connection_id, claim, state)

    if payload.initial_sync:
        if provider == "google" and sync_kind == SYNC_KIND_EMAIL:
            return worker_router._sync_single_gmail(connection_id, payload)
        if provider == "google" and sync_kind == SYNC_KIND_CALENDAR:
            return worker_router._sync_single_calendar(connection_id, payload)
        if provider == "microsoft" and sync_kind == SYNC_KIND_EMAIL:
            return worker_router._sync_single_outlook(connection_id, payload)
        if provider == "microsoft" and sync_kind == SYNC_KIND_CALENDAR:
            return worker_router._sync_single_outlook_calendar(connection_id, payload)

    if (
        provider == "google"
        and sync_kind == SYNC_KIND_EMAIL
        and metadata.get("source") == "gmail-webhook"
        and payload.history_id
    ):
        return worker_router._process_gmail_webhook(payload)

    if provider == "google" and sync_kind == SYNC_KIND_CALENDAR:
        active_channel_id = (
            _resolve_active_google_calendar_channel(service_supabase, connection_id)
            if service_supabase is not None
            else None
        )
        payload.resource_state = payload.resource_state or metadata.get("resource_state") or "exists"
        if (
            metadata.get("source") == "google-calendar-webhook"
            and active_channel_id
            and (not payload.channel_id or payload.channel_id == active_channel_id)
        ):
            payload.channel_id = active_channel_id
            return worker_router._process_calendar_webhook(payload)
        payload.channel_id = active_channel_id

    if provider == "google" and sync_kind == SYNC_KIND_EMAIL:
        return worker_router._sync_single_gmail(connection_id, payload)
    if provider == "google" and sync_kind == SYNC_KIND_CALENDAR:
        return worker_router._sync_single_calendar(connection_id, payload)
    if provider == "microsoft" and sync_kind == SYNC_KIND_EMAIL:
        return worker_router._sync_single_outlook(connection_id, payload)
    if provider == "microsoft" and sync_kind == SYNC_KIND_CALENDAR:
        return worker_router._sync_single_outlook_calendar(connection_id, payload)

    raise ValueError(f"Unsupported claim: provider={provider}, sync_kind={sync_kind}")


def process_one_claim(
    claim: Dict[str, Any],
    *,
    worker_id: str,
    service_supabase: Optional[Any] = None,
) -> Dict[str, Any]:
    service_supabase = service_supabase or get_service_role_client()
    connection_id = claim["connection_id"]
    sync_kind = claim["sync_kind"]
    state = None
    heartbeat_stop = None
    heartbeat_thread = None

    try:
        state = get_connection_sync_state(service_supabase, connection_id, sync_kind)
        heartbeat_stop, heartbeat_thread = start_connection_sync_lease_heartbeat(
            service_supabase,
            connection_id,
            sync_kind,
            worker_id,
            lease_seconds=int(claim.get("lease_seconds") or 600),
            logger=logger,
        )
        result = _dispatch_claimed_stream(
            claim,
            service_supabase=service_supabase,
            state=state,
        )
    except Exception as exc:
        if heartbeat_stop is not None and heartbeat_thread is not None:
            stop_connection_sync_lease_heartbeat(heartbeat_stop, heartbeat_thread)
        try:
            quarantine_result = maybe_quarantine_failed_connection(
                service_supabase,
                connection_id=connection_id,
                sync_kind=sync_kind,
                worker_id=worker_id,
                provider=claim.get("provider"),
                error=exc,
                retry_count=(state or {}).get("retry_count"),
            )
            if quarantine_result is not None:
                return quarantine_result
        except Exception:
            logger.exception(
                "[StreamWorker] failed to quarantine %s/%s after error",
                connection_id[:8],
                sync_kind,
            )
        try:
            fail_connection_sync_lease(
                service_supabase,
                connection_id,
                sync_kind,
                worker_id,
                str(exc),
                retry_seconds=get_failure_retry_seconds((state or {}).get("retry_count")),
            )
        except Exception:
            logger.exception(
                "[StreamWorker] failed to mark claim failure for %s/%s",
                connection_id[:8],
                sync_kind,
            )
        logger.exception(
            "[StreamWorker] claim failed for %s/%s",
            connection_id[:8],
            sync_kind,
        )
        return {"status": "error", "message": str(exc)}

    try:
        if result.get("status") == "error":
            try:
                quarantine_result = maybe_quarantine_failed_connection(
                    service_supabase,
                    connection_id=connection_id,
                    sync_kind=sync_kind,
                    worker_id=worker_id,
                    provider=claim.get("provider"),
                    error=result.get("message", "sync failed"),
                    retry_count=(state or {}).get("retry_count"),
                )
                if quarantine_result is not None:
                    return quarantine_result
            except Exception:
                logger.exception(
                    "[StreamWorker] failed to quarantine %s/%s after result error",
                    connection_id[:8],
                    sync_kind,
                )
            fail_connection_sync_lease(
                service_supabase,
                connection_id,
                sync_kind,
                worker_id,
                str(result.get("message", "sync failed")),
                retry_seconds=get_failure_retry_seconds((state or {}).get("retry_count")),
            )
            return result

        result_cursor = worker_router._extract_result_cursor(result)
        complete_connection_sync_lease(
            service_supabase,
            connection_id,
            sync_kind,
            worker_id,
            last_synced_cursor=result_cursor,
            latest_seen_cursor=result_cursor,
        )
        return result
    finally:
        if heartbeat_stop is not None and heartbeat_thread is not None:
            stop_connection_sync_lease_heartbeat(heartbeat_stop, heartbeat_thread)


def run_once(
    *,
    worker_id: Optional[str] = None,
    lease_seconds: int = 600,
    provider: Optional[str] = None,
    sync_kind: Optional[str] = None,
    service_supabase: Optional[Any] = None,
    claim_mode: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    service_supabase = service_supabase or get_service_role_client()
    worker_id = worker_id or build_worker_id()
    resolved_claim_mode = resolve_worker_claim_mode(claim_mode)

    claim = claim_connection_sync_lease(
        service_supabase,
        worker_id,
        lease_seconds=lease_seconds,
        provider=provider,
        sync_kind=sync_kind,
        claim_mode=resolved_claim_mode,
    )
    if not claim:
        return None

    logger.info(
        f"[StreamWorker] claimed {claim['provider']}/{claim['sync_kind']} "
        f"for {claim['connection_id'][:8]}..."
    )
    claim["lease_seconds"] = lease_seconds
    return process_one_claim(
        claim,
        worker_id=worker_id,
        service_supabase=service_supabase,
    )


def run_forever() -> None:
    worker_id = build_worker_id()
    lease_seconds = int(os.getenv("SYNC_WORKER_LEASE_SECONDS", "600"))
    poll_seconds = float(os.getenv("SYNC_WORKER_POLL_SECONDS", "5"))
    provider = os.getenv("SYNC_WORKER_PROVIDER") or None
    sync_kind = os.getenv("SYNC_WORKER_SYNC_KIND") or None
    configured_mode = os.getenv("SYNC_WORKER_MODE") or None
    claim_mode = resolve_worker_claim_mode(configured_mode)
    consecutive_failures = 0

    logger.info(
        f"[StreamWorker] starting worker_id={worker_id} "
        f"lease_seconds={lease_seconds} poll_seconds={poll_seconds} "
        f"claim_mode={claim_mode}"
    )

    while True:
        try:
            result = run_once(
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                provider=provider,
                sync_kind=sync_kind,
                claim_mode=claim_mode,
            )
            consecutive_failures = 0
            if result is None:
                time.sleep(poll_seconds)
        except Exception:
            consecutive_failures += 1
            sleep_seconds = min(poll_seconds * (2 ** min(consecutive_failures - 1, 4)), 60.0)
            logger.exception(
                "[StreamWorker] worker loop crashed; retrying in %.1fs",
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
