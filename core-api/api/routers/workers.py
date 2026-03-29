"""
Worker endpoints — process sync jobs delivered by QStash.

Each endpoint supports single-connection, batch, webhook-incremental, and
initial-sync modes while keeping runtime within Vercel's limits.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, field_validator, model_validator
from qstash import Receiver
from qstash.errors import SignatureError

from api.config import settings
from api.routers.cron import verify_cron_auth
from lib.supabase_client import get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/workers", tags=["workers"])

# Keep margin below Vercel hard timeout.
BATCH_TIME_BUDGET_SECONDS = 240


# ============================================================================
# Request / Response Models
# ============================================================================

class SyncPayload(BaseModel):
    """Payload for sync workers (single, batch, webhook, initial)."""

    connection_id: Optional[str] = None
    connection_ids: Optional[List[str]] = None

    # Webhook context (Google sends historyId as int)
    history_id: Optional[str] = None
    email_address: Optional[str] = None
    channel_id: Optional[str] = None
    resource_state: Optional[str] = None
    message_number: Optional[str] = None

    @field_validator("history_id", mode="before")
    @classmethod
    def coerce_history_id(cls, v: Any) -> Optional[str]:
        return str(v) if v is not None else None

    # Initial sync mode
    initial_sync: bool = False

    # Optional sync knobs
    days_back: Optional[int] = None
    days_past: Optional[int] = None
    days_future: Optional[int] = None
    max_results: Optional[int] = None

    class Config:
        extra = "allow"

    @model_validator(mode="after")
    def validate_mode(self) -> "SyncPayload":
        connection_id = self.connection_id
        connection_ids = self.connection_ids

        if bool(connection_id) == bool(connection_ids):
            raise ValueError("Exactly one of connection_id or connection_ids is required")

        is_batch = bool(connection_ids)
        is_gmail_webhook = bool(self.history_id or self.email_address)
        is_calendar_webhook = bool(
            self.channel_id
            or self.resource_state
            or self.message_number
        )

        if is_batch:
            if self.initial_sync:
                raise ValueError("initial_sync is not allowed in batch mode")
            if is_gmail_webhook or is_calendar_webhook:
                raise ValueError("Webhook fields are not allowed in batch mode")
            return self

        if self.initial_sync and (is_gmail_webhook or is_calendar_webhook):
            raise ValueError("initial_sync cannot be combined with webhook fields")

        if is_gmail_webhook:
            if not connection_id:
                raise ValueError("Webhook Gmail mode requires connection_id")
            if not self.history_id:
                raise ValueError("Webhook Gmail mode requires history_id")
            if is_calendar_webhook:
                raise ValueError("Cannot mix Gmail and Calendar webhook fields")

        if is_calendar_webhook:
            if not connection_id:
                raise ValueError("Webhook Calendar mode requires connection_id")
            if not self.channel_id:
                raise ValueError("Webhook Calendar mode requires channel_id")
            if is_gmail_webhook:
                raise ValueError("Cannot mix Gmail and Calendar webhook fields")

        return self


class AnalyzePayload(BaseModel):
    """Payload for email analysis worker."""

    user_id: Optional[str] = None
    limit: int = 100

    class Config:
        extra = "allow"


class WorkerResponse(BaseModel):
    """Standard worker response."""

    status: str
    message: Optional[str] = None
    processed: Optional[int] = None
    skipped: Optional[int] = None
    errors: Optional[int] = None
    budget_exhausted: Optional[bool] = None
    remaining: Optional[int] = None
    duration_seconds: Optional[float] = None
    analyzed_count: Optional[int] = None

    class Config:
        extra = "allow"


# ============================================================================
# Helpers
# ============================================================================


async def _require_auth(request: Request) -> None:
    """Raise 401 if cron/worker auth fails.

    Verification order:
    1. Dev mode bypass (delegated to verify_cron_auth)
    2. QStash Upstash-Signature JWT (if header + signing keys present)
    3. Bearer token fallback via verify_cron_auth
    4. Reject 401
    """
    authorization = request.headers.get("authorization")

    # Dev mode bypass (verify_cron_auth returns True when API_ENV=development)
    if verify_cron_auth(authorization):
        return

    # QStash signature verification
    qstash_signature = request.headers.get("upstash-signature")
    if qstash_signature and settings.qstash_current_signing_key and settings.qstash_next_signing_key:
        try:
            body = (await request.body()).decode("utf-8")
            receiver = Receiver(
                current_signing_key=settings.qstash_current_signing_key,
                next_signing_key=settings.qstash_next_signing_key,
            )
            receiver.verify(
                signature=qstash_signature,
                body=body,
                clock_tolerance=5,
            )
            return
        except SignatureError:
            logger.warning("[Worker] QStash signature verification failed")

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized",
    )


def _load_connection(
    connection_id: str,
    fields: str = "id, user_id, access_token, refresh_token, token_expires_at, metadata, provider_email",
) -> Tuple[Any, Optional[Dict[str, Any]]]:
    service_supabase = get_service_role_client()
    conn_result = service_supabase.table("ext_connections")\
        .select(fields)\
        .eq("id", connection_id)\
        .eq("is_active", True)\
        .maybe_single()\
        .execute()
    return service_supabase, decrypt_ext_connection_tokens(conn_result.data) if conn_result.data else None


def _touch_last_synced(service_supabase: Any, connection_id: str) -> None:
    service_supabase.table("ext_connections")\
        .update({"last_synced": datetime.now(timezone.utc).isoformat()})\
        .eq("id", connection_id)\
        .execute()


def _run_batch(
    connection_ids: List[str],
    processor: Callable[[str], Dict[str, Any]],
    worker_name: str,
) -> Dict[str, Any]:
    started = time.time()
    processed = 0
    skipped = 0
    errors = 0
    failed_ids: List[str] = []
    budget_exhausted = False
    remaining = 0

    for idx, connection_id in enumerate(connection_ids):
        if time.time() - started >= BATCH_TIME_BUDGET_SECONDS:
            budget_exhausted = True
            remaining = len(connection_ids) - idx
            logger.info(
                f"[Worker] {worker_name} batch budget exhausted "
                f"after {processed + skipped + errors} items"
            )
            break

        try:
            result = processor(connection_id)
            status_value = result.get("status", "ok")
            if status_value == "ok":
                processed += 1
            elif status_value == "skipped":
                skipped += 1
            else:
                errors += 1
                failed_ids.append(connection_id)
        except Exception as exc:
            errors += 1
            failed_ids.append(connection_id)
            logger.error(f"[Worker] {worker_name} batch item failed for {connection_id[:8]}...: {exc}")

    duration = time.time() - started
    return {
        "status": "partial" if errors > 0 else "ok",
        "processed": processed,
        "skipped": skipped,
        "errors": errors,
        "failed_ids": failed_ids,
        "budget_exhausted": budget_exhausted,
        "remaining": remaining,
        "duration_seconds": duration,
    }


def _sync_single_gmail(
    connection_id: str,
    payload: SyncPayload,
) -> Dict[str, Any]:
    from api.services.syncs import sync_gmail_cron
    from api.services.syncs.google_error_utils import is_permanent_google_api_error
    from api.services.syncs.google_services import get_google_services_for_connection

    from api.services.syncs.sync_gmail import sync_gmail_for_connection

    service_supabase = get_service_role_client()
    gmail_service, _, user_id = get_google_services_for_connection(connection_id, service_supabase)

    if not gmail_service:
        logger.warning(f"[Worker] Could not get Gmail service for {connection_id[:8]}...")
        return {"status": "skipped", "message": "Could not get Gmail service"}

    try:
        if payload.initial_sync:
            result = sync_gmail_for_connection(
                gmail_service=gmail_service,
                user_id=user_id,
                connection_id=connection_id,
                supabase_client=service_supabase,
                max_results=payload.max_results or 50,
                days_back=payload.days_back or 20,
            )
            # sync_gmail_for_connection manages last_synced internally
            # (skips update on partial errors)
            if result.get("success"):
                return {**result, "status": "ok"}

            error = result.get("error", "unknown")
            if is_permanent_google_api_error(error):
                return {"status": "skipped", "message": str(error)}
            return {"status": "error", "message": str(error)}

        result = sync_gmail_cron(
            gmail_service=gmail_service,
            connection_id=connection_id,
            user_id=user_id,
            service_supabase=service_supabase,
            days_back=payload.days_back or 7,
        )

        # sync_gmail_cron manages last_synced internally
        # (skips update on batch errors / error_count > 0)
        if result.get("status") == "success":
            return {**result, "status": "ok"}

        error = result.get("error", "unknown")
        if is_permanent_google_api_error(error):
            return {"status": "skipped", "message": str(error)}
        return {"status": "error", "message": str(error)}

    except Exception as exc:
        logger.error(f"[Worker] sync-gmail failed for {connection_id[:8]}...: {exc}")
        return {"status": "error", "message": str(exc)}


def _sync_single_calendar(
    connection_id: str,
    payload: SyncPayload,
) -> Dict[str, Any]:
    from api.services.syncs import sync_google_calendar_cron
    from api.services.syncs.google_error_utils import is_permanent_google_api_error
    from api.services.syncs.google_services import get_google_services_for_connection

    service_supabase = get_service_role_client()
    _, calendar_service, user_id = get_google_services_for_connection(connection_id, service_supabase)

    if not calendar_service:
        logger.warning(f"[Worker] Could not get Calendar service for {connection_id[:8]}...")
        return {"status": "skipped", "message": "Could not get Calendar service"}

    try:
        if payload.initial_sync:
            days_past = payload.days_past if payload.days_past is not None else 7
            days_future = payload.days_future if payload.days_future is not None else 60
        else:
            days_past = payload.days_past if payload.days_past is not None else 30
            days_future = payload.days_future if payload.days_future is not None else 90

        result = sync_google_calendar_cron(
            calendar_service=calendar_service,
            connection_id=connection_id,
            user_id=user_id,
            service_supabase=service_supabase,
            days_past=days_past,
            days_future=days_future,
        )

        # sync_google_calendar_cron manages last_synced internally
        # (skips update on batch errors)
        if result.get("status") == "success":
            return {**result, "status": "ok"}

        error = result.get("error", "unknown")
        if is_permanent_google_api_error(error):
            return {"status": "skipped", "message": str(error)}
        return {"status": "error", "message": str(error)}

    except Exception as exc:
        logger.error(f"[Worker] sync-calendar failed for {connection_id[:8]}...: {exc}")
        return {"status": "error", "message": str(exc)}


def _sync_single_outlook(
    connection_id: str,
    payload: SyncPayload,
) -> Dict[str, Any]:
    from api.services.microsoft.microsoft_oauth_provider import get_valid_microsoft_credentials
    from api.services.syncs import sync_outlook_incremental
    from api.services.syncs.sync_outlook import sync_outlook_for_connection

    service_supabase, connection_data = _load_connection(connection_id)

    if not connection_data:
        logger.warning(f"[Worker] Connection {connection_id[:8]}... not found or inactive")
        return {"status": "skipped", "message": "Connection not found"}

    user_id = connection_data["user_id"]

    try:
        if payload.initial_sync:
            access_token = get_valid_microsoft_credentials(connection_data, service_supabase)
            result = sync_outlook_for_connection(
                access_token=access_token,
                user_id=user_id,
                connection_id=connection_id,
                max_results=payload.max_results or 50,
                days_back=payload.days_back or 20,
            )
            # sync_outlook_for_connection manages last_synced internally
            # (skips update on batch errors)
            if result.get("success"):
                return {**result, "status": "ok"}
            return {"status": "error", "message": str(result.get("error", "unknown"))}

        result = sync_outlook_incremental(
            user_id=user_id,
            connection_id=connection_id,
            connection_data=connection_data,
        )

        # sync_outlook_incremental manages last_synced + delta_link internally
        if result.get("success") is False:
            return {"status": "error", "message": str(result.get("error", "unknown"))}

        return {**result, "status": "ok"}

    except Exception as exc:
        logger.error(f"[Worker] sync-outlook failed for {connection_id[:8]}...: {exc}")
        return {"status": "error", "message": str(exc)}


def _sync_single_outlook_calendar(
    connection_id: str,
    payload: SyncPayload,
) -> Dict[str, Any]:
    from api.services.syncs import sync_outlook_calendar_incremental
    from api.services.syncs.sync_outlook_calendar import sync_outlook_calendar

    service_supabase, connection_data = _load_connection(connection_id)

    if not connection_data:
        logger.warning(f"[Worker] Connection {connection_id[:8]}... not found or inactive")
        return {"status": "skipped", "message": "Connection not found"}

    user_id = connection_data["user_id"]

    try:
        if payload.initial_sync:
            days_past = payload.days_past if payload.days_past is not None else 7
            days_future = payload.days_future if payload.days_future is not None else 60
            result = sync_outlook_calendar(
                user_id=user_id,
                connection_id=connection_id,
                connection_data=connection_data,
                days_back=days_past,
                days_forward=days_future,
            )
        else:
            result = sync_outlook_calendar_incremental(
                user_id=user_id,
                connection_id=connection_id,
                connection_data=connection_data,
            )

        # sync_outlook_calendar_incremental and sync_outlook_calendar
        # manage last_synced + delta_link internally
        if result.get("success") is False:
            return {"status": "error", "message": str(result.get("error", "unknown"))}

        return {**result, "status": "ok"}

    except Exception as exc:
        logger.error(f"[Worker] sync-outlook-calendar failed for {connection_id[:8]}...: {exc}")
        return {"status": "error", "message": str(exc)}


def _process_gmail_webhook(payload: SyncPayload) -> Dict[str, Any]:
    from api.services.webhooks import process_gmail_notification

    if not payload.connection_id or not payload.history_id:
        return {"status": "error", "message": "Missing required webhook Gmail payload fields"}

    # Defensive consistency check: payload connection/email should refer to same active Google connection.
    _, connection_data = _load_connection(
        payload.connection_id,
        fields="id, provider, provider_email",
    )
    if not connection_data:
        return {"status": "error", "message": "Connection not found or inactive"}
    if connection_data.get("provider") != "google":
        return {"status": "error", "message": "Connection provider mismatch for Gmail webhook"}
    resolved_email = payload.email_address or connection_data.get("provider_email")
    if not resolved_email:
        return {"status": "error", "message": "Unable to resolve provider_email for Gmail webhook"}
    if payload.email_address and connection_data.get("provider_email") != payload.email_address:
        return {"status": "error", "message": "Webhook payload email does not match connection"}

    result = process_gmail_notification(resolved_email, payload.history_id)
    status_value = result.get("status")
    if status_value in {"ok", "success"}:
        service_supabase = get_service_role_client()
        _touch_last_synced(service_supabase, payload.connection_id)
        return {**result, "status": "ok"}

    return {
        **result,
        "status": "error",
        "message": str(result.get("message", "Webhook processing failed")),
    }


def _process_calendar_webhook(payload: SyncPayload) -> Dict[str, Any]:
    from api.services.webhooks import process_calendar_notification

    if not payload.connection_id or not payload.channel_id:
        return {"status": "error", "message": "Missing required webhook Calendar payload fields"}

    result = process_calendar_notification(
        channel_id=payload.channel_id,
        resource_state=payload.resource_state or "exists",
    )
    status_value = result.get("status")
    if status_value in {"ok", "success"}:
        service_supabase = get_service_role_client()
        _touch_last_synced(service_supabase, payload.connection_id)
        return {**result, "status": "ok"}

    return {
        **result,
        "status": "error",
        "message": str(result.get("message", "Webhook processing failed")),
    }


# ============================================================================
# Worker Endpoints
# ============================================================================

@router.post("/sync-gmail", response_model=WorkerResponse)
def worker_sync_gmail(
    payload: SyncPayload,
    _: None = Depends(_require_auth),
) -> Dict[str, Any]:
    """Sync Gmail (single, batch, webhook-incremental, initial)."""

    if payload.connection_ids:
        return _run_batch(payload.connection_ids, lambda cid: _sync_single_gmail(cid, payload), "sync-gmail")

    if payload.history_id or payload.email_address:
        result = _process_gmail_webhook(payload)
    else:
        if not payload.connection_id:
            raise HTTPException(status_code=400, detail="connection_id is required")
        result = _sync_single_gmail(payload.connection_id, payload)

    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=str(result.get("message", "unknown")))

    return result


@router.post("/sync-calendar", response_model=WorkerResponse)
def worker_sync_calendar(
    payload: SyncPayload,
    _: None = Depends(_require_auth),
) -> Dict[str, Any]:
    """Sync Calendar (single, batch, webhook-incremental, initial)."""

    if payload.connection_ids:
        return _run_batch(
            payload.connection_ids,
            lambda cid: _sync_single_calendar(cid, payload),
            "sync-calendar",
        )

    if payload.channel_id or payload.resource_state or payload.message_number:
        result = _process_calendar_webhook(payload)
    else:
        if not payload.connection_id:
            raise HTTPException(status_code=400, detail="connection_id is required")
        result = _sync_single_calendar(payload.connection_id, payload)

    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=str(result.get("message", "unknown")))

    return result


@router.post("/sync-outlook", response_model=WorkerResponse)
def worker_sync_outlook(
    payload: SyncPayload,
    _: None = Depends(_require_auth),
) -> Dict[str, Any]:
    """Sync Outlook mail (single, batch, initial)."""

    if payload.connection_ids:
        return _run_batch(
            payload.connection_ids,
            lambda cid: _sync_single_outlook(cid, payload),
            "sync-outlook",
        )

    if not payload.connection_id:
        raise HTTPException(status_code=400, detail="connection_id is required")
    result = _sync_single_outlook(payload.connection_id, payload)
    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=str(result.get("message", "unknown")))
    return result


@router.post("/sync-outlook-calendar", response_model=WorkerResponse)
def worker_sync_outlook_calendar(
    payload: SyncPayload,
    _: None = Depends(_require_auth),
) -> Dict[str, Any]:
    """Sync Outlook Calendar (single, batch, initial)."""

    if payload.connection_ids:
        return _run_batch(
            payload.connection_ids,
            lambda cid: _sync_single_outlook_calendar(cid, payload),
            "sync-outlook-calendar",
        )

    if not payload.connection_id:
        raise HTTPException(status_code=400, detail="connection_id is required")
    result = _sync_single_outlook_calendar(payload.connection_id, payload)
    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=str(result.get("message", "unknown")))
    return result


@router.post("/analyze-emails", response_model=WorkerResponse)
def worker_analyze_emails(
    payload: AnalyzePayload,
    _: None = Depends(_require_auth),
) -> Dict[str, Any]:
    """Analyze unanalyzed emails with AI."""

    from api.services.email.analyze_email_ai import analyze_unanalyzed_emails

    logger.info(
        f"[Worker] analyze-emails starting (user_id={payload.user_id or 'all'}, "
        f"limit={payload.limit})"
    )

    try:
        analyzed_count = analyze_unanalyzed_emails(
            user_id=payload.user_id,
            limit=payload.limit,
        )

        logger.info(f"[Worker] analyze-emails done: {analyzed_count} analyzed")
        return {
            "status": "ok",
            "message": f"Analyzed {analyzed_count} emails",
            "analyzed_count": analyzed_count,
        }

    except Exception as e:
        logger.error(f"[Worker] analyze-emails failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
