"""
Cron router - Scheduled background jobs for sync reliability
These jobs ensure data stays in sync even if webhooks fail

CRON JOB SCHEDULE:
==================

1. /api/cron/incremental-sync (Disabled rollback stub)
   - Broad sweep reconciliation has been replaced by per-stream scheduling
   - Route remains available as a fast rollback hook during migration
   - Should not be scheduled in Vercel cron

2. /api/cron/renew-watches (Every hour)
   - CRITICAL: Prevents watch subscriptions from expiring
   - Gmail watches expire after 7 days
   - Calendar watches expire after configured time
   - Automatically renews watches before they expire
   - Batch size configurable via RENEWAL_BATCH_SIZE (default 50)

3. /api/cron/setup-missing-watches (Every 6 hours)
   - Recreates watches for active connections that have no active subscription
   - Catches cases where watches expired and renewal missed them
   - Batch size limited to prevent thundering herd

4. /api/cron/watch-health (Disabled / manual recovery)
   - Queues targeted recovery work for suspiciously silent watches
   - Avoids a broad fleet-wide sweep
   - Capped oldest-first batch to limit blast radius

5. /api/cron/daily-verification (Daily at 2am)
   - Full sync for data integrity verification
   - Catches any edge cases or long-term drift
   - Runs full sync for a subset of users each day
"""
from fastapi import APIRouter, HTTPException, status, Header
from typing import Any, Dict, List, Optional
import hashlib
import hmac
import logging
import os
from datetime import datetime, timezone, timedelta
from sentry_sdk.crons import capture_checkin
from sentry_sdk.crons.consts import MonitorStatus
from api.config import settings
from lib.supabase_client import get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens
from api.services.syncs import (
    renew_watch_service_role,
    start_gmail_watch_service_role,
    start_calendar_watch_service_role
)
from api.services.syncs.sync_dispatcher import mark_stream_dirty
from api.services.syncs.google_services import get_google_services_for_connection
from api.services.microsoft.microsoft_oauth_provider import (
    MicrosoftReauthRequiredError,
    get_valid_microsoft_credentials,
)
from api.services.microsoft.microsoft_webhook_provider import renew_microsoft_subscription

from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/cron", tags=["cron"])
WATCH_PROVIDER_TO_SYNC_KIND = {
    "gmail": "email",
    "calendar": "calendar",
}


# ============================================================================
# Response Models
# ============================================================================

class IncrementalSyncResponse(BaseModel):
    """Response for incremental sync cron job."""
    status: str
    message: Optional[str] = None
    duration_seconds: Optional[float] = None
    total_users: Optional[int] = None
    users_processed: Optional[int] = None
    success: Optional[int] = None
    skipped: Optional[int] = None
    errors: Optional[int] = None
    jobs_enqueued: Optional[int] = None
    jobs_failed: Optional[int] = None
    batch_mode: Optional[bool] = None
    connections_considered: Optional[int] = None
    streams_marked: Optional[int] = None


class RenewWatchesResponse(BaseModel):
    """Response for watch renewal cron job."""
    status: str
    message: Optional[str] = None
    duration_seconds: Optional[float] = None
    total_expiring: Optional[int] = None
    renewed: int = 0
    errors: Optional[int] = None


class SetupWatchesResponse(BaseModel):
    """Response for setup missing watches cron job."""
    status: str
    message: Optional[str] = None
    duration_seconds: Optional[float] = None
    total_users: Optional[int] = None
    setup_needed: Optional[int] = None
    errors: Optional[int] = None


class WatchHealthResponse(BaseModel):
    """Response for targeted watch-health recovery cron job."""
    status: str
    message: Optional[str] = None
    duration_seconds: Optional[float] = None
    checked: int = 0
    queued: int = 0
    errors: int = 0


class DailyVerificationResponse(BaseModel):
    """Response for daily verification cron job."""
    status: str
    message: Optional[str] = None
    duration_seconds: Optional[float] = None
    total_stale: Optional[int] = None
    verified: int = 0
    errors: Optional[int] = None


class AnalyzeEmailsResponse(BaseModel):
    """Response for email analysis cron job."""
    status: str
    timestamp: str
    analyzed_count: int
    message: str


class AgentHealthResponse(BaseModel):
    """Response for agent health cron job."""
    status: str
    checked: int = 0
    healthy: int = 0
    errors: int = 0
    duration_seconds: Optional[float] = None


class CleanupResponse(BaseModel):
    """Response for cleanup cron jobs."""
    status: str
    duration_seconds: Optional[float] = None
    deleted: Optional[int] = None
    deleted_db_records: Optional[int] = None
    deleted_r2_files: Optional[int] = None
    elapsed_seconds: Optional[float] = None


class CronJobInfo(BaseModel):
    """Information about a single cron job."""
    name: str
    schedule: str
    description: str


class CronHealthResponse(BaseModel):
    """Response for cron health check."""
    status: str
    service: str
    timestamp: str
    jobs: List[CronJobInfo]


def verify_cron_auth(authorization: Optional[str]) -> bool:
    """
    Verify that the request is from Vercel Cron
    Vercel sends: Authorization: Bearer <CRON_SECRET>
    """
    if not authorization:
        return False

    # In development, allow any request
    if settings.api_env == "development":
        logger.info("🔓 Development mode: skipping cron auth check")
        return True

    # In production, verify the secret
    if not settings.cron_secret:
        logger.error("❌ CRON_SECRET not configured - rejecting all cron requests")
        return False

    expected_auth = f"Bearer {settings.cron_secret}"
    return hmac.compare_digest(authorization, expected_auth)


def is_cron_batch_mode_enabled() -> bool:
    """
    Batch mode for incremental-sync queue fanout.
    Default enabled in production.
    """
    value = os.getenv("CRON_BATCH_MODE", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


def get_cron_batch_size() -> int:
    """Return incremental-sync batch size, bounded to safe worker-sized chunks."""
    value = os.getenv("CRON_BATCH_SIZE", "100").strip()
    try:
        batch_size = int(value)
    except (TypeError, ValueError):
        return 100

    return batch_size if 1 <= batch_size <= 250 else 100


def get_reconciliation_stale_minutes() -> int:
    """Return stale threshold for reconciliation in minutes.

    Streams synced more recently than this are skipped.
    Default 45 min.  Tunable via RECONCILIATION_STALE_MINUTES.
    """
    value = os.getenv("RECONCILIATION_STALE_MINUTES", "45").strip()
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return 45
    return max(5, min(minutes, 1440))


def get_renewal_batch_size() -> int:
    """Return watch-renewal batch size per cron run.

    Default 50.  Tunable via RENEWAL_BATCH_SIZE.
    """
    value = os.getenv("RENEWAL_BATCH_SIZE", "50").strip()
    try:
        size = int(value)
    except (TypeError, ValueError):
        return 50
    return max(1, min(size, 200))


def get_watch_health_batch_size() -> int:
    """Return the max number of watch-health recovery marks per run."""
    value = os.getenv("WATCH_HEALTH_BATCH_SIZE", "25").strip()
    try:
        size = int(value)
    except (TypeError, ValueError):
        return 25
    return max(1, min(size, 100))


def get_watch_health_stale_hours() -> int:
    """Return the stale-notification threshold for active watches."""
    value = os.getenv("WATCH_HEALTH_STALE_HOURS", "24").strip()
    try:
        hours = int(value)
    except (TypeError, ValueError):
        return 24
    return max(1, min(hours, 7 * 24))


def get_watch_health_initial_grace_hours() -> int:
    """Return how long a never-notified watch gets before recovery."""
    value = os.getenv("WATCH_HEALTH_INITIAL_GRACE_HOURS", "12").strip()
    try:
        hours = int(value)
    except (TypeError, ValueError):
        return 12
    return max(1, min(hours, 7 * 24))


def get_watch_health_priority() -> int:
    """Return the priority used for watch-health dirty marks."""
    value = os.getenv("WATCH_HEALTH_PRIORITY", "25").strip()
    try:
        priority = int(value)
    except (TypeError, ValueError):
        return 25
    return max(0, min(priority, 99))


def _parse_optional_datetime(value: Optional[Any]) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _has_pending_sync_state(sync_state: Optional[Dict[str, Any]], *, now: datetime) -> bool:
    if not sync_state:
        return False
    if sync_state.get("dirty"):
        return True
    if int(sync_state.get("retry_count") or 0) > 0:
        return True
    lease_expires_at = _parse_optional_datetime(sync_state.get("lease_expires_at"))
    return bool(lease_expires_at and lease_expires_at > now)


def _classify_watch_health_candidate(
    watch: Dict[str, Any],
    sync_state: Optional[Dict[str, Any]],
    *,
    now: datetime,
    stale_after: timedelta,
    initial_grace_after: timedelta,
) -> Optional[str]:
    """
    Return a recovery reason when a watch looks silently unhealthy.

    This intentionally avoids a blanket "no notification for N minutes" rule.
    Quiet inboxes/calendars are normal. We only target watches that either used
    to notify and then went silent, or have never notified after a long grace
    window, and only when the corresponding sync stream is not already active.
    """
    if watch.get("provider") not in WATCH_PROVIDER_TO_SYNC_KIND:
        return None

    expiration = _parse_optional_datetime(watch.get("expiration"))
    if expiration and expiration <= now + timedelta(hours=24):
        return None

    if _has_pending_sync_state(sync_state, now=now):
        return None

    last_sync_finished_at = _parse_optional_datetime((sync_state or {}).get("last_sync_finished_at"))
    notification_count = int(watch.get("notification_count") or 0)
    last_notification_or_creation = (
        _parse_optional_datetime(watch.get("last_notification_at"))
        or _parse_optional_datetime(watch.get("created_at"))
    )

    if notification_count > 0:
        if (
            last_notification_or_creation
            and last_notification_or_creation <= now - stale_after
            and (
                last_sync_finished_at is None
                or last_sync_finished_at <= now - stale_after
            )
        ):
            return "stale_notifications"
        return None

    created_at = _parse_optional_datetime(watch.get("created_at"))
    if (
        notification_count == 0
        and created_at
        and created_at <= now - initial_grace_after
        and (
            last_sync_finished_at is None
            or last_sync_finished_at <= now - initial_grace_after
        )
    ):
        return "never_notified"

    return None


def _get_batch_bucket(now: Optional[datetime] = None) -> str:
    timestamp = now or datetime.now(timezone.utc)
    return timestamp.strftime("%Y%m%d%H%M")


def _hash_connection_ids(connection_ids: List[str]) -> str:
    # Deterministic queue dedup fingerprint, not security hashing.
    return hashlib.sha256(",".join(connection_ids).encode("utf-8")).hexdigest()[:12]


def _build_batch_token(connection_ids: List[str], bucket: str) -> str:
    return f"{bucket}-{_hash_connection_ids(connection_ids)}"


def _chunk_connection_ids(connection_ids: List[str], batch_size: int) -> List[List[str]]:
    return [
        connection_ids[idx:idx + batch_size]
        for idx in range(0, len(connection_ids), batch_size)
    ]


@router.get("/incremental-sync", response_model=IncrementalSyncResponse)
async def cron_incremental_sync(authorization: str = Header(None)):
    """
    Disabled rollback stub for the old broad-sweep reconciliation cron.

    Per-stream safety-net reconciliation is now scheduled via
    connection_sync_state.next_reconcile_at and claimed directly by workers.
    """
    logger.info("=" * 80)
    logger.info("🕐 CRON: Incremental sweep endpoint invoked")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"🔑 Authorization header present: {bool(authorization)}")
    logger.info(f"🌍 Environment: {settings.api_env}")

    # Verify authorization
    if not verify_cron_auth(authorization):
        logger.warning("⚠️ Unauthorized cron attempt - authorization failed")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    logger.info("✅ Authorization verified")
    logger.info("ℹ️ Incremental sweep remains disabled; workers reconcile via next_reconcile_at")
    logger.info("=" * 80)
    return {
        "status": "disabled",
        "message": "Broad sweep reconciliation has been replaced by worker-driven next_reconcile_at scheduling",
        "users_processed": 0,
        "jobs_enqueued": 0,
        "jobs_failed": 0,
        "streams_marked": 0,
        "batch_mode": False,
    }


@router.get("/renew-watches", response_model=RenewWatchesResponse)
async def cron_renew_watches(authorization: str = Header(None)):
    """
    CRON JOB: Renew expiring watch subscriptions
    
    RUNS: Every hour (configurable via vercel.json)


    PURPOSE: CRITICAL - Prevents watch subscriptions from expiring
    - Gmail watches expire after 7 days
    - Calendar watches expire after configured time
    - This job renews any watches expiring within 24 hours
    - Batch size is configurable via RENEWAL_BATCH_SIZE (default 50)
    - Ensures continuous real-time notifications

    Without this job, push notifications will stop working after 7 days!
    
    NOTE: Uses GET because Vercel cron jobs send GET requests by default
    """
    logger.info("=" * 80)
    logger.info("🕐 CRON: Starting watch renewal check")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone.utc).isoformat()}")
    
    # Verify authorization
    if not verify_cron_auth(authorization):
        logger.warning("⚠️ Unauthorized cron attempt")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    
    logger.info("✅ Authorization verified")
    start_time = datetime.now(timezone.utc)

    check_in_id = capture_checkin(monitor_slug="renew-watches", status=MonitorStatus.IN_PROGRESS)

    try:
        # Use service role to access all subscriptions
        service_supabase = get_service_role_client()

        # Get subscriptions expiring within 24 hours.
        # Limit batch size and renew soonest-first to smooth provider load
        # across hourly runs instead of renewing large batches all at once.
        # At 1.4k users with ~2 watches each, 50/hour provides 1200 renewals
        # per day, which comfortably covers the steady-state requirement.
        RENEWAL_BATCH_SIZE = get_renewal_batch_size()
        threshold_time = datetime.now(timezone.utc) + timedelta(hours=24)

        result = service_supabase.table('push_subscriptions')\
            .select('*, ext_connections!push_subscriptions_ext_connection_id_fkey!inner(id, user_id, is_active, access_token, refresh_token, token_expires_at, metadata)')\
            .eq('is_active', True)\
            .lt('expiration', threshold_time.isoformat())\
            .order('expiration', desc=False)\
            .limit(RENEWAL_BATCH_SIZE)\
            .execute()

        expiring_subs = result.data
        for sub in (expiring_subs or []):
            if sub.get('ext_connections'):
                sub['ext_connections'] = decrypt_ext_connection_tokens(sub['ext_connections'])

        if not expiring_subs:
            logger.info("ℹ️ No watches need renewal")
            capture_checkin(monitor_slug="renew-watches", check_in_id=check_in_id, status=MonitorStatus.OK)
            return {
                "status": "completed",
                "message": "No watches need renewal",
                "renewed": 0
            }

        logger.info(f"⚠️ Found {len(expiring_subs)} watches expiring within 24 hours")

        renewed_count = 0
        error_count = 0

        for sub in expiring_subs:
            try:
                connection_id = sub.get('ext_connection_id')
                user_id = sub.get('ext_connections', {}).get('user_id')
                provider = sub.get('provider')
                expiration = sub.get('expiration')

                if not connection_id or not user_id:
                    logger.warning(f"⚠️ Subscription {sub.get('id')} is missing connection_id or user_id")
                    error_count += 1
                    continue

                if not provider:
                    logger.warning(f"⚠️ Subscription {sub['id']} has no provider field")
                    error_count += 1
                    continue

                logger.info(f"🔄 Renewing {provider} watch for connection {connection_id[:8]}... (expires: {expiration})")

                if provider == 'microsoft':
                    # Microsoft renewal path — use Graph API PATCH
                    connection_data = sub.get('ext_connections', {})
                    subscription_id = sub.get('channel_id')
                    resource_type = sub.get('resource_type')

                    if not subscription_id:
                        logger.warning(f"⚠️ Microsoft subscription {sub.get('id')} missing channel_id")
                        error_count += 1
                        continue

                    if not connection_data.get('is_active'):
                        logger.warning(f"⚠️ Microsoft connection {connection_id[:8]}... is inactive; deactivating stale subscriptions")
                        try:
                            cleanup_result = service_supabase.table('push_subscriptions')\
                                .update({'is_active': False})\
                                .eq('ext_connection_id', connection_id)\
                                .eq('provider', 'microsoft')\
                                .eq('is_active', True)\
                                .execute()
                            cleaned_count = len(cleanup_result.data) if cleanup_result.data else 0
                            logger.info(f"🧹 Deactivated {cleaned_count} Microsoft subscription(s) for inactive connection {connection_id[:8]}...")
                        except Exception as cleanup_err:
                            logger.error(f"❌ Failed cleanup for inactive connection {connection_id[:8]}...: {cleanup_err}")
                            error_count += 1
                        continue

                    try:
                        access_token = get_valid_microsoft_credentials(connection_data, service_supabase)
                    except MicrosoftReauthRequiredError as e:
                        logger.warning(
                            f"🚫 Permanent Microsoft OAuth failure for connection {connection_id[:8]}... "
                            f"(user {user_id[:8]}...): {e} — deactivating"
                        )
                        try:
                            service_supabase.table('ext_connections')\
                                .update({
                                    'is_active': False,
                                    'updated_at': datetime.now(timezone.utc).isoformat()
                                })\
                                .eq('id', connection_id)\
                                .execute()
                            service_supabase.table('push_subscriptions')\
                                .update({'is_active': False})\
                                .eq('ext_connection_id', connection_id)\
                                .eq('is_active', True)\
                                .execute()
                        except Exception as deactivate_err:
                            logger.error(f"Failed to deactivate connection: {deactivate_err}")
                        error_count += 1
                        continue
                    except ValueError as e:
                        logger.error(f"❌ Microsoft token error for {connection_id[:8]}...: {e}")
                        error_count += 1
                        continue

                    result = renew_microsoft_subscription(
                        access_token=access_token,
                        subscription_id=subscription_id,
                        resource_type=resource_type,
                        supabase_client=service_supabase
                    )
                else:
                    # Google renewal path (gmail/calendar)
                    gmail_service, calendar_service, _ = get_google_services_for_connection(
                        connection_id,
                        service_supabase
                    )

                    if not gmail_service and not calendar_service:
                        logger.warning(f"⚠️ Could not get Google services for connection {connection_id[:8]}...")
                        error_count += 1
                        continue

                    result = renew_watch_service_role(
                        user_id=user_id,
                        provider=provider,
                        gmail_service=gmail_service,
                        calendar_service=calendar_service,
                        connection_id=connection_id,
                        service_supabase=service_supabase
                    )

                if result.get('success'):
                    logger.info(f"✅ Watch renewal completed for user {user_id[:8]}... ({provider})")
                    renewed_count += 1
                else:
                    logger.error(f"❌ Watch renewal failed for user {user_id[:8]}...: {result.get('error')}")
                    error_count += 1

            except Exception as e:
                logger.error(f"❌ Error renewing watch: {str(e)}")
                error_count += 1
                continue

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        logger.info(f"✅ CRON: Watch renewal completed in {duration:.2f}s")
        logger.info(f"📊 Results: {renewed_count} renewed, {error_count} errors")
        logger.info("=" * 80)

        capture_checkin(monitor_slug="renew-watches", check_in_id=check_in_id, status=MonitorStatus.OK)

        return {
            "status": "completed",
            "duration_seconds": duration,
            "total_expiring": len(expiring_subs),
            "renewed": renewed_count,
            "errors": error_count
        }

    except Exception as e:
        capture_checkin(monitor_slug="renew-watches", check_in_id=check_in_id, status=MonitorStatus.ERROR)
        logger.error(f"❌ CRON: Watch renewal failed: {str(e)}")
        logger.exception("Full traceback:")
        logger.info("=" * 80)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Watch renewal failed: {str(e)}"
        )


@router.get("/setup-missing-watches", response_model=SetupWatchesResponse)
async def cron_setup_missing_watches(authorization: str = Header(None)):
    """
    CRON JOB: Set up watches for users who don't have them
    
    RUNS: Disabled / manual recovery
    
    PURPOSE: Controlled recovery path for missing watch subscriptions
    - Sets up watches for new users who just connected Google
    - Recovers from watch setup failures
    - Intentionally left unscheduled during backlog stabilization

    NOTE: Uses GET because Vercel cron jobs send GET requests by default
    """
    logger.info("=" * 80)
    logger.info("🕐 CRON: Checking for users without watches")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone.utc).isoformat()}")
    
    # Verify authorization
    if not verify_cron_auth(authorization):
        logger.warning("⚠️ Unauthorized cron attempt")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    
    logger.info("✅ Authorization verified")
    start_time = datetime.now(timezone.utc)

    check_in_id = capture_checkin(monitor_slug="setup-missing-watches", status=MonitorStatus.IN_PROGRESS)

    try:
        # Use service role to access all connections
        service_supabase = get_service_role_client()

        # Get all active Google connections
        connections = service_supabase.table('ext_connections')\
            .select('user_id, id')\
            .eq('provider', 'google')\
            .eq('is_active', True)\
            .execute()

        if not connections.data:
            logger.info("ℹ️ No active connections")
            capture_checkin(monitor_slug="setup-missing-watches", check_in_id=check_in_id, status=MonitorStatus.OK)
            return {
                "status": "completed",
                "message": "No active connections",
                "setup_needed": 0
            }

        setup_count = 0
        error_count = 0

        # Limit how many watches we set up per cron run to avoid a burst
        # of Google notifications when all watches fire their initial sync.
        # At 20/hour, full recovery from a mass deactivation takes a few
        # hours instead of creating a thundering herd.
        SETUP_BATCH_SIZE = 50

        for conn in connections.data:
            user_id = conn['user_id']
            connection_id = conn['id']

            try:
                # Check if THIS CONNECTION has active Gmail watch (not just user)
                # This is important for multi-account support (secondary accounts)
                gmail_watch = service_supabase.table('push_subscriptions')\
                    .select('id')\
                    .eq('ext_connection_id', connection_id)\
                    .eq('provider', 'gmail')\
                    .eq('is_active', True)\
                    .execute()

                # Check if THIS CONNECTION has active Calendar watch
                calendar_watch = service_supabase.table('push_subscriptions')\
                    .select('id')\
                    .eq('ext_connection_id', connection_id)\
                    .eq('provider', 'calendar')\
                    .eq('is_active', True)\
                    .execute()

                needs_setup = not gmail_watch.data or not calendar_watch.data

                if needs_setup and setup_count + error_count >= SETUP_BATCH_SIZE:
                    logger.info(f"⏸️ Batch limit reached ({SETUP_BATCH_SIZE}), deferring remaining setups to next run")
                    break

                if needs_setup:
                    logger.info(f"🔧 Setting up watches for connection {connection_id[:8]}... (user {user_id[:8]}...)")

                    # Get Google services
                    gmail_service, calendar_service, _ = get_google_services_for_connection(
                        connection_id,
                        service_supabase
                    )

                    if not gmail_service and not calendar_service:
                        logger.warning(f"⚠️ Could not get Google services for connection {connection_id[:8]}...")
                        error_count += 1
                        continue

                    # Actually set up missing watches (#20 fix)
                    gmail_needed = not gmail_watch.data and gmail_service
                    calendar_needed = not calendar_watch.data and calendar_service
                    gmail_ok = not gmail_needed  # True if not needed
                    calendar_ok = not calendar_needed  # True if not needed

                    # Set up Gmail watch if missing
                    if gmail_needed:
                        result = start_gmail_watch_service_role(
                            user_id, gmail_service, connection_id, service_supabase
                        )
                        if result.get('success'):
                            logger.info(f"✅ Gmail watch set up for user {user_id[:8]}...")
                            gmail_ok = True
                        else:
                            logger.warning(f"⚠️ Gmail watch setup failed: {result.get('error')}")

                    # Set up Calendar watch if missing
                    if calendar_needed:
                        result = start_calendar_watch_service_role(
                            user_id, calendar_service, connection_id, service_supabase
                        )
                        if result.get('success'):
                            logger.info(f"✅ Calendar watch set up for user {user_id[:8]}...")
                            calendar_ok = True
                        else:
                            logger.warning(f"⚠️ Calendar watch setup failed: {result.get('error')}")

                    # Count as success only if ALL needed watches were set up
                    if gmail_ok and calendar_ok:
                        if gmail_needed or calendar_needed:
                            setup_count += 1
                    else:
                        error_count += 1

            except Exception as e:
                logger.error(f"❌ Error checking user {user_id[:8]}...: {str(e)}")
                error_count += 1
                continue

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        logger.info(f"✅ CRON: Watch setup check completed in {duration:.2f}s")
        logger.info(f"📊 Results: {setup_count} setups needed, {error_count} errors")
        logger.info("=" * 80)

        capture_checkin(monitor_slug="setup-missing-watches", check_in_id=check_in_id, status=MonitorStatus.OK)

        return {
            "status": "completed",
            "duration_seconds": duration,
            "total_users": len(connections.data),
            "setup_needed": setup_count,
            "errors": error_count
        }

    except Exception as e:
        capture_checkin(monitor_slug="setup-missing-watches", check_in_id=check_in_id, status=MonitorStatus.ERROR)
        logger.error(f"❌ CRON: Watch setup check failed: {str(e)}")
        logger.exception("Full traceback:")
        logger.info("=" * 80)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Watch setup check failed: {str(e)}"
        )


@router.get("/watch-health", response_model=WatchHealthResponse)
async def cron_watch_health(authorization: str = Header(None)):
    """
    CRON JOB: Queue targeted recovery work for suspiciously silent watches.

    This is intentionally not a blanket "no notification in N minutes" sweep.
    It only marks streams dirty when the watch looks stale and the stream is
    otherwise idle, so quiet inboxes/calendars are not spam-synced.
    """
    logger.info("=" * 80)
    logger.info("🩺 CRON: Starting watch-health recovery scan")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone.utc).isoformat()}")

    if not verify_cron_auth(authorization):
        logger.warning("⚠️ Unauthorized cron attempt")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    start_time = datetime.now(timezone.utc)
    check_in_id = capture_checkin(monitor_slug="watch-health", status=MonitorStatus.IN_PROGRESS)

    try:
        service_supabase = get_service_role_client()
        now = datetime.now(timezone.utc)
        stale_after = timedelta(hours=get_watch_health_stale_hours())
        initial_grace_after = timedelta(hours=get_watch_health_initial_grace_hours())
        batch_size = get_watch_health_batch_size()
        priority = get_watch_health_priority()

        watch_result = service_supabase.table("push_subscriptions")\
            .select(
                "id, ext_connection_id, provider, created_at, expiration, "
                "notification_count, last_notification_at"
            )\
            .in_("provider", list(WATCH_PROVIDER_TO_SYNC_KIND.keys()))\
            .eq("is_active", True)\
            .execute()

        active_watches = watch_result.data or []
        if not active_watches:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            capture_checkin(monitor_slug="watch-health", check_in_id=check_in_id, status=MonitorStatus.OK)
            return {
                "status": "completed",
                "message": "No active Google watches",
                "duration_seconds": duration,
                "checked": 0,
                "queued": 0,
                "errors": 0,
            }

        connection_ids = sorted(
            {
                str(watch["ext_connection_id"])
                for watch in active_watches
                if watch.get("ext_connection_id")
            }
        )

        state_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
        if connection_ids:
            state_result = service_supabase.table("connection_sync_state")\
                .select(
                    "connection_id, provider, sync_kind, dirty, retry_count, "
                    "next_retry_at, lease_expires_at, last_sync_finished_at"
                )\
                .in_("connection_id", connection_ids)\
                .in_("sync_kind", list(WATCH_PROVIDER_TO_SYNC_KIND.values()))\
                .execute()

            state_by_key = {
                (row["connection_id"], row["sync_kind"]): row
                for row in (state_result.data or [])
            }

        queued = 0
        errors = 0
        ordered_watches = sorted(
            active_watches,
            key=lambda watch: (
                _parse_optional_datetime(watch.get("last_notification_at"))
                or _parse_optional_datetime(watch.get("created_at"))
                or now
            ),
        )

        for watch in ordered_watches:
            if queued + errors >= batch_size:
                break

            connection_id = watch.get("ext_connection_id")
            watch_provider = watch.get("provider")
            sync_kind = WATCH_PROVIDER_TO_SYNC_KIND.get(watch_provider)
            if not connection_id or not sync_kind:
                continue

            sync_state = state_by_key.get((connection_id, sync_kind))
            reason = _classify_watch_health_candidate(
                watch,
                sync_state,
                now=now,
                stale_after=stale_after,
                initial_grace_after=initial_grace_after,
            )
            if not reason:
                continue

            stream_provider = (sync_state or {}).get("provider") or "google"
            try:
                marked = mark_stream_dirty(
                    service_supabase,
                    connection_id,
                    stream_provider,
                    sync_kind,
                    priority=priority,
                    metadata={
                        "source": "watch-health-recovery",
                        "reason": reason,
                        "watch_id": watch.get("id"),
                        "watch_provider": watch_provider,
                    },
                )
                if marked:
                    queued += 1
                    logger.info(
                        "🩺 queued watch-health recovery for %s/%s (%s)",
                        connection_id[:8],
                        sync_kind,
                        reason,
                    )
            except Exception:
                errors += 1
                logger.exception(
                    "❌ Failed watch-health recovery mark for %s/%s",
                    connection_id[:8],
                    sync_kind,
                )

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        capture_checkin(monitor_slug="watch-health", check_in_id=check_in_id, status=MonitorStatus.OK)
        return {
            "status": "completed",
            "duration_seconds": duration,
            "checked": len(active_watches),
            "queued": queued,
            "errors": errors,
        }

    except Exception as e:
        capture_checkin(monitor_slug="watch-health", check_in_id=check_in_id, status=MonitorStatus.ERROR)
        logger.error(f"❌ CRON: Watch health scan failed: {str(e)}")
        logger.exception("Full traceback:")
        logger.info("=" * 80)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Watch health scan failed: {str(e)}"
        )


@router.get("/daily-verification", response_model=DailyVerificationResponse)
async def cron_daily_verification(authorization: str = Header(None)):
    """
    CRON JOB: Daily full sync for data integrity verification
    
    RUNS: Daily at 2:00 AM UTC
    
    PURPOSE: Ensures long-term data integrity
    - Performs full sync (not just incremental)
    - Catches any edge cases or drift
    - Verifies database matches Google's state
    - Runs for a rotating subset of users (to manage load)
    
    NOTE: Uses GET because Vercel cron jobs send GET requests by default
    """
    logger.info("=" * 80)
    logger.info("🕐 CRON: Starting daily verification sync")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone.utc).isoformat()}")
    
    # Verify authorization
    if not verify_cron_auth(authorization):
        logger.warning("⚠️ Unauthorized cron attempt")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    
    logger.info("✅ Authorization verified")
    start_time = datetime.now(timezone.utc)

    check_in_id = capture_checkin(monitor_slug="daily-verification", status=MonitorStatus.IN_PROGRESS)

    try:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.warning(
            "⚠️ CRON: Daily verification is currently disabled because full verification "
            "sync is not implemented in this endpoint"
        )
        capture_checkin(monitor_slug="daily-verification", check_in_id=check_in_id, status=MonitorStatus.OK)
        return {
            "status": "disabled",
            "message": "Daily verification sync is disabled until full verification implementation is added",
            "duration_seconds": duration,
            "verified": 0,
            "errors": 0,
        }

    except Exception as e:
        capture_checkin(monitor_slug="daily-verification", check_in_id=check_in_id, status=MonitorStatus.ERROR)
        logger.error(f"❌ CRON: Daily verification failed: {str(e)}")
        logger.exception("Full traceback:")
        logger.info("=" * 80)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Daily verification failed: {str(e)}"
        )


@router.get("/analyze-emails", response_model=AnalyzeEmailsResponse)
async def analyze_unanalyzed_emails_cron(
    authorization: Optional[str] = Header(None)
):
    """
    AI Email Analysis Cron Job
    
    Analyzes any emails that haven't been processed by AI yet.
    This is a safety net to catch emails that failed analysis during sync.
    
    Runs every hour to ensure all emails are analyzed.
    """
    # Verify cron secret
    cron_secret = os.getenv("CRON_SECRET", "")
    if not cron_secret:
        logger.warning("⚠️ CRON_SECRET not configured - skipping auth check")
    elif authorization != f"Bearer {cron_secret}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization"
        )
    
    from api.services.email.analyze_email_ai import analyze_unanalyzed_emails
    from lib.queue import queue_client

    logger.info("🤖 Starting AI email analysis cron job")

    check_in_id = capture_checkin(monitor_slug="analyze-emails", status=MonitorStatus.IN_PROGRESS)

    try:
        # Try to enqueue via QStash; fall back to inline
        if queue_client.enqueue("analyze-emails", {"limit": 100}):
            logger.info("✅ Enqueued analyze-emails job to QStash")
            capture_checkin(monitor_slug="analyze-emails", check_in_id=check_in_id, status=MonitorStatus.OK)
            return {
                "status": "queued",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "analyzed_count": 0,
                "message": "Job enqueued to worker"
            }

        # Fallback: inline processing
        analyzed_count = analyze_unanalyzed_emails(limit=100)

        logger.info(f"✅ AI analysis cron completed: {analyzed_count} emails analyzed")

        capture_checkin(monitor_slug="analyze-emails", check_in_id=check_in_id, status=MonitorStatus.OK)

        return {
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "analyzed_count": analyzed_count,
            "message": f"Successfully analyzed {analyzed_count} emails"
        }

    except Exception as e:
        capture_checkin(monitor_slug="analyze-emails", check_in_id=check_in_id, status=MonitorStatus.ERROR)
        logger.error(f"❌ Error in AI analysis cron: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"AI analysis cron failed: {str(e)}"
        )


@router.get("/cleanup-orphaned-uploads", response_model=CleanupResponse)
async def cleanup_orphaned_uploads(authorization: Optional[str] = Header(None)):
    """
    CRON JOB: Clean up orphaned presigned uploads

    RUNS: Every hour

    PURPOSE: Cleans up files stuck in 'uploading' status for more than 1 hour
    - Finds files with status='uploading' older than 1 hour
    - Deletes them from R2 storage (if they exist)
    - Deletes the database records
    - Prevents orphaned files from accumulating

    This handles cases where:
    - Client got presigned URL but never uploaded
    - Client uploaded but never called /confirm
    - Network errors during upload flow
    """
    logger.info("=" * 80)
    logger.info("🕐 CRON: Starting orphaned upload cleanup")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone.utc).isoformat()}")

    # Verify authorization
    if not verify_cron_auth(authorization):
        logger.warning("⚠️ Unauthorized cron attempt")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    logger.info("✅ Authorization verified")
    start_time = datetime.now(timezone.utc)

    check_in_id = capture_checkin(monitor_slug="cleanup-orphaned-uploads", status=MonitorStatus.IN_PROGRESS)

    try:
        from lib.presigned_upload import PresignedUploadManager
        from lib.r2_client import get_r2_client

        # Use service role client to access all files (bypasses RLS)
        service_supabase = get_service_role_client()

        manager = PresignedUploadManager(
            r2_client=get_r2_client(),
            supabase_client=service_supabase,
        )

        deleted = manager.cleanup_orphaned(max_age_hours=1)

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        logger.info(f"✅ CRON: Orphaned upload cleanup completed in {duration:.2f}s")
        logger.info(f"📊 Results: {deleted} orphaned uploads deleted")
        logger.info("=" * 80)

        capture_checkin(monitor_slug="cleanup-orphaned-uploads", check_in_id=check_in_id, status=MonitorStatus.OK)

        return {
            "status": "completed",
            "duration_seconds": duration,
            "deleted": deleted,
        }

    except Exception as e:
        capture_checkin(monitor_slug="cleanup-orphaned-uploads", check_in_id=check_in_id, status=MonitorStatus.ERROR)
        logger.error(f"❌ CRON: Orphaned upload cleanup failed: {str(e)}")
        logger.exception("Full traceback:")
        logger.info("=" * 80)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cleanup failed: {str(e)}"
        )


@router.get("/cleanup-orphaned-chat-attachments", response_model=CleanupResponse)
async def cleanup_orphaned_chat_attachments(authorization: Optional[str] = Header(None)):
    """
    CRON JOB: Clean up orphaned chat attachments

    RUNS: Every hour

    PURPOSE: Cleans up chat attachments stuck in 'uploading' status for more than 1 hour
    - Finds chat_attachments with status='uploading' older than 1 hour
    - Deletes them from R2 storage (if they exist)
    - Deletes the database records
    - Prevents orphaned files from accumulating

    This handles cases where:
    - Client got presigned URL but never uploaded
    - Client uploaded but never called /confirm
    - Network errors during upload flow
    """
    logger.info("=" * 80)
    logger.info("🕐 CRON: Starting orphaned chat attachment cleanup")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone.utc).isoformat()}")

    # Verify authorization
    if not verify_cron_auth(authorization):
        logger.warning("⚠️ Unauthorized cron attempt")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    logger.info("✅ Authorization verified")
    start_time = datetime.now(timezone.utc)

    check_in_id = capture_checkin(monitor_slug="cleanup-orphaned-chat-attachments", status=MonitorStatus.IN_PROGRESS)

    try:
        from lib.r2_client import get_r2_client

        # Use service role client to access all attachments (bypasses RLS)
        service_supabase = get_service_role_client()
        r2_client = get_r2_client()

        # Find orphaned attachments (uploading status, older than 1 hour)
        one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

        orphaned_result = service_supabase.table("chat_attachments")\
            .select("id, r2_key, thumbnail_r2_key")\
            .eq("status", "uploading")\
            .lt("created_at", one_hour_ago)\
            .execute()

        orphaned_attachments = orphaned_result.data or []
        deleted_count = 0
        r2_deleted_count = 0

        logger.info(f"📊 Found {len(orphaned_attachments)} orphaned chat attachments")

        for att in orphaned_attachments:
            try:
                # Delete from R2 (ignore if not found)
                if att.get("r2_key"):
                    try:
                        r2_client.delete_file(att["r2_key"])
                        r2_deleted_count += 1
                    except Exception:
                        pass

                if att.get("thumbnail_r2_key"):
                    try:
                        r2_client.delete_file(att["thumbnail_r2_key"])
                        r2_deleted_count += 1
                    except Exception:
                        pass

                # Delete database record
                service_supabase.table("chat_attachments")\
                    .delete()\
                    .eq("id", att["id"])\
                    .execute()

                deleted_count += 1
                logger.info(f"🗑️ Deleted orphaned chat attachment: {att['id']}")

            except Exception as e:
                logger.error(f"❌ Failed to delete orphaned attachment {att['id']}: {e}")

        elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()

        logger.info("✅ CRON: Orphaned chat attachment cleanup completed")
        logger.info(f"📊 Deleted {deleted_count} DB records, {r2_deleted_count} R2 files")
        logger.info(f"⏱️ Elapsed time: {elapsed_time:.2f}s")
        logger.info("=" * 80)

        capture_checkin(monitor_slug="cleanup-orphaned-chat-attachments", check_in_id=check_in_id, status=MonitorStatus.OK)

        return {
            "status": "success",
            "deleted_db_records": deleted_count,
            "deleted_r2_files": r2_deleted_count,
            "elapsed_seconds": elapsed_time
        }

    except Exception as e:
        capture_checkin(monitor_slug="cleanup-orphaned-chat-attachments", check_in_id=check_in_id, status=MonitorStatus.ERROR)
        logger.error(f"❌ CRON: Orphaned chat attachment cleanup failed: {str(e)}")
        logger.exception("Full traceback:")
        logger.info("=" * 80)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cleanup failed: {str(e)}"
        )


@router.get("/agent-health", response_model=AgentHealthResponse)
async def cron_agent_health(authorization: str = Header(None)):
    """
    CRON JOB: Agent sandbox health check

    RUNS: Every 5 minutes

    PURPOSE: Verify running agent sandboxes are healthy
    - Checks all agents with active E2B sandboxes
    - Marks unreachable sandboxes as error
    - Fails running tasks on dead sandboxes
    """
    logger.info("=" * 80)
    logger.info("🤖 CRON: Starting agent health check")

    if not verify_cron_auth(authorization):
        logger.warning("Unauthorized cron attempt")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    start_time = datetime.now(timezone.utc)

    try:
        from api.services.agents.health import check_agent_health
        result = check_agent_health()

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"🤖 CRON: Agent health check completed in {duration:.2f}s")

        return AgentHealthResponse(
            status=result.get("status", "ok"),
            checked=result.get("checked", 0),
            healthy=result.get("healthy", 0),
            errors=result.get("errors", 0),
            duration_seconds=duration,
        )
    except Exception as e:
        logger.error(f"🤖 CRON: Agent health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Agent health check failed: {str(e)}",
        )


@router.get("/health", response_model=CronHealthResponse)
async def cron_health():
    """
    Health check endpoint for cron jobs
    Verifies cron system is operational
    """
    return {
        "status": "healthy",
        "service": "cron-jobs",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "jobs": [
            {
                "name": "incremental-sync",
                "schedule": "Disabled",
                "description": "Broad sweep replaced by worker-driven next_reconcile_at reconciliation"
            },
            {
                "name": "renew-watches",
                "schedule": "Every hour",
                "description": "CRITICAL: Renews expiring watch subscriptions"
            },
            {
                "name": "setup-missing-watches",
                "schedule": "Disabled / manual recovery",
                "description": "Controlled recovery path for restoring missing watches"
            },
            {
                "name": "watch-health",
                "schedule": "Disabled / manual recovery",
                "description": "Queues targeted recovery work for suspiciously silent watches"
            },
            {
                "name": "daily-verification",
                "schedule": "Daily at 2:00 AM UTC",
                "description": "Full sync for data integrity"
            },
            {
                "name": "analyze-emails",
                "schedule": "Every hour",
                "description": "AI analysis for unanalyzed emails"
            },
            {
                "name": "cleanup-orphaned-uploads",
                "schedule": "Every hour",
                "description": "Cleans up presigned uploads that were never completed"
            },
            {
                "name": "cleanup-orphaned-chat-attachments",
                "schedule": "Every hour",
                "description": "Cleans up chat attachments that were never completed"
            },
            {
                "name": "agent-health",
                "schedule": "Every 5 minutes",
                "description": "Verifies running agent E2B sandboxes are healthy"
            }
        ]
    }
