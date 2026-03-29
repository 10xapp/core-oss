"""
Cron router - Scheduled background jobs for sync reliability
These jobs ensure data stays in sync even if webhooks fail

CRON JOB SCHEDULE:
==================

1. /api/cron/incremental-sync (Every 15 minutes)
   - Safety net for missed webhook notifications
   - Runs incremental sync for all active users
   - Catches any emails/events that webhooks missed

2. /api/cron/renew-watches (Every 6 hours)
   - CRITICAL: Prevents watch subscriptions from expiring
   - Gmail watches expire after 7 days
   - Calendar watches expire after configured time
   - Automatically renews watches before they expire

3. /api/cron/setup-missing-watches (Every hour)
   - Ensures all users have active watches
   - Sets up watches for new users
   - Recovers from watch setup failures

4. /api/cron/daily-verification (Daily at 2am)
   - Full sync for data integrity verification
   - Catches any edge cases or long-term drift
   - Runs full sync for a subset of users each day
"""
from fastapi import APIRouter, HTTPException, status, Header
from typing import Optional
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
    sync_gmail_cron,
    sync_google_calendar_cron,
    renew_watch_service_role,
    start_gmail_watch_service_role,
    start_calendar_watch_service_role
)
from api.services.syncs.google_error_utils import is_permanent_google_api_error
from api.services.syncs.google_services import get_google_services_for_connection
from api.services.microsoft.microsoft_oauth_provider import (
    MicrosoftReauthRequiredError,
    get_valid_microsoft_credentials,
)
from api.services.microsoft.microsoft_webhook_provider import renew_microsoft_subscription

from pydantic import BaseModel
from typing import List

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/cron", tags=["cron"])


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


@router.get("/incremental-sync", response_model=IncrementalSyncResponse)
async def cron_incremental_sync(authorization: str = Header(None)):
    """
    CRON JOB: Incremental sync for all active users
    
    RUNS: Every 15 minutes
    
    PURPOSE: Safety net to catch any missed webhook notifications
    - Runs incremental sync for all users with active connections
    - Only syncs emails/events since last sync (efficient)
    - Ensures no data is lost if webhooks fail
    
    This job processes users in batches to handle rate limits gracefully.
    
    NOTE: Uses GET because Vercel cron jobs send GET requests by default
    """
    logger.info("=" * 80)
    logger.info("🕐 CRON: Starting incremental sync for all users")
    logger.info(f"⏰ Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"🔑 Authorization header present: {bool(authorization)}")
    logger.info(f"🌍 Environment: {settings.api_env}")
    
    # Verify authorization
    if not verify_cron_auth(authorization):
        logger.warning("⚠️ Unauthorized cron attempt - authorization failed")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    
    logger.info("✅ Authorization verified")
    start_time = datetime.now(timezone.utc)

    check_in_id = capture_checkin(monitor_slug="incremental-sync", status=MonitorStatus.IN_PROGRESS)

    try:
        from lib.queue import queue_client

        # Use service role client to access all user connections
        service_supabase = get_service_role_client()

        # Get all active connections (Google + Microsoft)
        connections = service_supabase.table('ext_connections')\
            .select('user_id, id, last_synced, provider')\
            .in_('provider', ['google', 'microsoft'])\
            .eq('is_active', True)\
            .execute()

        if not connections.data:
            logger.info("ℹ️ No active connections to sync")
            capture_checkin(monitor_slug="incremental-sync", check_in_id=check_in_id, status=MonitorStatus.OK)
            return {
                "status": "completed",
                "message": "No active connections",
                "users_processed": 0
            }

        total_connections = len(connections.data)
        logger.info(f"👥 Found {total_connections} active connections to sync")

        jobs_enqueued = 0
        jobs_failed = 0
        skipped_count = 0
        error_count = 0
        success_count = 0

        # Pre-filter stale connections once so all modes share skip behavior.
        stale_connections = []
        for conn in connections.data:
            last_synced = conn.get('last_synced')
            if last_synced:
                last_sync_dt = datetime.fromisoformat(last_synced.replace('Z', '+00:00'))
                if datetime.now(timezone.utc) - last_sync_dt < timedelta(minutes=10):
                    skipped_count += 1
                    continue
            stale_connections.append(conn)

        if not stale_connections:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            capture_checkin(monitor_slug="incremental-sync", check_in_id=check_in_id, status=MonitorStatus.OK)
            return {
                "status": "completed",
                "message": "No stale connections to sync",
                "duration_seconds": duration,
                "total_users": total_connections,
                "success": 0,
                "skipped": skipped_count,
                "errors": 0,
                "jobs_enqueued": 0,
                "jobs_failed": 0,
            }

        use_queue = queue_client.available
        batch_mode = is_cron_batch_mode_enabled()

        # --- Queue path: batched fanout (default) ---
        if use_queue and batch_mode:
            google_ids = [c['id'] for c in stale_connections if c.get('provider') == 'google']
            microsoft_ids = [c['id'] for c in stale_connections if c.get('provider') == 'microsoft']
            scheduled_connection_ids = set()
            dedup_bucket = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")

            batch_jobs = [
                ("sync-gmail", google_ids),
                ("sync-calendar", google_ids),
                ("sync-outlook", microsoft_ids),
                ("sync-outlook-calendar", microsoft_ids),
            ]

            for job_type, ids in batch_jobs:
                if not ids:
                    continue
                dedup_id = f"batch-{job_type}-{dedup_bucket}"
                if queue_client.enqueue_batch(job_type, ids, dedup_id=dedup_id):
                    jobs_enqueued += 1
                    scheduled_connection_ids.update(ids)
                else:
                    jobs_failed += 1
                    error_count += 1
                    logger.warning(f"⚠️ Queue publish failed for batch job {job_type} ({len(ids)} IDs)")

            success_count = len(scheduled_connection_ids)

        else:
            # --- Legacy path: per-connection queue OR inline fallback ---
            for conn in stale_connections:
                user_id = conn['user_id']
                provider = conn.get('provider')
                connection_id = conn['id']

                try:
                    if use_queue:
                        conn_enqueued = 0
                        conn_failed = 0
                        if provider == 'google':
                            if queue_client.enqueue_sync_for_connection(connection_id, "sync-gmail"):
                                conn_enqueued += 1
                            else:
                                conn_failed += 1
                            if queue_client.enqueue_sync_for_connection(connection_id, "sync-calendar"):
                                conn_enqueued += 1
                            else:
                                conn_failed += 1
                        elif provider == 'microsoft':
                            if queue_client.enqueue_sync_for_connection(connection_id, "sync-outlook"):
                                conn_enqueued += 1
                            else:
                                conn_failed += 1
                            if queue_client.enqueue_sync_for_connection(connection_id, "sync-outlook-calendar"):
                                conn_enqueued += 1
                            else:
                                conn_failed += 1

                        jobs_enqueued += conn_enqueued
                        jobs_failed += conn_failed

                        if conn_enqueued > 0:
                            success_count += 1

                        if conn_failed > 0:
                            logger.warning(
                                f"⚠️ Queue publish failures for connection {connection_id[:8]}...: "
                                f"{conn_failed} failed, {conn_enqueued} enqueued"
                            )
                            error_count += conn_failed
                        continue

                    # Inline fallback path (legacy behavior)
                    if provider == 'microsoft':
                        skipped_count += 1
                        continue

                    logger.info(f"🔄 Syncing connection {connection_id[:8]}... (user {user_id[:8]}...)")

                    gmail_service, calendar_service, _ = get_google_services_for_connection(
                        connection_id,
                        service_supabase
                    )

                    if not gmail_service and not calendar_service:
                        logger.warning(f"⚠️ Could not get Google services for connection {connection_id[:8]}...")
                        skipped_count += 1
                        continue

                    synced_gmail = False
                    synced_calendar = False

                    if gmail_service:
                        try:
                            result = sync_gmail_cron(
                                gmail_service=gmail_service,
                                connection_id=connection_id,
                                user_id=user_id,
                                service_supabase=service_supabase,
                                days_back=7
                            )
                            if result.get('status') == 'success':
                                synced_gmail = True
                            else:
                                if is_permanent_google_api_error(result.get('error')):
                                    logger.warning(f"⚠️ Gmail sync permanently unavailable for user {user_id[:8]}...: {result.get('error')}")
                                else:
                                    logger.error(f"❌ Gmail sync returned error: {result.get('error')}")
                        except Exception as e:
                            if is_permanent_google_api_error(e):
                                logger.warning(f"⚠️ Gmail sync permanently unavailable for user {user_id[:8]}...: {str(e)}")
                            else:
                                logger.error(f"❌ Gmail sync failed for user {user_id[:8]}...: {str(e)}")
                                logger.exception("Full traceback:")

                    if calendar_service:
                        try:
                            result = sync_google_calendar_cron(
                                calendar_service=calendar_service,
                                connection_id=connection_id,
                                user_id=user_id,
                                service_supabase=service_supabase,
                                days_past=30,
                                days_future=90
                            )
                            if result.get('status') == 'success':
                                synced_calendar = True
                            else:
                                if is_permanent_google_api_error(result.get('error')):
                                    logger.warning(f"⚠️ Calendar sync permanently unavailable for user {user_id[:8]}...: {result.get('error')}")
                                else:
                                    logger.error(f"❌ Calendar sync returned error: {result.get('error')}")
                        except Exception as e:
                            if is_permanent_google_api_error(e):
                                logger.warning(f"⚠️ Calendar sync permanently unavailable for user {user_id[:8]}...: {str(e)}")
                            else:
                                logger.error(f"❌ Calendar sync failed for user {user_id[:8]}...: {str(e)}")
                                logger.exception("Full traceback:")

                    if synced_gmail or synced_calendar:
                        service_supabase.table('ext_connections')\
                            .update({'last_synced': datetime.now(timezone.utc).isoformat()})\
                            .eq('id', connection_id)\
                            .execute()
                        success_count += 1
                    else:
                        skipped_count += 1

                except Exception as e:
                    logger.error(f"❌ Error syncing user {user_id[:8]}...: {str(e)}")
                    error_count += 1
                    continue

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        if use_queue:
            logger.info(f"✅ CRON: Enqueued {jobs_enqueued} sync jobs in {duration:.2f}s")
        else:
            logger.info(f"✅ CRON: Incremental sync completed in {duration:.2f}s")
        logger.info(
            f"📊 Results: {success_count} success, {skipped_count} skipped, {error_count} errors, "
            f"{jobs_enqueued} enqueued, {jobs_failed} failed enqueues"
        )
        logger.info("=" * 80)

        # Mark check-in as degraded whenever queue publishing has failures.
        checkin_status = MonitorStatus.OK
        if use_queue and jobs_failed > 0:
            checkin_status = MonitorStatus.ERROR
        capture_checkin(monitor_slug="incremental-sync", check_in_id=check_in_id, status=checkin_status)

        return {
            "status": "completed",
            "duration_seconds": duration,
            "total_users": total_connections,
            "success": success_count,
            "skipped": skipped_count,
            "errors": error_count,
            "jobs_enqueued": jobs_enqueued,
            "jobs_failed": jobs_failed,
            "batch_mode": bool(use_queue and batch_mode),
            "connections_considered": len(stale_connections),
        }

    except Exception as e:
        capture_checkin(monitor_slug="incremental-sync", check_in_id=check_in_id, status=MonitorStatus.ERROR)
        logger.error(f"❌ CRON: Incremental sync failed: {str(e)}")
        logger.exception("Full traceback:")
        logger.info("=" * 80)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Sync failed: {str(e)}"
        )


@router.get("/renew-watches", response_model=RenewWatchesResponse)
async def cron_renew_watches(authorization: str = Header(None)):
    """
    CRON JOB: Renew expiring watch subscriptions
    
    RUNS: Every 6 hours
    
    PURPOSE: CRITICAL - Prevents watch subscriptions from expiring
    - Gmail watches expire after 7 days
    - Calendar watches expire after configured time
    - This job renews any watches expiring within 24 hours
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

        # Get subscriptions expiring within 24 hours
        threshold_time = datetime.now(timezone.utc) + timedelta(hours=24)

        result = service_supabase.table('push_subscriptions')\
            .select('*, ext_connections!push_subscriptions_ext_connection_id_fkey!inner(id, user_id, is_active, access_token, refresh_token, token_expires_at, metadata)')\
            .eq('is_active', True)\
            .lt('expiration', threshold_time.isoformat())\
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
    
    RUNS: Every hour
    
    PURPOSE: Ensures all users have active watch subscriptions
    - Sets up watches for new users who just connected Google
    - Recovers from watch setup failures
    - Ensures no users are left without push notifications
    
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
                "schedule": "Every 15 minutes",
                "description": "Safety net for missed webhooks"
            },
            {
                "name": "renew-watches",
                "schedule": "Every 6 hours",
                "description": "CRITICAL: Renews expiring watch subscriptions"
            },
            {
                "name": "setup-missing-watches",
                "schedule": "Every hour",
                "description": "Ensures all users have active watches"
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
