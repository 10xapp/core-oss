"""
Sync router - Manual sync triggers and watch management endpoints

Supports both Google and Microsoft providers.
"""
from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timezone
from api.dependencies import get_current_user_jwt
from api.services.syncs import (
    setup_watches_for_user,
    sync_gmail_incremental,
    sync_outlook_incremental,
    sync_outlook_calendar_incremental,
    sync_google_calendar_cron
)
from api.services.calendar.google_api_helpers import get_google_calendar_service_for_account
from lib.supabase_client import get_authenticated_supabase_client, get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sync", tags=["sync"])


class EnsureWatchesRequest(BaseModel):
    user_id: str


class EnsureWatchesResponse(BaseModel):
    status: str
    gmail: Dict[str, Any]
    calendar: Dict[str, Any]
    message: str


class WatchProviderStatus(BaseModel):
    """Status of a provider's watch subscriptions."""
    active: bool
    watch: Optional[Dict[str, Any]] = None
    history: Optional[List[Dict[str, Any]]] = None


class WatchSummary(BaseModel):
    """Summary of watch subscription counts."""
    total_watches: int
    active_watches: int
    gmail_active: bool
    calendar_active: bool


class WatchStatusResponse(BaseModel):
    """Response model for watch status endpoint."""
    user_id: str
    gmail: WatchProviderStatus
    calendar: WatchProviderStatus
    summary: WatchSummary


class AccountSyncResult(BaseModel):
    """Sync result for a single account."""
    account_id: str
    provider: str
    email: Optional[str] = None
    is_primary: Optional[bool] = None
    gmail: Optional[Dict[str, Any]] = None
    calendar: Optional[Dict[str, Any]] = None
    outlook: Optional[Dict[str, Any]] = None
    outlook_calendar: Optional[Dict[str, Any]] = None


class SyncSummary(BaseModel):
    """Summary of sync operation."""
    total_accounts: int
    google_accounts: int
    microsoft_accounts: int


class TriggerSyncResponse(BaseModel):
    """Response model for trigger sync endpoint."""
    user_id: str
    accounts: List[AccountSyncResult]
    summary: Optional[SyncSummary] = None


@router.post("/ensure-watches")
async def ensure_watches(
    request: EnsureWatchesRequest,
    user_jwt: str = Depends(get_current_user_jwt)
) -> EnsureWatchesResponse:
    """
    Ensure watch subscriptions exist for a user.
    
    This endpoint:
    1. Checks if the user has active Gmail and Calendar watches
    2. Sets up missing watches
    3. Returns the status of both watches
    
    RECOMMENDED: Call this on every login to ensure watches are always active.
    
    Returns:
        - status: "all_active" | "setup_completed" | "setup_failed" | "no_connection"
        - gmail: Watch status and details
        - calendar: Watch status and details
        - message: Human-readable description
    """
    user_id = request.user_id
    
    try:
        logger.info(f"🔍 Checking watch subscriptions for user {user_id[:8]}...")
        
        auth_supabase = get_authenticated_supabase_client(user_jwt)
        
        # Check if user has an active Google connection
        connection = auth_supabase.table('ext_connections')\
            .select('id, provider, is_active, scopes')\
            .eq('user_id', user_id)\
            .eq('provider', 'google')\
            .eq('is_active', True)\
            .execute()
        
        if not connection.data:
            logger.info(f"ℹ️ No active Google connection for user {user_id[:8]}...")
            return EnsureWatchesResponse(
                status="no_connection",
                gmail={"active": False, "reason": "No Google connection"},
                calendar={"active": False, "reason": "No Google connection"},
                message="No active Google connection found. Please connect your Google account."
            )
        
        # Check existing watches
        watches = auth_supabase.table('push_subscriptions')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('is_active', True)\
            .gte('expiration', datetime.now(timezone.utc).isoformat())\
            .execute()
        
        # Organize by provider
        existing_watches = {watch['provider']: watch for watch in watches.data}
        
        gmail_watch = existing_watches.get('gmail')
        calendar_watch = existing_watches.get('calendar')
        
        # Check if both are active and not expiring soon (> 24 hours)
        needs_setup = []
        
        if gmail_watch:
            expiration = datetime.fromisoformat(gmail_watch['expiration'].replace('Z', '+00:00'))
            time_until_expiry = expiration - datetime.now(timezone.utc)
            hours_until_expiry = time_until_expiry.total_seconds() / 3600
            
            if hours_until_expiry < 24:
                logger.info(f"⚠️ Gmail watch expires in {hours_until_expiry:.1f} hours, will renew")
                needs_setup.append('gmail')
        else:
            logger.info("❌ No active Gmail watch found")
            needs_setup.append('gmail')
        
        if calendar_watch:
            expiration = datetime.fromisoformat(calendar_watch['expiration'].replace('Z', '+00:00'))
            time_until_expiry = expiration - datetime.now(timezone.utc)
            hours_until_expiry = time_until_expiry.total_seconds() / 3600
            
            if hours_until_expiry < 24:
                logger.info(f"⚠️ Calendar watch expires in {hours_until_expiry:.1f} hours, will renew")
                needs_setup.append('calendar')
        else:
            logger.info("❌ No active Calendar watch found")
            needs_setup.append('calendar')
        
        # If all watches are active and healthy, return success
        if not needs_setup:
            logger.info(f"✅ All watches active for user {user_id[:8]}...")
            return EnsureWatchesResponse(
                status="all_active",
                gmail={
                    "active": True,
                    "channel_id": gmail_watch['channel_id'],
                    "expiration": gmail_watch['expiration'],
                    "notification_count": gmail_watch.get('notification_count', 0)
                },
                calendar={
                    "active": True,
                    "channel_id": calendar_watch['channel_id'],
                    "expiration": calendar_watch['expiration'],
                    "notification_count": calendar_watch.get('notification_count', 0)
                },
                message="All watch subscriptions are active and healthy"
            )
        
        # Set up missing/expiring watches
        logger.info(f"🔧 Setting up watches for user {user_id[:8]}...: {needs_setup}")
        
        result = setup_watches_for_user(user_id, user_jwt)
        
        # Determine overall status
        gmail_success = result['gmail'] and result['gmail'].get('success', False)
        calendar_success = result['calendar'] and result['calendar'].get('success', False)
        
        if gmail_success and calendar_success:
            status = "setup_completed"
            message = "Watch subscriptions set up successfully"
        elif gmail_success or calendar_success:
            status = "partial_setup"
            message = "Some watch subscriptions set up, others failed"
        else:
            status = "setup_failed"
            message = "Failed to set up watch subscriptions"
        
        logger.info(f"✅ Watch setup result for user {user_id[:8]}...: {status}")
        
        return EnsureWatchesResponse(
            status=status,
            gmail=result['gmail'] or {"active": False, "error": "Setup failed"},
            calendar=result['calendar'] or {"active": False, "error": "Setup failed"},
            message=message
        )
        
    except Exception as e:
        logger.error(f"❌ Error ensuring watches for user {user_id[:8]}...: {str(e)}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to ensure watch subscriptions: {str(e)}"
        )


@router.get("/watch-status/{user_id}", response_model=WatchStatusResponse)
async def get_watch_status(
    user_id: str,
    user_jwt: str = Depends(get_current_user_jwt)
):
    """
    Get the current status of watch subscriptions for a user.
    
    Returns detailed information about active watches including:
    - Whether watches are active
    - Expiration times
    - Notification counts
    - Last notification received
    """
    try:
        logger.info(f"📊 Getting watch status for user {user_id[:8]}...")
        
        auth_supabase = get_authenticated_supabase_client(user_jwt)
        
        # Get all watches for user (including inactive)
        watches = auth_supabase.table('push_subscriptions')\
            .select('*')\
            .eq('user_id', user_id)\
            .order('created_at', desc=True)\
            .execute()
        
        # Organize by provider
        gmail_watches = [w for w in watches.data if w['provider'] == 'gmail']
        calendar_watches = [w for w in watches.data if w['provider'] == 'calendar']
        
        # Get most recent active watch for each
        active_gmail = next((w for w in gmail_watches if w['is_active']), None)
        active_calendar = next((w for w in calendar_watches if w['is_active']), None)
        
        return {
            "user_id": user_id,
            "gmail": {
                "active": active_gmail is not None if active_gmail else False,
                "watch": active_gmail,
                "history": gmail_watches
            },
            "calendar": {
                "active": active_calendar is not None if active_calendar else False,
                "watch": active_calendar,
                "history": calendar_watches
            },
            "summary": {
                "total_watches": len(watches.data),
                "active_watches": sum(1 for w in watches.data if w['is_active']),
                "gmail_active": active_gmail is not None,
                "calendar_active": active_calendar is not None
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting watch status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get watch status: {str(e)}"
        )


@router.post("/trigger-sync/{user_id}", response_model=TriggerSyncResponse)
async def trigger_manual_sync(
    user_id: str,
    user_jwt: str = Depends(get_current_user_jwt)
):
    """
    Manually trigger a sync for ALL connected accounts (multi-account support).

    Supports:
    - Google: Gmail + Calendar sync (all connected Google accounts)
    - Microsoft: Outlook email + Calendar sync (all connected Microsoft accounts)

    Useful for:
    - Testing the sync functionality
    - Forcing an immediate sync
    - Recovering from sync issues
    """
    try:
        logger.info(f"🔄 Manual sync triggered for user {user_id[:8]}...")

        auth_supabase = get_authenticated_supabase_client(user_jwt)
        service_supabase = get_service_role_client()

        # Get ALL active connections
        connections = auth_supabase.table('ext_connections')\
            .select('id, provider, provider_email, access_token, refresh_token, token_expires_at, delta_link, metadata, is_primary')\
            .eq('user_id', user_id)\
            .eq('is_active', True)\
            .execute()

        # Decrypt tokens and group connections by provider
        all_connections = [decrypt_ext_connection_tokens(c) for c in (connections.data or [])]
        google_connections = [c for c in all_connections if c['provider'] == 'google']
        microsoft_connections = [c for c in all_connections if c['provider'] == 'microsoft']

        logger.info(f"📊 Found {len(google_connections)} Google + {len(microsoft_connections)} Microsoft accounts")

        results = {
            "user_id": user_id,
            "accounts": []
        }

        # Sync ALL Google accounts
        for conn in google_connections:
            account_id = conn['id']
            account_email = conn.get('provider_email', 'unknown')
            account_result = {
                "account_id": account_id,
                "provider": "google",
                "email": account_email,
                "is_primary": conn.get('is_primary', False),
                "gmail": None,
                "calendar": None
            }

            # Sync Gmail for this account
            try:
                # Use the primary sync for primary account (backwards compatible)
                if conn.get('is_primary'):
                    gmail_result = sync_gmail_incremental(user_id, user_jwt)
                else:
                    # For secondary accounts, we need per-account sync
                    # Currently sync_gmail_incremental only syncs primary
                    # TODO: Add per-account Gmail sync support
                    gmail_result = {"status": "skipped", "message": "Secondary Gmail sync not yet implemented"}

                account_result['gmail'] = gmail_result
                logger.info(f"✅ Gmail sync for {account_email}: {gmail_result.get('status', 'done')}")
            except Exception as e:
                logger.error(f"❌ Gmail sync failed for {account_email}: {str(e)}")
                account_result['gmail'] = {"success": False, "error": str(e)}

            # Sync Calendar for this account
            try:
                calendar_service, _ = get_google_calendar_service_for_account(user_id, user_jwt, account_id)
                if calendar_service:
                    calendar_result = sync_google_calendar_cron(
                        calendar_service=calendar_service,
                        connection_id=account_id,
                        user_id=user_id,
                        service_supabase=service_supabase,
                        days_past=7,
                        days_future=60
                    )
                    account_result['calendar'] = calendar_result
                    logger.info(f"✅ Calendar sync for {account_email}: {calendar_result.get('new_events', 0)} new")
                else:
                    account_result['calendar'] = {"success": False, "error": "Could not get calendar service"}
            except Exception as e:
                logger.error(f"❌ Calendar sync failed for {account_email}: {str(e)}")
                account_result['calendar'] = {"success": False, "error": str(e)}

            results['accounts'].append(account_result)

        # Sync ALL Microsoft accounts
        for conn in microsoft_connections:
            account_id = conn['id']
            account_email = conn.get('provider_email', 'unknown')
            account_result = {
                "account_id": account_id,
                "provider": "microsoft",
                "email": account_email,
                "is_primary": conn.get('is_primary', False),
                "outlook": None,
                "outlook_calendar": None
            }

            # Sync Outlook Email
            try:
                outlook_result = sync_outlook_incremental(
                    user_id=user_id,
                    connection_id=account_id,
                    connection_data=conn
                )
                account_result['outlook'] = outlook_result
                logger.info(f"✅ Outlook sync for {account_email}: {outlook_result.get('new_emails', 0)} new")
            except Exception as e:
                logger.error(f"❌ Outlook sync failed for {account_email}: {str(e)}")
                account_result['outlook'] = {"success": False, "error": str(e)}

            # Sync Outlook Calendar
            try:
                outlook_calendar_result = sync_outlook_calendar_incremental(
                    user_id=user_id,
                    connection_id=account_id,
                    connection_data=conn
                )
                account_result['outlook_calendar'] = outlook_calendar_result
                logger.info(f"✅ Outlook Calendar sync for {account_email}: {outlook_calendar_result.get('new_events', 0)} new")
            except Exception as e:
                logger.error(f"❌ Outlook Calendar sync failed for {account_email}: {str(e)}")
                account_result['outlook_calendar'] = {"success": False, "error": str(e)}

            results['accounts'].append(account_result)

        # Summary
        results['summary'] = {
            "total_accounts": len(results['accounts']),
            "google_accounts": len(google_connections),
            "microsoft_accounts": len(microsoft_connections)
        }

        logger.info(f"✅ Manual sync completed for {len(results['accounts'])} accounts")
        return results

    except Exception as e:
        logger.error(f"❌ Error triggering manual sync: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger sync: {str(e)}"
        )

