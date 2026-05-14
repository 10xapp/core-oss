"""
Calendar sync for cron jobs - bypasses RLS using service role
"""
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
import logging
from googleapiclient.errors import HttpError
from lib.google_retry import GOOGLE_API_NUM_RETRIES

from lib.batch_utils import batch_upsert, get_existing_external_ids
from api.services.calendar.event_parser import parse_google_event_to_data
from api.services.notifications.calendar_invites import (
    get_calendar_event_rows_by_external_ids,
    reconcile_calendar_invite_notifications,
)
from api.services.syncs.connection_state import (
    batch_has_orphaned_user_error,
    deactivate_connection_with_subscriptions,
)
from api.services.syncs.google_error_utils import is_permanent_google_api_error

logger = logging.getLogger(__name__)


def sync_google_calendar_cron(
    calendar_service,
    connection_id: str,
    user_id: str,
    service_supabase,
    days_past: int = 30,
    days_future: int = 90
) -> Dict[str, Any]:
    """
    Sync calendar events from Google Calendar for cron jobs.
    Uses service role Supabase client to bypass RLS.

    Args:
        calendar_service: Google Calendar API service
        connection_id: External connection ID
        user_id: User's ID
        service_supabase: Service role Supabase client (bypasses RLS)
        days_past: Number of days in the past to sync (default 30)
        days_future: Number of days in the future to sync (default 90)

    Returns:
        Dict with sync results
    """
    synced_count = 0
    updated_count = 0
    connection_deactivated = False

    try:
        connection_info = service_supabase.table('ext_connections')\
            .select('provider_email')\
            .eq('id', connection_id)\
            .maybe_single()\
            .execute()
        connection_email = connection_info.data.get('provider_email') if connection_info and connection_info.data else None

        # Fetch events from Google Calendar with expanded time range
        sync_started_at = datetime.now(timezone.utc)
        sync_marker = sync_started_at.isoformat()
        time_min = (sync_started_at - timedelta(days=days_past)).isoformat()
        time_max = (sync_started_at + timedelta(days=days_future)).isoformat()

        page_token = None
        new_sync_token = None
        total_fetched = 0
        all_events_data = []
        all_external_ids = []

        # Handle pagination to get ALL events in the time range
        while True:
            logger.info(f"📥 Fetching events page (token: {page_token[:20] if page_token else 'first page'})")

            events_result = calendar_service.events().list(
                calendarId='primary',
                timeMin=time_min,
                timeMax=time_max,
                maxResults=250,  # Max allowed by API
                singleEvents=True,
                orderBy='startTime',
                pageToken=page_token
            ).execute(num_retries=GOOGLE_API_NUM_RETRIES)

            events = events_result.get('items', [])
            total_fetched += len(events)
            new_sync_token = events_result.get('nextSyncToken') or new_sync_token

            logger.info(f"📦 Processing {len(events)} events from this page (total so far: {total_fetched})")

            # Parse all events in this page
            for event in events:
                event_data = parse_google_event_to_data(event, user_id, connection_id, include_raw_item=True)
                # Stable marker for this run; enables safe stale-row deletion without huge NOT IN filters.
                event_data['synced_at'] = sync_marker
                all_events_data.append(event_data)
                all_external_ids.append(event_data['external_id'])

            # Check if there are more pages
            page_token = events_result.get('nextPageToken')
            if not page_token:
                break

        # Get existing IDs to calculate new vs updated counts
        existing_ids = get_existing_external_ids(
            service_supabase, 'calendar_events', user_id, all_external_ids
        )
        synced_count = len([eid for eid in all_external_ids if eid not in existing_ids])
        updated_count = len(all_external_ids) - synced_count
        previous_rows_by_external_id: Dict[str, Dict[str, Any]] = {}

        # Batch upsert all events
        batch_had_errors = False
        if all_events_data:
            logger.info(f"📤 Batch upserting {len(all_events_data)} events...")
            result = batch_upsert(
                service_supabase,
                'calendar_events',
                all_events_data,
                'user_id,external_id'
            )
            if result['errors']:
                logger.warning(f"⚠️ Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True
                if batch_has_orphaned_user_error(result['errors']):
                    deactivate_connection_with_subscriptions(
                        service_supabase,
                        connection_id,
                        reason="orphaned user detected during Google Calendar sync",
                    )
                    connection_deactivated = True

        # Delete local events no longer in Google (only within sync time range)
        deleted_count = 0
        if not batch_had_errors:
            try:
                stale_rows = []
                null_sync_stale_rows = []
                if all_external_ids:
                    stale_rows = service_supabase.table('calendar_events')\
                        .select('*')\
                        .eq('user_id', user_id)\
                        .eq('ext_connection_id', connection_id)\
                        .gte('start_time', time_min)\
                        .lte('start_time', time_max)\
                        .lt('synced_at', sync_marker)\
                        .execute().data or []

                    delete_result = service_supabase.table('calendar_events')\
                        .delete()\
                        .eq('user_id', user_id)\
                        .eq('ext_connection_id', connection_id)\
                        .gte('start_time', time_min)\
                        .lte('start_time', time_max)\
                        .lt('synced_at', sync_marker)\
                        .execute()
                    deleted_count = len(delete_result.data) if delete_result.data else 0

                    null_sync_stale_rows = service_supabase.table('calendar_events')\
                        .select('*')\
                        .eq('user_id', user_id)\
                        .eq('ext_connection_id', connection_id)\
                        .gte('start_time', time_min)\
                        .lte('start_time', time_max)\
                        .is_('synced_at', 'null')\
                        .execute().data or []

                    # Backfill cleanup for legacy rows that don't have synced_at populated.
                    null_sync_delete_result = service_supabase.table('calendar_events')\
                        .delete()\
                        .eq('user_id', user_id)\
                        .eq('ext_connection_id', connection_id)\
                        .gte('start_time', time_min)\
                        .lte('start_time', time_max)\
                        .is_('synced_at', 'null')\
                        .execute()
                    deleted_count += len(null_sync_delete_result.data) if null_sync_delete_result.data else 0
                else:
                    stale_rows = service_supabase.table('calendar_events')\
                        .select('*')\
                        .eq('user_id', user_id)\
                        .eq('ext_connection_id', connection_id)\
                        .gte('start_time', time_min)\
                        .lte('start_time', time_max)\
                        .execute().data or []

                    # Google returned zero events — delete all local events in range
                    delete_result = service_supabase.table('calendar_events')\
                        .delete()\
                        .eq('user_id', user_id)\
                        .eq('ext_connection_id', connection_id)\
                        .gte('start_time', time_min)\
                        .lte('start_time', time_max)\
                        .execute()
                    deleted_count = len(delete_result.data) if delete_result.data else 0

                for stale_row in [*stale_rows, *null_sync_stale_rows]:
                    external_id = stale_row.get('external_id')
                    if external_id:
                        previous_rows_by_external_id[external_id] = stale_row
                if deleted_count > 0:
                    logger.info(f"🗑️ Deleted {deleted_count} events no longer in Google Calendar")
            except Exception as e:
                logger.warning(f"⚠️ Delete reconciliation failed (non-fatal): {e}")

        if not batch_had_errors:
            current_rows_by_external_id = get_calendar_event_rows_by_external_ids(
                client=service_supabase,
                user_id=user_id,
                external_ids=all_external_ids,
                connection_id=connection_id,
            )
            try:
                reconcile_calendar_invite_notifications(
                    client=service_supabase,
                    user_id=user_id,
                    account_email=connection_email,
                    previous_rows_by_external_id=previous_rows_by_external_id,
                    current_rows_by_external_id=current_rows_by_external_id,
                )
            except Exception as e:
                logger.warning(f"⚠️ Calendar invite notification reconciliation failed (non-fatal): {e}")

        # Update last synced timestamp only if no errors occurred
        if not batch_had_errors:
            if new_sync_token:
                service_supabase.table('push_subscriptions')\
                    .update({
                        'sync_token': new_sync_token,
                        'updated_at': datetime.now(timezone.utc).isoformat(),
                    })\
                    .eq('ext_connection_id', connection_id)\
                    .eq('provider', 'calendar')\
                    .eq('is_active', True)\
                    .execute()
                logger.info("🔄 Saved sync token after calendar worker sync")

            service_supabase.table('ext_connections')\
                .update({'last_synced': datetime.now(timezone.utc).isoformat()})\
                .eq('id', connection_id)\
                .execute()
        else:
            logger.warning("⚠️ Skipping last_synced update due to batch errors")

        if connection_deactivated:
            return {
                "status": "quarantined",
                "message": "Connection deactivated because its user no longer exists",
                "new_events": synced_count,
                "updated_events": updated_count,
                "deleted_events": deleted_count,
                "total_events": synced_count + updated_count,
                "total_fetched": total_fetched,
            }

        logger.info(f"✅ Calendar sync complete: {synced_count} new, {updated_count} updated, {deleted_count} deleted (total fetched: {total_fetched})")

        return {
            "status": "success",
            "new_events": synced_count,
            "updated_events": updated_count,
            "deleted_events": deleted_count,
            "total_events": synced_count + updated_count,
            "total_fetched": total_fetched,
            "new_sync_token": new_sync_token,
        }

    except HttpError as e:
        if is_permanent_google_api_error(e):
            logger.warning(f"⚠️ Calendar API permanently unavailable for connection {connection_id[:8]}...: {str(e)}")
        else:
            logger.error(f"❌ Google Calendar API error: {str(e)}")
        return {
            "status": "error",
            "error": f"Google Calendar API error: {str(e)}",
            "new_events": synced_count,
            "updated_events": updated_count
        }
    except Exception as e:
        logger.error(f"❌ Error syncing calendar: {str(e)}")
        logger.exception("Full traceback:")
        return {
            "status": "error",
            "error": str(e),
            "new_events": synced_count,
            "updated_events": updated_count
        }


def _parse_calendar_event(
    event: Dict[str, Any],
    user_id: str,
    connection_id: str
) -> Dict[str, Any]:
    """Deprecated: use parse_google_event_to_data instead."""
    return parse_google_event_to_data(event, user_id, connection_id, include_raw_item=True)
