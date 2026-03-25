"""
Calendar Webhook Service - Processes Google Calendar push notifications

Handles real-time sync when Calendar sends push notifications.
Includes proper token refresh and sync token management.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

from googleapiclient.errors import HttpError

from lib.supabase_client import get_service_role_client
from lib.batch_utils import batch_upsert
from api.services.google_auth import (
    get_calendar_service_for_webhook,
    GoogleAuthError
)
from api.services.calendar.event_parser import parse_google_event_to_data
from api.services.notifications.calendar_invites import (
    get_calendar_event_rows_by_external_ids,
    reconcile_calendar_invite_notifications,
)

logger = logging.getLogger(__name__)

# Time window for full sync fallback
FULL_SYNC_DAYS_PAST = 7
FULL_SYNC_DAYS_FUTURE = 30
FULL_SYNC_MAX_RESULTS = 100


def process_calendar_notification(
    channel_id: str,
    resource_state: str
) -> Dict[str, Any]:
    """
    Process a Google Calendar push notification.

    Args:
        channel_id: The UUID of the notification channel
        resource_state: "sync" (initial) or "exists" (change notification)

    Returns:
        Dict with processing results
    """
    logger.info(f"📅 Processing Calendar notification: channel={channel_id}, state={resource_state}")

    # Handle sync message (initial verification from Google)
    if resource_state == "sync":
        logger.info(f"✅ Calendar sync verification received for channel {channel_id}")
        return {"status": "ok", "message": "Sync verified"}

    # Handle actual change notification
    if resource_state != "exists":
        logger.info(f"ℹ️ Ignoring Calendar notification with state: {resource_state}")
        return {"status": "ok", "message": f"Unhandled state: {resource_state}"}

    supabase = get_service_role_client()

    # Find subscription by channel_id
    subscription = supabase.table('push_subscriptions')\
        .select(
            '*, ext_connections!push_subscriptions_ext_connection_id_fkey!inner('
            'user_id, access_token, refresh_token, token_expires_at, metadata, provider_email'
            ')'
        )\
        .eq('channel_id', channel_id)\
        .eq('provider', 'calendar')\
        .eq('is_active', True)\
        .execute()

    if not subscription.data:
        logger.warning(f"⚠️ No active subscription found for channel {channel_id}")
        return {"status": "ok", "message": "No active subscription"}

    sub_data = subscription.data[0]
    user_id = sub_data['ext_connections']['user_id']
    account_email = sub_data['ext_connections'].get('provider_email')
    sync_token = sub_data.get('sync_token')
    subscription_id = sub_data['id']
    connection_id = sub_data['ext_connection_id']

    logger.info(f"🔄 Processing Calendar changes for user {user_id}")

    # Update notification count
    notification_count = sub_data.get('notification_count', 0) + 1
    supabase.table('push_subscriptions')\
        .update({
            'notification_count': notification_count,
            'last_notification_at': datetime.now(timezone.utc).isoformat()
        })\
        .eq('id', subscription_id)\
        .execute()

    # Get valid credentials with automatic refresh
    try:
        calendar_service, _ = get_calendar_service_for_webhook(sub_data, supabase)
    except GoogleAuthError as e:
        logger.error(f"❌ Authentication failed for user {user_id}: {str(e)}")
        return {"status": "error", "message": f"Auth failed: {str(e)}"}

    # Use incremental sync if we have a sync_token
    result = {"status": "ok"}
    if sync_token:
        result = _incremental_sync(
            calendar_service, supabase, user_id, connection_id,
            subscription_id, sync_token, account_email
        )
        # If sync token expired, result will indicate fallback needed
        if result.get('fallback_needed'):
            sync_token = None

    # Full sync if no sync_token or it expired
    if not sync_token:
        result = _full_sync(
            calendar_service, supabase, user_id, connection_id, subscription_id, account_email
        )

    return result


def _incremental_sync(
    calendar_service,
    supabase,
    user_id: str,
    connection_id: str,
    subscription_id: str,
    sync_token: str,
    account_email: Optional[str],
) -> Dict[str, Any]:
    """
    Perform incremental sync using sync token.
    """
    logger.info("📅 Using incremental sync with sync token")

    try:
        # Paginate through all changed events
        all_events: List[Dict[str, Any]] = []
        new_sync_token = None
        page_token = None
        page_count = 0

        while True:
            request_kwargs: Dict[str, Any] = {
                'calendarId': 'primary',
                'syncToken': sync_token,
                'maxResults': 50,
            }
            if page_token:
                request_kwargs['pageToken'] = page_token

            events_result = calendar_service.events().list(**request_kwargs).execute()

            page_events = events_result.get('items', [])
            all_events.extend(page_events)
            page_count += 1

            # nextSyncToken is only present on the LAST page
            new_sync_token = events_result.get('nextSyncToken') or new_sync_token
            page_token = events_result.get('nextPageToken')

            if not page_token:
                break

        logger.info(f"📅 Found {len(all_events)} changed events across {page_count} page(s)")

        # Separate cancelled and active events
        cancelled_ids: List[str] = []
        events_to_upsert: List[Dict[str, Any]] = []

        for event in all_events:
            if event.get('status') == 'cancelled':
                cancelled_ids.append(event['id'])
            else:
                event_data = _parse_event_to_data(event, user_id, connection_id)
                events_to_upsert.append(event_data)

        previous_rows_by_external_id = get_calendar_event_rows_by_external_ids(
            client=supabase,
            user_id=user_id,
            external_ids=cancelled_ids,
            connection_id=connection_id,
        ) if cancelled_ids else {}

        # Delete cancelled events
        deleted_count = 0
        for event_id in cancelled_ids:
            try:
                supabase.table('calendar_events')\
                    .delete()\
                    .eq('user_id', user_id)\
                    .eq('external_id', event_id)\
                    .execute()
                deleted_count += 1
                logger.debug(f"🗑️ Deleted cancelled event: {event_id}")
            except Exception as e:
                logger.error(f"❌ Error deleting event {event_id}: {str(e)}")

        # Batch upsert active events
        synced_count = 0
        batch_had_errors = False
        if events_to_upsert:
            logger.info(f"📤 Batch upserting {len(events_to_upsert)} events...")
            result = batch_upsert(
                supabase,
                'calendar_events',
                events_to_upsert,
                'user_id,external_id'
            )
            synced_count = result['success_count']
            if result['errors']:
                logger.warning(f"⚠️ Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True

        if not batch_had_errors:
            current_rows_by_external_id = get_calendar_event_rows_by_external_ids(
                client=supabase,
                user_id=user_id,
                external_ids=[
                    event_data['external_id']
                    for event_data in events_to_upsert
                    if event_data.get('external_id')
                ],
                connection_id=connection_id,
            )
            reconcile_calendar_invite_notifications(
                client=supabase,
                user_id=user_id,
                account_email=account_email,
                previous_rows_by_external_id=previous_rows_by_external_id,
                current_rows_by_external_id=current_rows_by_external_id,
            )

        # Update sync token only after ALL pages processed and no batch errors
        if new_sync_token and not batch_had_errors:
            supabase.table('push_subscriptions')\
                .update({
                    'sync_token': new_sync_token,
                    'updated_at': datetime.now(timezone.utc).isoformat()
                })\
                .eq('id', subscription_id)\
                .execute()
            logger.info("🔄 Updated sync token")
        elif batch_had_errors:
            logger.warning("⚠️ Skipping sync_token update due to batch errors")

        logger.info(f"✅ Calendar incremental sync: {synced_count} synced, {deleted_count} deleted")
        return {"status": "ok", "synced": synced_count, "deleted": deleted_count}

    except HttpError as e:
        if e.resp.status == 410:
            # Sync token expired - need full sync
            logger.warning("⚠️ Sync token expired (410 Gone), falling back to full sync")
            return {"status": "ok", "fallback_needed": True}
        elif e.resp.status == 401:
            logger.error("❌ Authentication error (401) - token may have been revoked")
            return {"status": "error", "message": "Authentication failed", "code": 401}
        else:
            logger.error(f"❌ Calendar API error: {str(e)}")
            return {"status": "error", "message": str(e)}
    except Exception as e:
        logger.error(f"❌ Incremental sync failed: {str(e)}")
        return {"status": "error", "message": str(e)}


def _full_sync(
    calendar_service,
    supabase,
    user_id: str,
    connection_id: str,
    subscription_id: str,
    account_email: Optional[str],
) -> Dict[str, Any]:
    """
    Perform full sync fetching events in a time window.

    Gets events from FULL_SYNC_DAYS_PAST days ago to FULL_SYNC_DAYS_FUTURE days ahead.
    """
    logger.info("📅 Performing full calendar sync")

    try:
        now = datetime.now(timezone.utc)
        time_min = (now - timedelta(days=FULL_SYNC_DAYS_PAST)).isoformat()
        time_max = (now + timedelta(days=FULL_SYNC_DAYS_FUTURE)).isoformat()

        # Paginate through ALL events in the time range
        all_events: List[Dict[str, Any]] = []
        new_sync_token = None
        page_token = None
        page_count = 0

        while True:
            request_kwargs: Dict[str, Any] = {
                'calendarId': 'primary',
                'timeMin': time_min,
                'timeMax': time_max,
                'maxResults': FULL_SYNC_MAX_RESULTS,
                'singleEvents': True,
                'orderBy': 'startTime',
            }
            if page_token:
                request_kwargs['pageToken'] = page_token

            events_result = calendar_service.events().list(**request_kwargs).execute()

            page_events = events_result.get('items', [])
            all_events.extend(page_events)
            page_count += 1

            # nextSyncToken is only present on the LAST page
            new_sync_token = events_result.get('nextSyncToken') or new_sync_token
            page_token = events_result.get('nextPageToken')

            if not page_token:
                break

        events = all_events
        logger.info(f"📅 Found {len(events)} events in time range across {page_count} page(s)")

        # Get all external_ids from Google Calendar response
        google_event_ids = [event['id'] for event in events if event.get('id')]
        previous_rows_by_external_id: Dict[str, Dict[str, Any]] = {}

        # Delete local events that no longer exist in Google Calendar.
        # Safe because we paginated through ALL results before reaching here.
        deleted_count = 0
        if google_event_ids:
            stale_rows = supabase.table('calendar_events')\
                .select('*')\
                .eq('user_id', user_id)\
                .eq('ext_connection_id', connection_id)\
                .gte('start_time', time_min)\
                .lte('start_time', time_max)\
                .not_.in_('external_id', google_event_ids)\
                .execute().data or []

            # Delete events for this user that are NOT in Google's response
            # Only delete events within the sync time range to avoid removing future events
            delete_result = supabase.table('calendar_events')\
                .delete()\
                .eq('user_id', user_id)\
                .eq('ext_connection_id', connection_id)\
                .gte('start_time', time_min)\
                .lte('start_time', time_max)\
                .not_.in_('external_id', google_event_ids)\
                .execute()
            deleted_count = len(delete_result.data) if delete_result.data else 0
            for stale_row in stale_rows:
                external_id = stale_row.get('external_id')
                if external_id:
                    previous_rows_by_external_id[external_id] = stale_row
            if deleted_count > 0:
                logger.info(f"🗑️ Deleted {deleted_count} events no longer in Google Calendar")
        else:
            stale_rows = supabase.table('calendar_events')\
                .select('*')\
                .eq('user_id', user_id)\
                .eq('ext_connection_id', connection_id)\
                .gte('start_time', time_min)\
                .lte('start_time', time_max)\
                .execute().data or []

            # No events from Google in this time range - delete all local events in range
            delete_result = supabase.table('calendar_events')\
                .delete()\
                .eq('user_id', user_id)\
                .eq('ext_connection_id', connection_id)\
                .gte('start_time', time_min)\
                .lte('start_time', time_max)\
                .execute()
            deleted_count = len(delete_result.data) if delete_result.data else 0
            for stale_row in stale_rows:
                external_id = stale_row.get('external_id')
                if external_id:
                    previous_rows_by_external_id[external_id] = stale_row
            if deleted_count > 0:
                logger.info(f"🗑️ Deleted {deleted_count} events (Google Calendar empty for time range)")

        # Parse all events for batch upsert
        events_to_upsert: List[Dict[str, Any]] = []
        for event in events:
            event_data = _parse_event_to_data(event, user_id, connection_id)
            events_to_upsert.append(event_data)

        # Batch upsert all events
        synced_count = 0
        batch_had_errors = False
        if events_to_upsert:
            logger.info(f"📤 Batch upserting {len(events_to_upsert)} events...")
            result = batch_upsert(
                supabase,
                'calendar_events',
                events_to_upsert,
                'user_id,external_id'
            )
            synced_count = result['success_count']
            if result['errors']:
                logger.warning(f"⚠️ Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True

        if not batch_had_errors:
            current_rows_by_external_id = get_calendar_event_rows_by_external_ids(
                client=supabase,
                user_id=user_id,
                external_ids=google_event_ids,
                connection_id=connection_id,
            )
            reconcile_calendar_invite_notifications(
                client=supabase,
                user_id=user_id,
                account_email=account_email,
                previous_rows_by_external_id=previous_rows_by_external_id,
                current_rows_by_external_id=current_rows_by_external_id,
            )

        # Update sync token for future incremental syncs (only if no batch errors)
        if new_sync_token and not batch_had_errors:
            supabase.table('push_subscriptions')\
                .update({
                    'sync_token': new_sync_token,
                    'updated_at': datetime.now(timezone.utc).isoformat()
                })\
                .eq('id', subscription_id)\
                .execute()
            logger.info("🔄 Saved sync token after full sync")
        elif batch_had_errors:
            logger.warning("⚠️ Skipping sync_token update due to batch errors")

        logger.info(f"✅ Full calendar sync completed - synced {synced_count} events, deleted {deleted_count} events")
        return {"status": "ok", "synced": synced_count, "deleted": deleted_count, "full_sync": True}

    except HttpError as e:
        logger.error(f"❌ Full sync HTTP error: {str(e)}")
        return {"status": "error", "message": str(e)}
    except Exception as e:
        logger.error(f"❌ Full sync failed: {str(e)}")
        return {"status": "error", "message": str(e)}


def _parse_event_to_data(
    event: Dict[str, Any],
    user_id: str,
    connection_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Parse Google Calendar event into database format.

    Delegates to shared parser for consistent behavior across sync paths.
    """
    return parse_google_event_to_data(event, user_id, connection_id)
