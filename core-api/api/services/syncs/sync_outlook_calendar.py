"""
Microsoft Outlook Calendar Sync

Syncs calendar events from Outlook via Microsoft Graph API delta queries.
Similar pattern to sync_outlook.py for emails.

Key features:
- Delta queries for efficient incremental sync
- Stores calendar_delta_link in ext_connections.metadata
- Converts Microsoft recurrence to RRULE format
- Handles all-day events correctly
"""
import requests
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from lib.supabase_client import get_service_role_client
from lib.batch_utils import batch_upsert, get_existing_external_ids
from api.services.notifications.calendar_invites import (
    get_calendar_event_rows_by_external_ids,
    reconcile_calendar_invite_notifications,
)
from api.services.microsoft.microsoft_oauth_provider import MicrosoftOAuthProvider

logger = logging.getLogger(__name__)

# Microsoft Graph API
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

# Fields to select from calendar events
OUTLOOK_CALENDAR_FIELDS = ",".join([
    "id",
    "subject",
    "body",
    "bodyPreview",
    "start",
    "end",
    "location",
    "attendees",
    "isAllDay",
    "isCancelled",
    "recurrence",
    "showAs",
    "importance",
    "sensitivity",
    "organizer",
    "webLink",
    "createdDateTime",
    "lastModifiedDateTime"
])

# Day mapping for recurrence conversion
DAY_MAP = {
    'sunday': 'SU',
    'monday': 'MO',
    'tuesday': 'TU',
    'wednesday': 'WE',
    'thursday': 'TH',
    'friday': 'FR',
    'saturday': 'SA',
}

# Frequency mapping for recurrence conversion
FREQ_MAP = {
    'daily': 'DAILY',
    'weekly': 'WEEKLY',
    'absoluteMonthly': 'MONTHLY',
    'relativeMonthly': 'MONTHLY',
    'absoluteYearly': 'YEARLY',
    'relativeYearly': 'YEARLY',
}


def _get_valid_access_token(connection_data: Dict[str, Any]) -> str:
    """Get a valid access token, refreshing if necessary."""
    token_expires_at = connection_data.get('token_expires_at')

    if token_expires_at:
        try:
            if isinstance(token_expires_at, str):
                expires = datetime.fromisoformat(token_expires_at.replace('Z', '+00:00'))
            else:
                expires = token_expires_at

            # If token expires in less than 5 minutes, refresh it
            if expires <= datetime.now(timezone.utc) + timedelta(minutes=5):
                logger.info("🔄 [Outlook Calendar] Token expired or expiring soon, refreshing...")
                oauth_provider = MicrosoftOAuthProvider()
                new_tokens = oauth_provider.refresh_access_token(connection_data)

                # Update connection in database
                supabase = get_service_role_client()
                supabase.table('ext_connections').update({
                    'access_token': new_tokens['access_token'],
                    'refresh_token': new_tokens.get('refresh_token', connection_data.get('refresh_token')),
                    'token_expires_at': (datetime.now(timezone.utc) + timedelta(seconds=new_tokens['expires_in'])).isoformat()
                }).eq('id', connection_data['id']).execute()

                return new_tokens['access_token']
        except Exception as e:
            logger.warning(f"⚠️ [Outlook Calendar] Token refresh check failed: {e}")

    return connection_data.get('access_token')


def _convert_recurrence_to_rrule(recurrence: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Convert Microsoft recurrence pattern to iCal RRULE string.

    Microsoft format:
    {
        "pattern": {
            "type": "weekly",
            "interval": 1,
            "daysOfWeek": ["monday", "wednesday", "friday"]
        },
        "range": {
            "type": "endDate",
            "startDate": "2024-01-01",
            "endDate": "2024-12-31"
        }
    }

    RRULE format:
    "RRULE:FREQ=WEEKLY;INTERVAL=1;BYDAY=MO,WE,FR;UNTIL=20241231"
    """
    if not recurrence:
        return None

    pattern = recurrence.get('pattern', {})
    range_info = recurrence.get('range', {})

    pattern_type = pattern.get('type', 'daily')
    freq = FREQ_MAP.get(pattern_type, 'DAILY')

    parts = [f"FREQ={freq}"]

    # Add interval
    interval = pattern.get('interval', 1)
    if interval > 1:
        parts.append(f"INTERVAL={interval}")

    # Add days of week
    days_of_week = pattern.get('daysOfWeek', [])
    if days_of_week:
        days = ','.join(DAY_MAP.get(d.lower(), '') for d in days_of_week if d.lower() in DAY_MAP)
        if days:
            parts.append(f"BYDAY={days}")

    # Add day of month for monthly
    day_of_month = pattern.get('dayOfMonth')
    if day_of_month and pattern_type in ['absoluteMonthly', 'absoluteYearly']:
        parts.append(f"BYMONTHDAY={day_of_month}")

    # Add month for yearly
    month = pattern.get('month')
    if month and pattern_type in ['absoluteYearly', 'relativeYearly']:
        parts.append(f"BYMONTH={month}")

    # Add range constraints
    range_type = range_info.get('type', 'noEnd')
    if range_type == 'endDate':
        end_date = range_info.get('endDate', '')
        if end_date:
            until = end_date.replace('-', '')
            parts.append(f"UNTIL={until}")
    elif range_type == 'numbered':
        count = range_info.get('numberOfOccurrences', 1)
        parts.append(f"COUNT={count}")

    return "RRULE:" + ";".join(parts)


def _parse_outlook_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a Microsoft Graph calendar event into our standard schema.

    Field mapping:
    - subject -> title
    - bodyPreview or body.content -> description
    - location.displayName -> location
    - attendees[].emailAddress.address -> attendees
    - start.dateTime -> start_time
    - end.dateTime -> end_time
    - isAllDay -> is_all_day
    - recurrence.pattern -> recurrence (RRULE conversion)
    """
    # Parse location
    location = event.get('location', {})
    location_str = location.get('displayName', '') if isinstance(location, dict) else ''

    # Parse attendees
    attendees = []
    for attendee in event.get('attendees', []):
        email_addr = attendee.get('emailAddress', {})
        if email_addr:
            attendees.append({
                'email': email_addr.get('address', ''),
                'name': email_addr.get('name', ''),
                'response': attendee.get('status', {}).get('response', 'none'),
            })

    # Parse start/end times
    start = event.get('start', {})
    end = event.get('end', {})

    start_time = start.get('dateTime')
    end_time = end.get('dateTime')
    # Note: Outlook provides timezone info but we currently normalize to UTC.
    # Logging for debugging timezone issues.
    start_tz = start.get('timeZone', 'UTC')
    end_tz = end.get('timeZone', 'UTC')
    logger.debug(f"Event {event.get('id', 'unknown')}: start_tz={start_tz}, end_tz={end_tz}")

    # Handle all-day events
    is_all_day = event.get('isAllDay', False)

    # Microsoft returns datetimes without timezone for all-day events
    # Need to ensure proper format for database
    if start_time and not start_time.endswith('Z') and '+' not in start_time:
        # Assume UTC if no timezone info
        start_time = start_time + 'Z' if not is_all_day else start_time + 'T00:00:00Z'
    if end_time and not end_time.endswith('Z') and '+' not in end_time:
        end_time = end_time + 'Z' if not is_all_day else end_time + 'T23:59:59Z'

    # Convert recurrence
    recurrence = event.get('recurrence')
    rrule = _convert_recurrence_to_rrule(recurrence) if recurrence else None

    # Map showAs to status
    show_as = event.get('showAs', 'busy')
    status_map = {
        'free': 'free',
        'tentative': 'tentative',
        'busy': 'confirmed',
        'oof': 'confirmed',  # Out of office
        'workingElsewhere': 'confirmed',
        'unknown': 'confirmed'
    }
    status = status_map.get(show_as, 'confirmed')

    # Check if cancelled
    if event.get('isCancelled', False):
        status = 'cancelled'

    return {
        "external_id": event.get('id'),
        "title": event.get('subject', 'Untitled Event'),
        "description": event.get('bodyPreview', ''),
        "location": location_str,
        "start_time": start_time,
        "end_time": end_time,
        "is_all_day": is_all_day,
        "status": status,
        "attendees": attendees,
        "recurrence": rrule,
        "raw_item": event,
    }


def sync_outlook_calendar_incremental(
    user_id: str,
    connection_id: str,
    connection_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Sync Outlook calendar events using delta queries.

    Uses deltaLink stored in ext_connections.metadata['calendar_delta_link']
    for efficient incremental sync.

    Args:
        user_id: User's ID
        connection_id: The ext_connection_id
        connection_data: Connection data with tokens and metadata

    Returns:
        Dict with sync results
    """
    try:
        supabase = get_service_role_client()
        access_token = _get_valid_access_token(connection_data)
        account_email = connection_data.get('provider_email')
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get calendar delta link from metadata
        metadata = connection_data.get('metadata', {}) or {}
        delta_link = metadata.get('calendar_delta_link')

        all_events = []
        new_delta_link = None

        # Build initial URL or use delta link
        if delta_link:
            url = delta_link
            logger.info(f"🔄 [Outlook Calendar] Using existing deltaLink for user {user_id[:8]}...")
        else:
            # First sync - get events from 7 days ago to 60 days forward
            logger.info(f"🔄 [Outlook Calendar] No deltaLink found, starting fresh delta sync for user {user_id[:8]}...")

            now = datetime.now(timezone.utc)
            start_date = (now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
            end_date = (now + timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Use /calendarView/delta (NOT /calendar/events/delta)
            # Delta queries don't support $select, $filter, $orderby, $expand, $search
            # Only startDateTime and endDateTime are allowed
            url = f"{GRAPH_API_URL}/me/calendarView/delta"
            url += f"?startDateTime={start_date}"
            url += f"&endDateTime={end_date}"

        # Paginate through all results
        page_count = 0
        max_pages = 20  # Safety limit

        while url and page_count < max_pages:
            page_count += 1

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 401:
                logger.error("❌ [Outlook Calendar] Token expired or invalid")
                return {"success": False, "error": "Token expired"}

            if response.status_code != 200:
                error_data = response.json().get('error', {})
                error_msg = error_data.get('message', response.text)
                logger.error(f"❌ [Outlook Calendar] API error: {error_msg}")
                return {"success": False, "error": error_msg}

            data = response.json()
            events = data.get('value', [])
            all_events.extend(events)

            logger.info(f"📄 [Outlook Calendar] Fetched delta page {page_count}, {len(all_events)} changes so far...")

            # Check for more pages
            if '@odata.nextLink' in data:
                url = data['@odata.nextLink']
            else:
                url = None
                # Only store deltaLink when all pages are exhausted
                new_delta_link = data.get('@odata.deltaLink')

        logger.info(f"📊 [Outlook Calendar] Found {len(all_events)} delta changes")

        # Separate deleted events from active events
        deleted_ids = []
        active_events = []
        for event in all_events:
            if '@removed' in event:
                event_id = event.get('id')
                if event_id:
                    deleted_ids.append(event_id)
            else:
                active_events.append(event)

        previous_rows_by_external_id = get_calendar_event_rows_by_external_ids(
            client=supabase,
            user_id=user_id,
            external_ids=deleted_ids,
            connection_id=connection_id,
        ) if deleted_ids else {}

        # Delete removed events
        deleted_count = 0
        delete_had_errors = False
        for event_id in deleted_ids:
            try:
                supabase.table('calendar_events').delete()\
                    .eq('user_id', user_id)\
                    .eq('external_id', event_id)\
                    .execute()
                deleted_count += 1
            except Exception as e:
                logger.error(f"❌ [Outlook Calendar] Error deleting event {event_id}: {str(e)}")
                delete_had_errors = True

        # Parse all active events for batch upsert
        all_events_data = []
        all_external_ids = []

        for event in active_events:
            try:
                event_data = _parse_outlook_event(event)
                if not event_data.get('external_id'):
                    continue
                organizer = event.get('organizer') or {}
                organizer_email = organizer.get('emailAddress', {}).get('address') if isinstance(organizer, dict) else None

                db_record = {
                    'user_id': user_id,
                    'ext_connection_id': connection_id,
                    'external_id': event_data['external_id'],
                    'title': event_data['title'],
                    'description': event_data['description'],
                    'location': event_data['location'],
                    'start_time': event_data['start_time'],
                    'end_time': event_data['end_time'],
                    'is_all_day': event_data['is_all_day'],
                    'status': event_data['status'],
                    'attendees': event_data.get('attendees', []),
                    'organizer_email': organizer_email,
                    'is_organizer': (
                        isinstance(account_email, str)
                        and isinstance(organizer_email, str)
                        and account_email.strip().lower()
                        == organizer_email.strip().lower()
                    ),
                    'synced_at': datetime.now(timezone.utc).isoformat(),
                    'raw_item': event_data['raw_item']
                }
                all_events_data.append(db_record)
                all_external_ids.append(event_data['external_id'])
            except Exception as e:
                logger.error(f"❌ [Outlook Calendar] Error parsing event {event.get('id', 'unknown')}: {str(e)}")

        # Get existing IDs to calculate new vs updated counts
        existing_ids = get_existing_external_ids(
            supabase, 'calendar_events', user_id, all_external_ids
        )
        added_count = len([eid for eid in all_external_ids if eid not in existing_ids])
        updated_count = len(all_external_ids) - added_count

        # Batch upsert all events
        batch_had_errors = False
        if all_events_data:
            logger.info(f"📤 [Outlook Calendar] Batch upserting {len(all_events_data)} events...")
            result = batch_upsert(
                supabase,
                'calendar_events',
                all_events_data,
                'user_id,external_id'
            )
            if result['errors']:
                logger.warning(f"⚠️ [Outlook Calendar] Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True

        if not batch_had_errors:
            current_rows_by_external_id = get_calendar_event_rows_by_external_ids(
                client=supabase,
                user_id=user_id,
                external_ids=all_external_ids,
                connection_id=connection_id,
            )
            reconcile_calendar_invite_notifications(
                client=supabase,
                user_id=user_id,
                account_email=account_email,
                previous_rows_by_external_id=previous_rows_by_external_id,
                current_rows_by_external_id=current_rows_by_external_id,
            )

        # Store new deltaLink in metadata (only if no errors)
        if new_delta_link and not (batch_had_errors or delete_had_errors):
            updated_metadata = {**metadata, 'calendar_delta_link': new_delta_link}
            supabase.table('ext_connections').update({
                'metadata': updated_metadata,
                'last_synced': datetime.now(timezone.utc).isoformat()
            }).eq('id', connection_id).execute()
        elif batch_had_errors or delete_had_errors:
            logger.warning("⚠️ [Outlook Calendar] Skipping delta_link update due to batch/delete errors")

        logger.info(f"✅ [Outlook Calendar] Sync completed: {added_count} added, {updated_count} updated, {deleted_count} deleted")

        return {
            "success": True,
            "new_events": added_count,
            "updated_events": updated_count,
            "deleted_events": deleted_count,
            "total_processed": len(all_events)
        }

    except Exception as e:
        logger.error(f"❌ [Outlook Calendar] Error during sync: {str(e)}")
        import traceback
        logger.error(f"❌ [Outlook Calendar] Traceback: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}


def sync_outlook_calendar(
    user_id: str,
    connection_id: str,
    connection_data: Dict[str, Any],
    days_back: int = 7,
    days_forward: int = 60
) -> Dict[str, Any]:
    """
    Full Outlook calendar sync (non-delta).

    Used for initial sync or when delta link is invalid.

    Args:
        user_id: User's ID
        connection_id: The ext_connection_id
        connection_data: Connection data with tokens
        days_back: Number of days in the past to sync
        days_forward: Number of days in the future to sync

    Returns:
        Dict with sync results
    """
    try:
        supabase = get_service_role_client()
        access_token = _get_valid_access_token(connection_data)
        account_email = connection_data.get('provider_email')
        headers = {"Authorization": f"Bearer {access_token}"}

        # Calculate date range
        now = datetime.now(timezone.utc)
        start_date = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date = (now + timedelta(days=days_forward)).strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"🔄 [Outlook Calendar] Starting full sync for user {user_id[:8]}... ({days_back} days back, {days_forward} days forward)")

        # Build URL
        url = f"{GRAPH_API_URL}/me/calendar/events"
        url += f"?$select={OUTLOOK_CALENDAR_FIELDS}"
        url += f"&$filter=start/dateTime ge '{start_date}' and end/dateTime le '{end_date}'"
        url += "&$orderby=start/dateTime"
        url += "&$top=100"

        all_events = []
        page_count = 0
        max_pages = 10

        while url and page_count < max_pages:
            page_count += 1

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                error_data = response.json().get('error', {})
                error_msg = error_data.get('message', response.text)
                logger.error(f"❌ [Outlook Calendar] API error: {error_msg}")
                return {"success": False, "error": error_msg}

            data = response.json()
            events = data.get('value', [])
            all_events.extend(events)

            logger.info(f"📄 [Outlook Calendar] Fetched page {page_count}, {len(all_events)} events so far...")

            url = data.get('@odata.nextLink')

        logger.info(f"📊 [Outlook Calendar] Found {len(all_events)} events")

        # Parse all events for batch upsert
        all_events_data = []
        all_external_ids = []
        previous_rows_by_external_id: Dict[str, Dict[str, Any]] = {}

        for event in all_events:
            try:
                event_data = _parse_outlook_event(event)
                if not event_data.get('external_id'):
                    continue
                organizer = event.get('organizer') or {}
                organizer_email = organizer.get('emailAddress', {}).get('address') if isinstance(organizer, dict) else None

                db_record = {
                    'user_id': user_id,
                    'ext_connection_id': connection_id,
                    'external_id': event_data['external_id'],
                    'title': event_data['title'],
                    'description': event_data['description'],
                    'location': event_data['location'],
                    'start_time': event_data['start_time'],
                    'end_time': event_data['end_time'],
                    'is_all_day': event_data['is_all_day'],
                    'status': event_data['status'],
                    'attendees': event_data.get('attendees', []),
                    'organizer_email': organizer_email,
                    'is_organizer': (
                        isinstance(account_email, str)
                        and isinstance(organizer_email, str)
                        and account_email.strip().lower()
                        == organizer_email.strip().lower()
                    ),
                    'synced_at': datetime.now(timezone.utc).isoformat(),
                    'raw_item': event_data['raw_item']
                }
                all_events_data.append(db_record)
                all_external_ids.append(event_data['external_id'])
            except Exception as e:
                logger.error(f"❌ [Outlook Calendar] Error parsing event: {str(e)}")

        # Get existing IDs to calculate new vs updated counts
        existing_ids = get_existing_external_ids(
            supabase, 'calendar_events', user_id, all_external_ids
        )
        synced_count = len([eid for eid in all_external_ids if eid not in existing_ids])
        updated_count = len(all_external_ids) - synced_count

        deleted_count = 0
        stale_rows = []
        if all_external_ids:
            stale_rows = supabase.table('calendar_events').select('*')\
                .eq('user_id', user_id)\
                .eq('ext_connection_id', connection_id)\
                .gte('start_time', start_date)\
                .lte('start_time', end_date)\
                .not_.in_('external_id', all_external_ids)\
                .execute().data or []
            delete_result = supabase.table('calendar_events').delete()\
                .eq('user_id', user_id)\
                .eq('ext_connection_id', connection_id)\
                .gte('start_time', start_date)\
                .lte('start_time', end_date)\
                .not_.in_('external_id', all_external_ids)\
                .execute()
            deleted_count = len(delete_result.data) if delete_result.data else 0
        else:
            stale_rows = supabase.table('calendar_events').select('*')\
                .eq('user_id', user_id)\
                .eq('ext_connection_id', connection_id)\
                .gte('start_time', start_date)\
                .lte('start_time', end_date)\
                .execute().data or []
            delete_result = supabase.table('calendar_events').delete()\
                .eq('user_id', user_id)\
                .eq('ext_connection_id', connection_id)\
                .gte('start_time', start_date)\
                .lte('start_time', end_date)\
                .execute()
            deleted_count = len(delete_result.data) if delete_result.data else 0

        for stale_row in stale_rows:
            external_id = stale_row.get('external_id')
            if external_id:
                previous_rows_by_external_id[external_id] = stale_row

        # Batch upsert all events
        batch_had_errors = False
        if all_events_data:
            logger.info(f"📤 [Outlook Calendar] Batch upserting {len(all_events_data)} events...")
            result = batch_upsert(
                supabase,
                'calendar_events',
                all_events_data,
                'user_id,external_id'
            )
            if result['errors']:
                logger.warning(f"⚠️ [Outlook Calendar] Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True

        if not batch_had_errors:
            current_rows_by_external_id = get_calendar_event_rows_by_external_ids(
                client=supabase,
                user_id=user_id,
                external_ids=all_external_ids,
                connection_id=connection_id,
            )
            reconcile_calendar_invite_notifications(
                client=supabase,
                user_id=user_id,
                account_email=account_email,
                previous_rows_by_external_id=previous_rows_by_external_id,
                current_rows_by_external_id=current_rows_by_external_id,
            )

        # Update last synced only if no errors
        if not batch_had_errors:
            supabase.table('ext_connections').update({
                'last_synced': datetime.now(timezone.utc).isoformat()
            }).eq('id', connection_id).execute()
        else:
            logger.warning("⚠️ [Outlook Calendar] Skipping last_synced update due to batch errors")

        logger.info(f"✅ [Outlook Calendar] Full sync completed: {synced_count} added, {updated_count} updated, {deleted_count} deleted")

        return {
            "success": True,
            "new_events": synced_count,
            "updated_events": updated_count,
            "deleted_events": deleted_count,
            "total_events": len(all_events)
        }

    except Exception as e:
        logger.error(f"❌ [Outlook Calendar] Error during full sync: {str(e)}")
        import traceback
        logger.error(f"❌ [Outlook Calendar] Traceback: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}
