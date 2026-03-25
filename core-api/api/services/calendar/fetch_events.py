"""
Calendar service - Fetch operations for calendar events

Supports unified multi-account calendar view:
- Events include account metadata (account_email, account_provider)
- Response includes accounts_status for all connected calendar accounts
- Supports filtering by multiple accounts via account_ids parameter
"""
import threading
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from lib.supabase_client import supabase, get_authenticated_supabase_client, get_service_role_client
from api.services.calendar.event_parser import parse_google_event_to_data
from api.services.notifications.calendar_invites import (
    get_calendar_event_rows_by_external_ids,
    reconcile_calendar_invite_notifications,
)
import logging

logger = logging.getLogger(__name__)


def get_calendar_accounts_status(user_id: str, user_jwt: str) -> List[Dict[str, Any]]:
    """
    Get sync status for all active calendar accounts.

    Returns list of account status objects with:
    - connection_id: ext_connection_id
    - email: provider_email
    - provider: google/microsoft
    - avatar: profile picture URL
    - last_synced: last sync timestamp

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests

    Returns:
        List of account status dictionaries
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    try:
        result = auth_supabase.table('ext_connections')\
            .select('id, provider_email, provider, is_active, metadata, updated_at')\
            .eq('user_id', user_id)\
            .eq('is_active', True)\
            .order('account_order')\
            .execute()

        return [{
            'connection_id': acc['id'],
            'email': acc['provider_email'],
            'provider': acc['provider'],
            'avatar': acc.get('metadata', {}).get('picture') if acc.get('metadata') else None,
            'last_synced': acc.get('updated_at')
        } for acc in result.data or []]
    except Exception as e:
        logger.warning(f"Could not fetch calendar accounts status: {e}")
        return []


# Cache freshness threshold: trigger background sync if last sync is older than this
CACHE_STALE_THRESHOLD_MINUTES = 15
# Prevent thundering-herd syncs when many requests hit stale cache
SYNC_IN_FLIGHT_TTL_MINUTES = 10
_SYNC_IN_FLIGHT: Dict[str, datetime] = {}
_SYNC_IN_FLIGHT_LOCK = threading.Lock()


def _is_cache_stale(user_id: str, account_ids: Optional[List[str]] = None) -> bool:
    """
    Check if the calendar cache is stale by looking at the most recent
    last_notification_at on the user's calendar push subscriptions.

    When account_ids is provided, only checks those accounts — so a fresh
    account A won't mask a stale account B.

    Returns:
        True if any targeted subscription is stale (no recent sync activity)
    """
    try:
        svc_supabase = get_service_role_client()
        query = svc_supabase.table('push_subscriptions')\
            .select(
                'ext_connection_id, last_notification_at, updated_at, '
                'ext_connections!push_subscriptions_ext_connection_id_fkey!inner(user_id)'
            )\
            .eq('provider', 'calendar')\
            .eq('is_active', True)\
            .eq('ext_connections.user_id', user_id)

        if account_ids and len(account_ids) > 0:
            query = query.in_('ext_connection_id', account_ids)

        result = query.execute()

        if not result.data:
            return True

        threshold = datetime.now(timezone.utc) - timedelta(minutes=CACHE_STALE_THRESHOLD_MINUTES)

        # Stale if ANY targeted subscription is stale
        for sub in result.data:
            last_activity = sub.get('last_notification_at') or sub.get('updated_at')
            if not last_activity:
                return True
            last_dt = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
            if last_dt <= threshold:
                return True

        return False
    except Exception as e:
        logger.warning(f"Could not check cache freshness: {e}")
        return False


def _trigger_sync_if_stale(
    user_id: str,
    user_jwt: str,
    account_ids: Optional[List[str]] = None
) -> None:
    """
    If the calendar cache is stale, trigger a background sync in a separate thread.
    The caller still returns cached data immediately (stale-while-revalidate).
    """
    try:
        if not _is_cache_stale(user_id, account_ids):
            return

        started_at = datetime.now(timezone.utc)
        with _SYNC_IN_FLIGHT_LOCK:
            last_started = _SYNC_IN_FLIGHT.get(user_id)
            if last_started and (started_at - last_started) < timedelta(minutes=SYNC_IN_FLIGHT_TTL_MINUTES):
                logger.info(f"⏳ Background sync already in-flight for user {user_id[:8]}..., skipping")
                return
            _SYNC_IN_FLIGHT[user_id] = started_at

        logger.info(f"🔄 Calendar cache stale for user {user_id[:8]}..., triggering background sync")

        def _background_sync() -> None:
            try:
                accounts_status = get_calendar_accounts_status(user_id, user_jwt)
                auth_supabase = get_authenticated_supabase_client(user_jwt)

                for account in accounts_status:
                    conn_id = account['connection_id']
                    if account_ids and conn_id not in account_ids:
                        continue

                    provider = account.get('provider', 'google')
                    if provider == 'google':
                        _sync_single_google_account_events(user_id, user_jwt, conn_id, auth_supabase)
                    elif provider == 'microsoft':
                        _sync_single_microsoft_account_events(user_id, user_jwt, conn_id, auth_supabase)

                logger.info(f"✅ Background calendar sync completed for user {user_id[:8]}...")
            except Exception as e:
                logger.error(f"❌ Background calendar sync failed for user {user_id[:8]}...: {e}")
            finally:
                with _SYNC_IN_FLIGHT_LOCK:
                    if _SYNC_IN_FLIGHT.get(user_id) == started_at:
                        _SYNC_IN_FLIGHT.pop(user_id, None)

        thread = threading.Thread(target=_background_sync, daemon=True)
        thread.start()
    except Exception as e:
        logger.warning(f"Could not trigger background sync: {e}")


def _enrich_attendees_from_raw_item(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract rich attendee data from raw_item (Google/Microsoft API response).

    The attendees TEXT[] column stores simple email strings for filtering.
    Rich data (display_name, response_status) comes from raw_item.

    Returns:
        List of attendee objects with email, display_name, response_status
    """
    raw_item = event.get('raw_item') or {}
    raw_attendees = raw_item.get('attendees', [])

    if not raw_attendees:
        # Fallback: convert simple email strings to objects
        simple_attendees = event.get('attendees') or []
        return [
            {'email': email, 'display_name': None, 'response_status': None}
            for email in simple_attendees
            if isinstance(email, str)
        ]

    # Extract rich data from raw_item
    return [
        {
            'email': a.get('email'),
            'display_name': a.get('displayName'),
            'response_status': a.get('responseStatus'),
        }
        for a in raw_attendees
        if a.get('email')
    ]


def _map_event_with_account_metadata(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a calendar event with account metadata from joined ext_connections.
    Also enriches attendees with full details from raw_item.

    Args:
        event: Raw event dict with ext_connections join data

    Returns:
        Event dict with account_email, account_provider, account_avatar fields
        and enriched attendees with display_name, response_status
    """
    ext_conn = event.get('ext_connections', {}) or {}

    # Enrich attendees from raw_item
    enriched_attendees = _enrich_attendees_from_raw_item(event)

    # Build mapped event with account metadata
    mapped = {
        **event,
        'account_email': ext_conn.get('provider_email'),
        'account_provider': ext_conn.get('provider'),
        'account_avatar': ext_conn.get('metadata', {}).get('picture') if ext_conn.get('metadata') else None,
        'attendees': enriched_attendees,  # Override with enriched data
    }

    # Remove nested ext_connections from response (already flattened)
    if 'ext_connections' in mapped:
        del mapped['ext_connections']

    return mapped


def _sync_single_microsoft_account_events(
    user_id: str,
    user_jwt: str,
    connection_id: str,
    auth_supabase
) -> List[Dict[str, Any]]:
    """
    Sync events from a single Microsoft Outlook account.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT
        connection_id: The ext_connection_id to sync
        auth_supabase: Authenticated Supabase client

    Returns:
        List of synced event dictionaries
    """
    from api.services.calendar.microsoft_api_helpers import get_microsoft_calendar_service_for_account
    from api.services.syncs.sync_outlook_calendar import sync_outlook_calendar_incremental

    try:
        # Get Microsoft access token and connection data
        access_token, conn_id, connection_data = get_microsoft_calendar_service_for_account(
            user_id, user_jwt, connection_id
        )

        if not access_token or not conn_id or not connection_data:
            logger.warning(f"Could not get Microsoft service for connection {connection_id[:8]}...")
            return []

        # Run incremental sync (this saves events to DB)
        sync_result = sync_outlook_calendar_incremental(
            user_id=user_id,
            connection_id=conn_id,
            connection_data=connection_data
        )

        if not sync_result.get('success'):
            logger.error(f"Microsoft sync failed for {connection_id[:8]}...: {sync_result.get('error')}")
            return []

        # Query the synced events from DB
        result = auth_supabase.table('calendar_events')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('ext_connection_id', connection_id)\
            .order('start_time', desc=False)\
            .execute()

        events = result.data or []
        logger.info(f"✅ Synced {len(events)} events from Microsoft account {connection_id[:8]}...")
        return events

    except Exception as e:
        logger.error(f"Error syncing Microsoft account {connection_id[:8]}...: {str(e)}")
        return []


def _sync_single_google_account_events(
    user_id: str,
    user_jwt: str,
    connection_id: str,
    auth_supabase
) -> List[Dict[str, Any]]:
    """
    Sync events from a single Google Calendar account.

    Paginates through all Google Calendar API pages and batch-upserts
    into the database in a single operation (no per-event queries).

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT
        connection_id: The ext_connection_id to sync
        auth_supabase: Authenticated Supabase client

    Returns:
        List of synced event dictionaries
    """
    from googleapiclient.errors import HttpError
    from api.services.calendar.google_api_helpers import get_google_calendar_service_for_account
    from lib.batch_utils import batch_upsert

    try:
        service, conn_id = get_google_calendar_service_for_account(user_id, user_jwt, connection_id)

        if not service or not conn_id:
            logger.warning(f"Could not get Google service for connection {connection_id[:8]}...")
            return []

        connection_info = auth_supabase.table('ext_connections')\
            .select('provider_email')\
            .eq('id', connection_id)\
            .maybe_single()\
            .execute()
        connection_email = connection_info.data.get('provider_email') if connection_info and connection_info.data else None

        # Fetch events from Google Calendar (last 7 days to next 30 days)
        now = datetime.now(timezone.utc)
        sync_marker = now.isoformat()
        time_min = (now - timedelta(days=7)).isoformat()
        time_max = (now + timedelta(days=30)).isoformat()

        # Paginate through ALL pages
        all_events: List[Dict[str, Any]] = []
        page_token = None

        while True:
            request_kwargs: Dict[str, Any] = {
                'calendarId': 'primary',
                'timeMin': time_min,
                'timeMax': time_max,
                'maxResults': 100,
                'singleEvents': True,
                'orderBy': 'startTime',
            }
            if page_token:
                request_kwargs['pageToken'] = page_token

            events_result = service.events().list(**request_kwargs).execute()
            all_events.extend(events_result.get('items', []))

            page_token = events_result.get('nextPageToken')
            if not page_token:
                break

        logger.info(f"📅 Fetched {len(all_events)} events from Google API for {connection_id[:8]}...")

        # Parse all events using shared parser
        events_to_upsert: List[Dict[str, Any]] = []
        for event in all_events:
            event_data = parse_google_event_to_data(
                event, user_id, connection_id, include_raw_item=True
            )
            event_data['synced_at'] = sync_marker
            events_to_upsert.append(event_data)

        previous_rows_by_external_id: Dict[str, Dict[str, Any]] = {}

        # Batch upsert — single operation instead of per-event SELECT+INSERT/UPDATE
        synced_count = 0
        batch_had_errors = False
        if events_to_upsert:
            result = batch_upsert(
                auth_supabase,
                'calendar_events',
                events_to_upsert,
                'user_id,external_id'
            )
            synced_count = result['success_count']
            if result['errors']:
                logger.warning(f"⚠️ Batch upsert errors for {connection_id[:8]}...: {result['errors'][:3]}")
                batch_had_errors = True

        if not batch_had_errors:
            stale_rows = []
            null_sync_stale_rows = []
            if events_to_upsert:
                stale_rows = auth_supabase.table('calendar_events')\
                    .select('*')\
                    .eq('user_id', user_id)\
                    .eq('ext_connection_id', connection_id)\
                    .gte('start_time', time_min)\
                    .lte('start_time', time_max)\
                    .lt('synced_at', sync_marker)\
                    .execute().data or []
                null_sync_stale_rows = auth_supabase.table('calendar_events')\
                    .select('*')\
                    .eq('user_id', user_id)\
                    .eq('ext_connection_id', connection_id)\
                    .gte('start_time', time_min)\
                    .lte('start_time', time_max)\
                    .is_('synced_at', 'null')\
                    .execute().data or []

                auth_supabase.table('calendar_events')\
                    .delete()\
                    .eq('user_id', user_id)\
                    .eq('ext_connection_id', connection_id)\
                    .gte('start_time', time_min)\
                    .lte('start_time', time_max)\
                    .lt('synced_at', sync_marker)\
                    .execute()
                auth_supabase.table('calendar_events')\
                    .delete()\
                    .eq('user_id', user_id)\
                    .eq('ext_connection_id', connection_id)\
                    .gte('start_time', time_min)\
                    .lte('start_time', time_max)\
                    .is_('synced_at', 'null')\
                    .execute()
            else:
                stale_rows = auth_supabase.table('calendar_events')\
                    .select('*')\
                    .eq('user_id', user_id)\
                    .eq('ext_connection_id', connection_id)\
                    .gte('start_time', time_min)\
                    .lte('start_time', time_max)\
                    .execute().data or []
                auth_supabase.table('calendar_events')\
                    .delete()\
                    .eq('user_id', user_id)\
                    .eq('ext_connection_id', connection_id)\
                    .gte('start_time', time_min)\
                    .lte('start_time', time_max)\
                    .execute()

            for stale_row in [*stale_rows, *null_sync_stale_rows]:
                external_id = stale_row.get('external_id')
                if external_id:
                    previous_rows_by_external_id[external_id] = stale_row

            current_rows_by_external_id = get_calendar_event_rows_by_external_ids(
                client=auth_supabase,
                user_id=user_id,
                external_ids=[event_data['external_id'] for event_data in events_to_upsert if event_data.get('external_id')],
                connection_id=connection_id,
            )
            reconcile_calendar_invite_notifications(
                client=auth_supabase,
                user_id=user_id,
                account_email=connection_email,
                previous_rows_by_external_id=previous_rows_by_external_id,
                current_rows_by_external_id=current_rows_by_external_id,
            )

        # Update last synced timestamp only if no batch errors
        if not batch_had_errors:
            auth_supabase.table('ext_connections')\
                .update({'last_synced': datetime.now(timezone.utc).isoformat()})\
                .eq('id', connection_id)\
                .execute()
        else:
            logger.warning(f"⚠️ Skipping last_synced update for {connection_id[:8]}... due to batch errors")

        logger.info(f"✅ Synced {synced_count} events from Google account {connection_id[:8]}...")

        # Return events from DB so they have proper IDs
        db_result = auth_supabase.table('calendar_events')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('ext_connection_id', connection_id)\
            .gte('start_time', time_min)\
            .lte('start_time', time_max)\
            .order('start_time', desc=False)\
            .execute()

        return db_result.data or []

    except HttpError as e:
        logger.error(f"Google Calendar API error for {connection_id[:8]}...: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Error syncing Google account {connection_id[:8]}...: {str(e)}")
        return []


def get_events(
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    Fetch calendar events for a user with optional date filtering
    """
    query = supabase.table('calendar_events')\
        .select('*')\
        .eq('user_id', user_id)\
        .order('start_time', desc=False)\
        .limit(limit)
    
    if start_date:
        query = query.gte('start_time', start_date)
    
    if end_date:
        query = query.lte('start_time', end_date)
    
    result = query.execute()
    
    return {
        "events": result.data,
        "count": len(result.data)
    }


def get_upcoming_events(user_id: str, limit: int = 10) -> Dict[str, Any]:
    """
    Get upcoming calendar events for a user
    """
    now = datetime.utcnow().isoformat() + "Z"
    
    result = supabase.table('calendar_events')\
        .select('*')\
        .eq('user_id', user_id)\
        .gte('start_time', now)\
        .order('start_time', desc=False)\
        .limit(limit)\
        .execute()
    
    return {
        "events": result.data,
        "count": len(result.data)
    }


def get_events_filtered(
    user_id: str,
    user_jwt: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    Fetch calendar events with optional date filtering using authenticated client.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        start_date: Optional start date in ISO 8601 format
        end_date: Optional end date in ISO 8601 format
        limit: Maximum number of events to return
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    query = auth_supabase.table('calendar_events')\
        .select('*')\
        .eq('user_id', user_id)\
        .order('start_time', desc=False)\
        .limit(limit)

    if start_date:
        query = query.gte('start_time', start_date)

    if end_date:
        query = query.lte('start_time', end_date)

    result = query.execute()

    return {
        "events": result.data,
        "count": len(result.data)
    }


def _filter_owned_account_ids(
    user_id: str,
    account_ids: List[str],
    user_jwt: str,
) -> List[str]:
    """Filter account_ids to only those owned by the authenticated user.

    Args:
        user_id: Authenticated user's ID
        account_ids: Requested account IDs to validate
        user_jwt: User's Supabase JWT

    Returns:
        List of account_ids that belong to the user
    """
    if not account_ids:
        return []

    auth_supabase = get_authenticated_supabase_client(user_jwt)
    result = auth_supabase.table('ext_connections')\
        .select('id')\
        .eq('user_id', user_id)\
        .in_('id', account_ids)\
        .execute()

    owned_ids = {row['id'] for row in (result.data or [])}
    filtered = [aid for aid in account_ids if aid in owned_ids]

    if len(filtered) < len(account_ids):
        dropped = set(account_ids) - owned_ids
        logger.warning(
            f"IDOR blocked: user {user_id[:8]}... requested account_ids "
            f"not owned: {[d[:8] + '...' for d in dropped]}"
        )

    return filtered


def get_all_events(
    user_id: str,
    user_jwt: str,
    account_ids: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get all calendar events for a user with unified multi-account support.
    Smart caching: Tries DB first, falls back to Google Calendar API if empty.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        account_ids: Optional list of ext_connection_ids to filter by specific accounts.
                     If None or empty, returns events from all accounts (unified view).

    Returns:
        Dict with events (including account metadata), count, source, unified flag,
        and accounts_status for all connected calendar accounts.
    """

    # Track whether caller explicitly requested account filtering
    requested_filtering = account_ids is not None and len(account_ids) > 0

    # Validate ownership of requested account_ids
    if requested_filtering:
        account_ids = _filter_owned_account_ids(user_id, account_ids, user_jwt)
        # All requested IDs were foreign — return empty, don't widen to unified view
        if not account_ids:
            accounts_status = get_calendar_accounts_status(user_id, user_jwt)
            return {
                "events": [],
                "count": 0,
                "source": "filtered",
                "unified": False,
                "account_ids": [],
                "accounts_status": accounts_status
            }

    # Determine if this is a unified view (all accounts) or filtered
    is_unified = not requested_filtering

    # Use authenticated Supabase client
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Try fetching from database first with JOIN to get account metadata
    query = auth_supabase.table('calendar_events')\
        .select('*, ext_connections(provider_email, provider, metadata)')\
        .eq('user_id', user_id)\
        .order('start_time', desc=False)

    # Filter by accounts if specified (supports multiple accounts)
    if account_ids and len(account_ids) > 0:
        if len(account_ids) == 1:
            query = query.eq('ext_connection_id', account_ids[0])
        else:
            query = query.in_('ext_connection_id', account_ids)

    result = query.execute()

    # If we have events in DB, return them with account metadata
    if result.data and len(result.data) > 0:
        logger.info(f"✅ Found {len(result.data)} cached events (unified: {is_unified})")

        # Map events with account metadata
        mapped_events = [_map_event_with_account_metadata(e) for e in result.data]

        # Get accounts status for frontend
        accounts_status = get_calendar_accounts_status(user_id, user_jwt)

        # Check cache freshness — if stale, trigger background sync
        _trigger_sync_if_stale(user_id, user_jwt, account_ids)

        return {
            "events": mapped_events,
            "count": len(mapped_events),
            "source": "cache",
            "unified": is_unified,
            "account_ids": account_ids,
            "accounts_status": accounts_status
        }
    
    # Otherwise, try to fetch from all connected calendar accounts
    logger.info("📦 No cached events found, syncing from all connected accounts")

    # Get all active calendar accounts
    accounts_status = get_calendar_accounts_status(user_id, user_jwt)

    if not accounts_status:
        return {
            "events": [],
            "count": 0,
            "source": "none",
            "unified": is_unified,
            "account_ids": account_ids,
            "accounts_status": []
        }

    # Sync events from all accounts (or filtered accounts if specified)
    all_synced_events = []
    synced_accounts = 0

    for account in accounts_status:
        connection_id = account['connection_id']
        provider = account.get('provider', 'google')

        # Skip if filtering by specific accounts and this one isn't included
        if account_ids and connection_id not in account_ids:
            continue

        if provider == 'google':
            events = _sync_single_google_account_events(
                user_id, user_jwt, connection_id, auth_supabase
            )
            all_synced_events.extend(events)
            if events:
                synced_accounts += 1
        elif provider == 'microsoft':
            events = _sync_single_microsoft_account_events(
                user_id, user_jwt, connection_id, auth_supabase
            )
            all_synced_events.extend(events)
            if events:
                synced_accounts += 1
        else:
            logger.info(f"Skipping {provider} account {connection_id[:8]}... (unsupported provider)")

    # Add account metadata to synced events
    for event in all_synced_events:
        if 'account_email' not in event:
            account_info = next(
                (acc for acc in accounts_status if acc['connection_id'] == event.get('ext_connection_id')),
                {}
            )
            event['account_email'] = account_info.get('email')
            event['account_provider'] = account_info.get('provider')
            event['account_avatar'] = account_info.get('avatar')

    # Sort all events by start_time
    all_synced_events.sort(key=lambda e: e.get('start_time', ''))

    logger.info(f"✅ Synced {len(all_synced_events)} events from {synced_accounts} accounts")

    return {
        "events": all_synced_events,
        "count": len(all_synced_events),
        "source": "api_synced",
        "synced_accounts": synced_accounts,
        "unified": is_unified,
        "account_ids": account_ids,
        "accounts_status": accounts_status
    }


def get_today_events(
    user_id: str,
    user_jwt: str,
    account_ids: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get today's calendar events for a user with unified multi-account support.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        account_ids: Optional list of ext_connection_ids to filter by specific accounts.
                     If None or empty, returns events from all accounts (unified view).

    Returns:
        Dict with events (including account metadata), count, date, unified flag,
        and accounts_status for all connected calendar accounts.
    """
    # Track whether caller explicitly requested account filtering
    requested_filtering = account_ids is not None and len(account_ids) > 0

    # Validate ownership of requested account_ids
    if requested_filtering:
        account_ids = _filter_owned_account_ids(user_id, account_ids, user_jwt)
        # All requested IDs were foreign — return empty, don't widen to unified view
        if not account_ids:
            now = datetime.now(timezone.utc)
            accounts_status = get_calendar_accounts_status(user_id, user_jwt)
            return {
                "events": [],
                "count": 0,
                "date": now.replace(hour=0, minute=0, second=0, microsecond=0).date().isoformat(),
                "unified": False,
                "account_ids": [],
                "accounts_status": accounts_status
            }

    # Determine if this is a unified view (all accounts) or filtered
    is_unified = not requested_filtering

    # Use authenticated Supabase client
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get today's date range in UTC
    now = datetime.now(timezone.utc)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Query with JOIN to get account metadata
    query = auth_supabase.table('calendar_events')\
        .select('*, ext_connections(provider_email, provider, metadata)')\
        .eq('user_id', user_id)\
        .gte('start_time', start_of_day.isoformat())\
        .lte('start_time', end_of_day.isoformat())\
        .order('start_time', desc=False)

    # Filter by accounts if specified (supports multiple accounts)
    if account_ids and len(account_ids) > 0:
        if len(account_ids) == 1:
            query = query.eq('ext_connection_id', account_ids[0])
        else:
            query = query.in_('ext_connection_id', account_ids)

    result = query.execute()

    # Map events with account metadata
    mapped_events = [_map_event_with_account_metadata(e) for e in (result.data or [])]

    # Get accounts status for frontend
    accounts_status = get_calendar_accounts_status(user_id, user_jwt)

    return {
        "events": mapped_events,
        "count": len(mapped_events),
        "date": start_of_day.date().isoformat(),
        "unified": is_unified,
        "account_ids": account_ids,
        "accounts_status": accounts_status
    }


def get_event_by_id(event_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a specific calendar event by ID
    """
    result = supabase.table('calendar_events')\
        .select('*')\
        .eq('id', event_id)\
        .single()\
        .execute()
    
    return result.data if result.data else None
