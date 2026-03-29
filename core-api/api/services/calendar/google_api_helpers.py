"""
Google Calendar API helper functions
Shared utilities for interacting with Google Calendar API
"""
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from lib.supabase_client import get_authenticated_supabase_client, get_service_role_client
from lib.token_encryption import (
    decrypt_ext_connection_tokens,
    encrypt_token_fields,
)
import logging
import uuid
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


def get_google_calendar_service_for_account(user_id: str, user_jwt: str, account_id: str):
    """
    Get an authenticated Google Calendar API service instance for a specific account.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        account_id: The ext_connection_id to use

    Returns:
        Tuple of (service, connection_id) or (None, None) if no connection
    """
    try:
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        logger.info(f"🔍 Getting Calendar service for account {account_id[:8]}...")

        # Get specific connection by ID
        connection_result = auth_supabase.table('ext_connections')\
            .select('id, access_token, refresh_token, token_expires_at, metadata, provider_email')\
            .eq('id', account_id)\
            .eq('user_id', user_id)\
            .eq('provider', 'google')\
            .eq('is_active', True)\
            .single()\
            .execute()

        if not connection_result.data:
            logger.warning(f"❌ No active Google connection found for account {account_id}")
            return None, None

        connection_data = decrypt_ext_connection_tokens(connection_result.data)
        connection_data['user_id'] = user_id
        connection_id = connection_data['id']

        # Get valid credentials (refresh if needed)
        credentials = _get_google_credentials(connection_data)

        if not credentials:
            logger.error(f"❌ Unable to get valid credentials for account {account_id}")
            return None, None

        # Build Google Calendar API client
        service = build('calendar', 'v3', credentials=credentials)

        logger.info(f"✅ Built Calendar service for account {account_id[:8]}...")
        return service, connection_id

    except Exception as e:
        logger.error(f"❌ Error getting Calendar service for account: {str(e)}")
        return None, None


def get_google_calendar_service(user_id: str, user_jwt: str, account_id: str = None):
    """
    Get an authenticated Google Calendar API service instance

    For multi-account support: If account_id is provided, uses that specific account.
    Otherwise, uses the primary account.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        account_id: Optional specific account to use (ext_connection_id)

    Returns:
        Tuple of (service, connection_id) or (None, None) if no connection
    """
    # If specific account requested, use that
    if account_id:
        return get_google_calendar_service_for_account(user_id, user_jwt, account_id)

    try:
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        logger.info(f"🔍 Looking for Google connection for user {user_id}")

        # Get user's Google OAuth connection (primary first, fallback to most recent)
        # This supports multi-account: uses primary account for calendar
        connection_result = auth_supabase.table('ext_connections')\
            .select('id, access_token, refresh_token, token_expires_at, metadata, provider_email')\
            .eq('user_id', user_id)\
            .eq('provider', 'google')\
            .eq('is_active', True)\
            .order('is_primary', desc=True)\
            .order('created_at', desc=True)\
            .limit(1)\
            .execute()

        if not connection_result.data or len(connection_result.data) == 0:
            logger.warning(f"❌ No active Google connection found for user {user_id}")
            logger.info("💡 User needs to connect their Google Calendar via OAuth")
            return None, None

        connection_data = decrypt_ext_connection_tokens(connection_result.data[0])  # Get first result from list
        connection_data['user_id'] = user_id
        connection_id = connection_data['id']

        logger.info(f"✅ Found Google connection (ID: {connection_id})")

        # Get valid credentials (refresh if needed)
        credentials = _get_google_credentials(connection_data)

        if not credentials:
            logger.error(f"❌ Unable to get valid credentials for user {user_id}")
            logger.error("💡 Token may be expired or invalid. User should re-authenticate.")
            return None, None

        logger.info("✅ Got valid credentials")

        # Build Google Calendar API client
        service = build('calendar', 'v3', credentials=credentials)

        logger.info("✅ Built Google Calendar API service")

        return service, connection_id

    except Exception as e:
        logger.error(f"❌ Error getting Google Calendar service: {str(e)}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return None, None


def _get_google_credentials(connection_data: Dict[str, Any]) -> Optional[Credentials]:
    """
    Build a full Google Credentials object with refresh capability.
    Refreshes the token first if expired, then returns credentials that
    the Google API client can auto-refresh if needed during a request.
    """
    from api.config import settings

    # Refresh token proactively if expired
    access_token = _refresh_google_token_if_needed(connection_data)
    if not access_token:
        return None

    refresh_token = connection_data.get('refresh_token')
    metadata = connection_data.get('metadata') or {}
    client_id = metadata.get('client_id') or settings.google_client_id
    client_secret = metadata.get('client_secret') or settings.google_client_secret

    return Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=client_id,
        client_secret=client_secret,
    )


def _refresh_google_token_if_needed(connection_data: Dict[str, Any]) -> Optional[str]:
    """
    Check if access token is expired and refresh if needed
    Returns the valid access token
    """
    token_expires_at = connection_data.get('token_expires_at')

    # If no expiry time, assume token is still valid
    if not token_expires_at:
        return connection_data.get('access_token')

    # Check if token is expired (with 5 minute buffer)
    # Handle various timestamp formats from Supabase (may have variable decimal places)
    try:
        expires_at = datetime.fromisoformat(token_expires_at.replace('Z', '+00:00'))
    except ValueError:
        # Fallback: parse with dateutil for non-standard formats
        from dateutil import parser
        expires_at = parser.parse(token_expires_at)
    now = datetime.now(timezone.utc)

    if expires_at > now + timedelta(minutes=5):
        # Token is still valid
        return connection_data.get('access_token')

    # Token appears expired based on cached data, but re-check DB
    # in case another concurrent request already refreshed it
    connection_id = connection_data.get('id')
    if connection_id:
        try:
            service_supabase = get_service_role_client()
            fresh_result = service_supabase.table('ext_connections')\
                .select('access_token, token_expires_at')\
                .eq('id', connection_id)\
                .execute()

            if fresh_result.data and len(fresh_result.data) > 0:
                fresh_data = decrypt_ext_connection_tokens(fresh_result.data[0])
                fresh_expires_at_str = fresh_data.get('token_expires_at')
                if fresh_expires_at_str:
                    try:
                        fresh_expires_at = datetime.fromisoformat(fresh_expires_at_str.replace('Z', '+00:00'))
                    except ValueError:
                        from dateutil import parser
                        fresh_expires_at = parser.parse(fresh_expires_at_str)

                    if fresh_expires_at > now + timedelta(minutes=5):
                        # Another request already refreshed the token!
                        logger.info(f"Token already refreshed by another request for connection {connection_id[:8]}...")
                        return fresh_data.get('access_token')
        except Exception as e:
            logger.warning(f"Pre-refresh DB check failed, proceeding with refresh: {e}")

    # Token is truly expired, need to refresh
    refresh_token = connection_data.get('refresh_token')
    if not refresh_token:
        logger.error("No refresh token available")
        return None

    try:
        # Use Google's refresh flow
        from google.auth.transport.requests import Request
        from api.config import settings

        # Get client credentials from metadata or fall back to settings
        metadata = connection_data.get('metadata') or {}
        client_id = metadata.get('client_id') or settings.google_client_id
        client_secret = metadata.get('client_secret') or settings.google_client_secret

        if not client_id or not client_secret:
            logger.error("Missing Google OAuth client credentials (client_id or client_secret)")
            logger.error("Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET environment variables")
            return None

        credentials = Credentials(
            token=connection_data.get('access_token'),
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret
        )

        credentials.refresh(Request())

        # Update tokens in database - use connection_id to update only THIS account
        # (not all Google accounts for the user)
        connection_id = connection_data.get('id')
        if not connection_id:
            logger.error("Cannot save refreshed token: no connection_id in connection_data")
            return None

        new_expires_at = datetime.now(timezone.utc) + timedelta(seconds=3600)
        update_data = {
            'access_token': credentials.token,
            'token_expires_at': new_expires_at.isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }

        # Save new refresh_token if Google issued one
        if credentials.refresh_token and credentials.refresh_token != connection_data.get('refresh_token'):
            update_data['refresh_token'] = credentials.refresh_token
            connection_data['refresh_token'] = credentials.refresh_token
            logger.info("Google issued new refresh token, saving it")

        service_supabase = get_service_role_client()
        service_supabase.table('ext_connections')\
            .update(encrypt_token_fields(update_data))\
            .eq('id', connection_id)\
            .execute()

        logger.info(f"Successfully refreshed Google access token for connection {connection_id}")
        return credentials.token

    except Exception as e:
        logger.error(f"Failed to refresh token: {str(e)}")
        return None


def get_user_timezone(service) -> str:
    """
    Get user's timezone from their Google Calendar settings.

    Args:
        service: Authenticated Google Calendar API service

    Returns:
        IANA timezone string (e.g., 'Asia/Kolkata', 'America/New_York')
        Falls back to 'UTC' if unable to fetch
    """
    try:
        calendar = service.calendars().get(calendarId='primary').execute()
        user_timezone = calendar.get('timeZone', 'UTC')
        logger.info(f"📍 User timezone from Google Calendar: {user_timezone}")
        return user_timezone
    except Exception as e:
        logger.warning(f"⚠️ Failed to get user timezone, falling back to UTC: {e}")
        return 'UTC'


def convert_to_google_event_format(event_data: Dict[str, Any], user_timezone: str = 'UTC', is_update: bool = False) -> Dict[str, Any]:
    """
    Convert our event format to Google Calendar event format.

    Per Google Calendar API docs:
    - dateTime: RFC3339 format. Offset required UNLESS timeZone is specified.
    - timeZone: IANA timezone name (e.g., 'Asia/Kolkata')

    We use: dateTime (no offset) + timeZone field
    """
    summary = event_data.get('summary') or event_data.get('title') or 'Untitled Event'

    google_event = {
        'summary': summary,
    }

    if event_data.get('description'):
        google_event['description'] = event_data['description']

    if event_data.get('location'):
        google_event['location'] = event_data['location']

    is_all_day = event_data.get('is_all_day') or event_data.get('all_day', False)
    start_time = event_data.get('start_time')
    end_time = event_data.get('end_time')

    if is_all_day:
        if start_time:
            google_event['start'] = {'date': start_time[:10]}
        if end_time:
            google_event['end'] = {'date': end_time[:10]}
    else:
        # Per Google API: when timeZone is specified, offset in dateTime is not required
        if start_time:
            google_event['start'] = {
                'dateTime': start_time,
                'timeZone': user_timezone
            }
        if end_time:
            google_event['end'] = {
                'dateTime': end_time,
                'timeZone': user_timezone
            }

    if event_data.get('status'):
        google_event['status'] = event_data['status']

    # Attendees: allow empty list to clear attendees on update
    if 'attendees' in event_data and event_data.get('attendees') is not None:
        attendees = event_data.get('attendees')
        # Normalize to list
        if not isinstance(attendees, list):
            attendees = [attendees]
        google_event['attendees'] = [
            {'email': str(email)} for email in attendees if email
        ]

    # Recurrence rules (Google expects a list of strings like RRULE:, EXDATE:, RDATE:)
    # Allow empty list to clear recurrence on update
    if 'recurrence' in event_data and event_data.get('recurrence') is not None:
        rec = event_data.get('recurrence')
        if isinstance(rec, list):
            google_event['recurrence'] = [str(r) for r in rec]
        else:
            google_event['recurrence'] = [str(rec)]

    # Google Meet video conferencing (only for creates, not updates)
    if not is_update and event_data.get('add_google_meet'):
        google_event['conferenceData'] = {
            'createRequest': {
                'requestId': str(uuid.uuid4()),
                'conferenceSolutionKey': {'type': 'hangoutsMeet'}
            }
        }

    # Validate required time fields (only for creates, not partial updates)
    if not is_update and ('start' not in google_event or 'end' not in google_event):
        raise ValueError("Missing required 'start' or 'end' for Google event")

    logger.info(f"📅 Google event: {google_event}")
    return google_event


def extract_meeting_link(event: Dict[str, Any]) -> Optional[str]:
    """
    Extract the video conference meeting link from a Google Calendar event.

    Checks conferenceData.entryPoints for a video entry point first,
    then falls back to hangoutLink if present.

    Args:
        event: Google Calendar API event object

    Returns:
        Meeting URL string, or None if no meeting link found
    """
    # Try conferenceData first (newer/richer format)
    conference_data = event.get('conferenceData')
    if conference_data:
        entry_points = conference_data.get('entryPoints', [])
        for entry_point in entry_points:
            if entry_point.get('entryPointType') == 'video':
                return entry_point.get('uri')

    # Fallback to hangoutLink (legacy Google Meet field)
    return event.get('hangoutLink')
