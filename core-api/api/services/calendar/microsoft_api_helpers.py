"""
Microsoft Calendar API helper functions
Shared utilities for interacting with Microsoft Graph Calendar API

Provides similar interface to google_api_helpers.py for consistency.
"""
from typing import Optional, Dict, Any, Tuple
from lib.supabase_client import get_authenticated_supabase_client, get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens
import logging
import httpx


logger = logging.getLogger(__name__)

# Microsoft Graph API base URL
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"


def get_microsoft_calendar_service_for_account(
    user_id: str,
    user_jwt: str,
    account_id: str
) -> Tuple[Optional[str], Optional[str], Optional[Dict]]:
    """
    Get Microsoft access token and connection info for a specific account.

    Unlike Google, Microsoft doesn't use a "service" object - we use httpx directly.
    Returns the access token and connection details.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        account_id: The ext_connection_id to use

    Returns:
        Tuple of (access_token, connection_id, connection_data) or (None, None, None)
    """
    try:
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        logger.info(f"🔍 Getting Microsoft Calendar access for account {account_id[:8]}...")

        # Get specific connection by ID
        connection_result = auth_supabase.table('ext_connections')\
            .select('id, access_token, refresh_token, token_expires_at, metadata, provider_email')\
            .eq('id', account_id)\
            .eq('user_id', user_id)\
            .eq('provider', 'microsoft')\
            .eq('is_active', True)\
            .single()\
            .execute()

        if not connection_result.data:
            logger.warning(f"❌ No active Microsoft connection found for account {account_id}")
            return None, None, None

        connection_data = decrypt_ext_connection_tokens(connection_result.data)
        connection_data['user_id'] = user_id
        connection_id = connection_data['id']

        # Get valid access token (refresh if needed)
        access_token = _refresh_microsoft_token_if_needed(connection_data)

        if not access_token:
            logger.error(f"❌ Unable to get valid access token for account {account_id}")
            return None, None, None

        logger.info(f"✅ Got Microsoft Calendar access for account {account_id[:8]}...")
        return access_token, connection_id, connection_data

    except Exception as e:
        logger.error(f"❌ Error getting Microsoft Calendar access: {str(e)}")
        return None, None, None


def get_microsoft_calendar_service(
    user_id: str,
    user_jwt: str,
    account_id: str = None
) -> Tuple[Optional[str], Optional[str], Optional[Dict]]:
    """
    Get Microsoft access token for calendar operations.

    For multi-account support: If account_id is provided, uses that specific account.
    Otherwise, uses the primary Microsoft account.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        account_id: Optional specific account to use (ext_connection_id)

    Returns:
        Tuple of (access_token, connection_id, connection_data) or (None, None, None)
    """
    # If specific account requested, use that
    if account_id:
        return get_microsoft_calendar_service_for_account(user_id, user_jwt, account_id)

    try:
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        logger.info(f"🔍 Looking for Microsoft connection for user {user_id}")

        # Get user's Microsoft OAuth connection (primary first, fallback to most recent)
        connection_result = auth_supabase.table('ext_connections')\
            .select('id, access_token, refresh_token, token_expires_at, metadata, provider_email')\
            .eq('user_id', user_id)\
            .eq('provider', 'microsoft')\
            .eq('is_active', True)\
            .order('is_primary', desc=True)\
            .order('created_at', desc=True)\
            .limit(1)\
            .execute()

        if not connection_result.data or len(connection_result.data) == 0:
            logger.warning(f"❌ No active Microsoft connection found for user {user_id}")
            return None, None, None

        connection_data = decrypt_ext_connection_tokens(connection_result.data[0])
        connection_data['user_id'] = user_id
        connection_id = connection_data['id']

        logger.info(f"✅ Found Microsoft connection (ID: {connection_id})")

        # Get valid access token (refresh if needed)
        access_token = _refresh_microsoft_token_if_needed(connection_data)

        if not access_token:
            logger.error(f"❌ Unable to get valid access token for user {user_id}")
            return None, None, None

        return access_token, connection_id, connection_data

    except Exception as e:
        logger.error(f"❌ Error getting Microsoft Calendar access: {str(e)}")
        return None, None, None


def _refresh_microsoft_token_if_needed(connection_data: Dict[str, Any]) -> Optional[str]:
    """
    Check if Microsoft access token is expired and refresh if needed.
    Returns the valid access token.
    """
    from api.services.microsoft.microsoft_oauth_provider import get_valid_microsoft_credentials

    try:
        service_supabase = get_service_role_client()
        return get_valid_microsoft_credentials(connection_data, service_supabase)
    except Exception as e:
        logger.error(f"❌ Failed to refresh Microsoft token: {e}")
        return None


def get_user_timezone_microsoft(access_token: str) -> str:
    """
    Get user's timezone from their Outlook mailbox settings.

    Args:
        access_token: Valid Microsoft access token

    Returns:
        IANA timezone string (e.g., 'Asia/Kolkata', 'America/New_York')
        Falls back to 'UTC' if unable to fetch
    """
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(
                f"{GRAPH_API_URL}/me/mailboxSettings",
                headers={"Authorization": f"Bearer {access_token}"}
            )

        if response.status_code == 200:
            data = response.json()
            user_timezone = data.get('timeZone', 'UTC')
            logger.info(f"📍 User timezone from Microsoft: {user_timezone}")
            return user_timezone
        else:
            logger.warning(f"⚠️ Failed to get Microsoft timezone: {response.status_code}")
            return 'UTC'

    except Exception as e:
        logger.warning(f"⚠️ Failed to get Microsoft timezone, falling back to UTC: {e}")
        return 'UTC'


def convert_to_microsoft_event_format(event_data: Dict[str, Any], user_timezone: str = 'UTC') -> Dict[str, Any]:
    """
    Convert our event format to Microsoft Graph event format.

    Microsoft Graph Event format:
    - subject: Event title
    - body: { contentType: "HTML" or "Text", content: "..." }
    - start: { dateTime: "2024-01-01T10:00:00", timeZone: "Asia/Kolkata" }
    - end: { dateTime: "2024-01-01T11:00:00", timeZone: "Asia/Kolkata" }
    - location: { displayName: "..." }
    - isAllDay: boolean
    """
    subject = event_data.get('summary') or event_data.get('title') or 'Untitled Event'

    microsoft_event = {
        'subject': subject,
    }

    # Description/body
    description = event_data.get('description')
    if description:
        microsoft_event['body'] = {
            'contentType': 'Text',
            'content': description
        }

    # Location
    location = event_data.get('location')
    if location:
        microsoft_event['location'] = {
            'displayName': location
        }

    is_all_day = event_data.get('is_all_day') or event_data.get('all_day', False)
    start_time = event_data.get('start_time')
    end_time = event_data.get('end_time')

    microsoft_event['isAllDay'] = is_all_day

    if is_all_day:
        # For all-day events, Microsoft expects date only without time
        if start_time:
            # Strip time portion if present
            start_date = start_time[:10] if 'T' in start_time else start_time
            microsoft_event['start'] = {
                'dateTime': f"{start_date}T00:00:00",
                'timeZone': user_timezone
            }
        if end_time:
            end_date = end_time[:10] if 'T' in end_time else end_time
            microsoft_event['end'] = {
                'dateTime': f"{end_date}T00:00:00",
                'timeZone': user_timezone
            }
    else:
        # Regular timed events
        if start_time:
            # Remove timezone offset if present, use timeZone field instead
            clean_start = _strip_timezone_offset(start_time)
            microsoft_event['start'] = {
                'dateTime': clean_start,
                'timeZone': user_timezone
            }
        if end_time:
            clean_end = _strip_timezone_offset(end_time)
            microsoft_event['end'] = {
                'dateTime': clean_end,
                'timeZone': user_timezone
            }

    # Validate required fields
    if 'start' not in microsoft_event or 'end' not in microsoft_event:
        raise ValueError("Missing required 'start' or 'end' for Microsoft event")

    logger.info(f"📅 Microsoft event: {microsoft_event}")
    return microsoft_event


def _strip_timezone_offset(datetime_str: str) -> str:
    """
    Remove timezone offset from datetime string.
    Microsoft Graph wants dateTime without offset when timeZone is specified.

    "2024-01-01T10:00:00+05:30" -> "2024-01-01T10:00:00"
    "2024-01-01T10:00:00Z" -> "2024-01-01T10:00:00"
    """
    if not datetime_str:
        return datetime_str

    # Remove Z suffix
    if datetime_str.endswith('Z'):
        return datetime_str[:-1]

    # Remove +HH:MM or -HH:MM offset
    if '+' in datetime_str:
        return datetime_str.split('+')[0]
    if datetime_str.count('-') > 2:  # Has negative offset like -05:00
        parts = datetime_str.rsplit('-', 1)
        if ':' in parts[-1]:  # It's a timezone offset
            return parts[0]

    return datetime_str


# ============== Microsoft Graph Calendar API Operations ==============

def create_microsoft_event(
    access_token: str,
    event_data: Dict[str, Any],
    user_timezone: str = 'UTC'
) -> Dict[str, Any]:
    """
    Create an event in Microsoft Outlook Calendar.

    Args:
        access_token: Valid Microsoft access token
        event_data: Event data in our format
        user_timezone: User's timezone

    Returns:
        Dict with success status and created event data
    """
    try:
        microsoft_event = convert_to_microsoft_event_format(event_data, user_timezone)

        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                f"{GRAPH_API_URL}/me/events",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                },
                json=microsoft_event
            )

        if response.status_code == 201:
            created_event = response.json()
            event_id = created_event.get('id')
            logger.info(f"✅ Created event in Microsoft Calendar: {event_id}")
            return {
                "success": True,
                "event_id": event_id,
                "event": created_event
            }
        else:
            error = response.json().get('error', {})
            error_msg = error.get('message', response.text)
            logger.error(f"❌ Failed to create Microsoft event: {error_msg}")
            return {
                "success": False,
                "error": error_msg
            }

    except Exception as e:
        logger.error(f"❌ Error creating Microsoft event: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def update_microsoft_event(
    access_token: str,
    event_id: str,
    event_data: Dict[str, Any],
    user_timezone: str = 'UTC'
) -> Dict[str, Any]:
    """
    Update an event in Microsoft Outlook Calendar.

    Args:
        access_token: Valid Microsoft access token
        event_id: Microsoft event ID
        event_data: Updated event data in our format
        user_timezone: User's timezone

    Returns:
        Dict with success status and updated event data
    """
    try:
        microsoft_event = convert_to_microsoft_event_format(event_data, user_timezone)

        with httpx.Client(timeout=30.0) as client:
            response = client.patch(
                f"{GRAPH_API_URL}/me/events/{event_id}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                },
                json=microsoft_event
            )

        if response.status_code == 200:
            updated_event = response.json()
            logger.info(f"✅ Updated event in Microsoft Calendar: {event_id}")
            return {
                "success": True,
                "event_id": event_id,
                "event": updated_event
            }
        else:
            error = response.json().get('error', {})
            error_msg = error.get('message', response.text)
            logger.error(f"❌ Failed to update Microsoft event: {error_msg}")
            return {
                "success": False,
                "error": error_msg
            }

    except Exception as e:
        logger.error(f"❌ Error updating Microsoft event: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def delete_microsoft_event(
    access_token: str,
    event_id: str
) -> Dict[str, Any]:
    """
    Delete an event from Microsoft Outlook Calendar.

    Args:
        access_token: Valid Microsoft access token
        event_id: Microsoft event ID

    Returns:
        Dict with success status
    """
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.delete(
                f"{GRAPH_API_URL}/me/events/{event_id}",
                headers={"Authorization": f"Bearer {access_token}"}
            )

        # 204 No Content = success, 404 = already deleted
        if response.status_code in [204, 404]:
            logger.info(f"✅ Deleted event from Microsoft Calendar: {event_id}")
            return {"success": True}
        else:
            error = response.json().get('error', {}) if response.text else {}
            error_msg = error.get('message', f"HTTP {response.status_code}")
            logger.error(f"❌ Failed to delete Microsoft event: {error_msg}")
            return {
                "success": False,
                "error": error_msg
            }

    except Exception as e:
        logger.error(f"❌ Error deleting Microsoft event: {e}")
        return {
            "success": False,
            "error": str(e)
        }
