"""
Centralized Google OAuth Token Management

This module provides a single source of truth for Google OAuth token handling,
including automatic refresh, secure storage updates, and credential building.

Used by:
- Webhook handlers (Gmail, Calendar push notifications)
- Cron jobs (incremental sync, watch renewal)
- User-initiated API calls (email, calendar operations)
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build

from lib.supabase_client import get_service_role_client
from lib.token_encryption import (
    decrypt_ext_connection_tokens,
    encrypt_token_fields,
)
from api.config import settings

logger = logging.getLogger(__name__)

# Token refresh buffer - refresh if expiring within this time
TOKEN_REFRESH_BUFFER_MINUTES = 5

# Default token lifetime (Google access tokens typically last 1 hour)
DEFAULT_TOKEN_LIFETIME_SECONDS = 3600


class GoogleAuthError(Exception):
    """Base exception for Google authentication errors"""
    pass


class TokenRefreshError(GoogleAuthError):
    """Raised when token refresh fails"""
    pass


class NoConnectionError(GoogleAuthError):
    """Raised when no OAuth connection is found"""
    pass


class InvalidTokenError(GoogleAuthError):
    """Raised when tokens are invalid or missing"""
    pass


def get_valid_credentials(
    connection_data: Dict[str, Any],
    supabase_client=None
) -> Credentials:
    """
    Get valid Google credentials, refreshing if needed.

    This is the main function that should be used by webhooks and cron jobs
    to obtain valid credentials before making Google API calls.

    Args:
        connection_data: Dict containing:
            - id: Connection ID
            - user_id: User ID
            - access_token: Current access token
            - refresh_token: Refresh token for obtaining new access tokens
            - token_expires_at: ISO timestamp of token expiration (optional)
            - metadata: Optional dict with client_id/client_secret overrides
        supabase_client: Optional Supabase client for DB updates.
                        If None, uses service role client.

    Returns:
        Valid Google Credentials object

    Raises:
        InvalidTokenError: If tokens are missing or invalid
        TokenRefreshError: If token refresh fails
    """
    access_token = connection_data.get('access_token')
    refresh_token = connection_data.get('refresh_token')

    if not access_token:
        raise InvalidTokenError("No access token available")

    if not refresh_token:
        raise InvalidTokenError("No refresh token available - user must re-authenticate")

    # Check if token needs refresh
    if _is_token_expired(connection_data.get('token_expires_at')):
        logger.info("🔄 Token expired or expiring soon, refreshing...")
        return _refresh_and_save_token(connection_data, supabase_client)

    # Token is still valid, return full credentials with refresh capability
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


def get_credentials_for_connection(
    connection_id: str,
    supabase_client=None
) -> Tuple[Credentials, Dict[str, Any]]:
    """
    Get valid credentials for a specific connection ID.

    Args:
        connection_id: The ext_connections.id
        supabase_client: Optional Supabase client. Uses service role if None.

    Returns:
        Tuple of (Credentials, connection_data dict)

    Raises:
        NoConnectionError: If connection not found
        InvalidTokenError: If tokens are missing
        TokenRefreshError: If refresh fails
    """
    if supabase_client is None:
        supabase_client = get_service_role_client()

    result = supabase_client.table('ext_connections')\
        .select('id, user_id, access_token, refresh_token, token_expires_at, metadata')\
        .eq('id', connection_id)\
        .eq('is_active', True)\
        .single()\
        .execute()

    if not result.data:
        raise NoConnectionError(f"No active connection found with ID {connection_id}")

    connection_data = decrypt_ext_connection_tokens(result.data)
    credentials = get_valid_credentials(connection_data, supabase_client)

    return credentials, connection_data


def get_credentials_for_user(
    user_id: str,
    provider: str = 'google',
    supabase_client=None
) -> Tuple[Credentials, Dict[str, Any]]:
    """
    Get valid credentials for a user's OAuth connection.

    For multi-account support: Uses primary account first, falls back to most recent.

    Args:
        user_id: The user's ID
        provider: OAuth provider (default: 'google')
        supabase_client: Optional Supabase client. Uses service role if None.

    Returns:
        Tuple of (Credentials, connection_data dict including 'id')

    Raises:
        NoConnectionError: If no active connection found
        InvalidTokenError: If tokens are missing
        TokenRefreshError: If refresh fails
    """
    if supabase_client is None:
        supabase_client = get_service_role_client()

    # Get primary account first, fallback to most recent (multi-account support)
    result = supabase_client.table('ext_connections')\
        .select('id, user_id, access_token, refresh_token, token_expires_at, metadata, provider_email')\
        .eq('user_id', user_id)\
        .eq('provider', provider)\
        .eq('is_active', True)\
        .order('is_primary', desc=True)\
        .order('created_at', desc=True)\
        .limit(1)\
        .execute()

    if not result.data or len(result.data) == 0:
        raise NoConnectionError(f"No active {provider} connection found for user {user_id}")

    connection_data = decrypt_ext_connection_tokens(result.data[0])  # Get first result from list
    credentials = get_valid_credentials(connection_data, supabase_client)

    return credentials, connection_data


def get_gmail_service_for_webhook(
    connection_data: Dict[str, Any],
    supabase_client=None
) -> Tuple[Any, str]:
    """
    Build Gmail service for webhook processing.

    Convenience function that handles token refresh and returns
    both the service and connection_id.

    Args:
        connection_data: Connection data from push_subscriptions join
        supabase_client: Optional Supabase client

    Returns:
        Tuple of (Gmail service, connection_id)
    """
    # Extract connection info - webhook queries join ext_connections
    if 'ext_connections' in connection_data:
        # Nested from join query — decrypt nested tokens
        ext_conn = decrypt_ext_connection_tokens(connection_data['ext_connections'])
        conn_for_refresh = {
            'id': connection_data.get('ext_connection_id'),
            'user_id': ext_conn.get('user_id'),
            'access_token': ext_conn.get('access_token'),
            'refresh_token': ext_conn.get('refresh_token'),
            'token_expires_at': ext_conn.get('token_expires_at'),
            'metadata': ext_conn.get('metadata') or {}
        }
    else:
        conn_for_refresh = connection_data

    credentials = get_valid_credentials(conn_for_refresh, supabase_client)
    service = build('gmail', 'v1', credentials=credentials)

    return service, conn_for_refresh.get('id')


def get_calendar_service_for_webhook(
    connection_data: Dict[str, Any],
    supabase_client=None
) -> Tuple[Any, str]:
    """
    Build Calendar service for webhook processing.

    Args:
        connection_data: Connection data from push_subscriptions join
        supabase_client: Optional Supabase client

    Returns:
        Tuple of (Calendar service, connection_id)
    """
    # Extract connection info - webhook queries join ext_connections
    if 'ext_connections' in connection_data:
        # Nested from join query — decrypt nested tokens
        ext_conn = decrypt_ext_connection_tokens(connection_data['ext_connections'])
        conn_for_refresh = {
            'id': connection_data.get('ext_connection_id'),
            'user_id': ext_conn.get('user_id'),
            'access_token': ext_conn.get('access_token'),
            'refresh_token': ext_conn.get('refresh_token'),
            'token_expires_at': ext_conn.get('token_expires_at'),
            'metadata': ext_conn.get('metadata') or {}
        }
    else:
        conn_for_refresh = connection_data

    credentials = get_valid_credentials(conn_for_refresh, supabase_client)
    service = build('calendar', 'v3', credentials=credentials)

    return service, conn_for_refresh.get('id')


def _is_token_expired(token_expires_at: Optional[str]) -> bool:
    """
    Check if token is expired or will expire within the buffer period.

    Args:
        token_expires_at: ISO timestamp string or None

    Returns:
        True if token is expired or expiring soon, False otherwise
    """
    if not token_expires_at:
        # No expiry time stored - assume it might be expired
        # This is conservative but safer than assuming it's valid
        logger.warning("No token_expires_at stored, assuming token may be expired")
        return True

    try:
        # Parse the expiry time
        if token_expires_at.endswith('Z'):
            expires_at = datetime.fromisoformat(token_expires_at.replace('Z', '+00:00'))
        elif '+' in token_expires_at or token_expires_at.endswith('+00:00'):
            expires_at = datetime.fromisoformat(token_expires_at)
        else:
            # Assume UTC if no timezone
            expires_at = datetime.fromisoformat(token_expires_at).replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        buffer = timedelta(minutes=TOKEN_REFRESH_BUFFER_MINUTES)

        return expires_at <= (now + buffer)

    except (ValueError, TypeError) as e:
        logger.warning(f"Could not parse token_expires_at '{token_expires_at}': {e}")
        return True


def _refresh_and_save_token(
    connection_data: Dict[str, Any],
    supabase_client=None
) -> Credentials:
    """
    Refresh the access token and save it to the database.

    Uses upsert-style update to avoid race conditions when multiple
    processes try to refresh simultaneously.

    Args:
        connection_data: Dict with connection info
        supabase_client: Supabase client for DB updates

    Returns:
        New Credentials object with fresh token

    Raises:
        TokenRefreshError: If refresh fails
    """
    if supabase_client is None:
        supabase_client = get_service_role_client()

    connection_id = connection_data.get('id')
    user_id = connection_data.get('user_id')
    refresh_token = connection_data.get('refresh_token')

    if not refresh_token:
        raise TokenRefreshError("No refresh token available")

    # Get client credentials
    metadata = connection_data.get('metadata', {}) or {}
    client_id = metadata.get('client_id') or settings.google_client_id
    client_secret = metadata.get('client_secret') or settings.google_client_secret

    if not client_id or not client_secret:
        raise TokenRefreshError(
            "Missing Google OAuth credentials. "
            "Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET environment variables."
        )

    try:
        # Build credentials object for refresh
        credentials = Credentials(
            token=connection_data.get('access_token'),
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret
        )

        # Perform the refresh
        credentials.refresh(Request())

        # Calculate new expiry time
        new_expires_at = datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_TOKEN_LIFETIME_SECONDS)

        # Save to database - use connection_id if available, otherwise user_id+provider
        update_data = {
            'access_token': credentials.token,
            'token_expires_at': new_expires_at.isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }

        # Also update refresh_token if Google issued a new one
        if credentials.refresh_token and credentials.refresh_token != refresh_token:
            update_data['refresh_token'] = credentials.refresh_token
            logger.info("🔄 Google issued new refresh token, saving it")

        if connection_id:
            supabase_client.table('ext_connections')\
                .update(encrypt_token_fields(update_data))\
                .eq('id', connection_id)\
                .execute()
            logger.info("✅ Successfully refreshed and saved Google access token")
        else:
            # Don't update by user_id+provider - could corrupt other connections
            # if user has multiple Google accounts linked
            logger.error(f"❌ Cannot save refreshed token: no connection_id provided for user {user_id}")
            raise TokenRefreshError("Cannot save refreshed token without connection_id")

        return credentials

    except RefreshError as e:
        logger.error(f"❌ Google token refresh failed: {str(e)}")

        # Mark connection as needing re-auth if refresh token is invalid
        if 'invalid_grant' in str(e).lower():
            logger.error("🔴 Refresh token is invalid - user must re-authenticate")
            if connection_id:
                supabase_client.table('ext_connections')\
                    .update({'is_active': False})\
                    .eq('id', connection_id)\
                    .execute()

        raise TokenRefreshError(f"Failed to refresh token: {str(e)}") from e

    except Exception as e:
        logger.error(f"❌ Unexpected error during token refresh: {str(e)}")
        raise TokenRefreshError(f"Token refresh failed: {str(e)}") from e


def get_current_gmail_history_id(gmail_service) -> Optional[str]:
    """
    Get the current historyId from Gmail.

    Used to recover from expired/invalid history IDs in webhook handlers.

    Args:
        gmail_service: Authenticated Gmail API service

    Returns:
        Current historyId string, or None if failed
    """
    try:
        profile = gmail_service.users().getProfile(userId='me').execute()
        history_id = profile.get('historyId')
        logger.info(f"📧 Retrieved current Gmail historyId: {history_id}")
        return history_id
    except Exception as e:
        logger.error(f"❌ Failed to get Gmail profile/historyId: {str(e)}")
        return None
