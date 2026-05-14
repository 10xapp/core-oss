"""
Google API service builder for cron jobs and workers.

Builds authenticated Gmail and Calendar services from stored credentials,
refreshing tokens as needed. Used by cron.py and workers.py.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Any

from api.services.syncs.google_error_utils import is_permanent_google_oauth_error
from lib.google_retry import refresh_credentials
from lib.token_encryption import (
    decrypt_ext_connection_tokens,
    encrypt_token_fields,
)

logger = logging.getLogger(__name__)


def get_google_services_for_connection(
    connection_id: str,
    service_supabase: Any,
) -> Tuple[Optional[Any], Optional[Any], Optional[str]]:
    """
    Build Google API services using service role credentials.

    Fetches stored OAuth tokens for a connection, refreshes if needed,
    and returns ready-to-use Gmail and Calendar service objects.

    Args:
        connection_id: The ext_connection ID to get services for
        service_supabase: Service role Supabase client

    Returns:
        Tuple of (gmail_service, calendar_service, user_id).
        All three are None when credentials are unavailable.
    """
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from api.config import settings

    try:
        # Get connection by ID using service role (bypasses RLS)
        connection_result = service_supabase.table('ext_connections')\
            .select('id, user_id, access_token, refresh_token, token_expires_at, metadata')\
            .eq('id', connection_id)\
            .eq('is_active', True)\
            .maybe_single()\
            .execute()

        if not connection_result or not connection_result.data:
            return None, None, None

        connection_data = decrypt_ext_connection_tokens(connection_result.data)
        user_id = connection_data['user_id']
        access_token = connection_data.get('access_token')
        refresh_token = connection_data.get('refresh_token')

        if not access_token:
            logger.warning(f"⚠️ No access token for user {user_id}")
            return None, None, None

        # Get client credentials from metadata or fall back to settings
        metadata = connection_data.get('metadata') or {}
        client_id = metadata.get('client_id') or settings.google_client_id
        client_secret = metadata.get('client_secret') or settings.google_client_secret

        if not client_id or not client_secret:
            logger.error("Missing Google OAuth client credentials (client_id or client_secret)")
            logger.error("Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET environment variables")
            return None, None, None

        # Check if token needs refresh (expired or expires within 5 minutes)
        token_expires_at = connection_data.get('token_expires_at')
        needs_refresh = False

        if token_expires_at:
            try:
                expires_at = datetime.fromisoformat(token_expires_at.replace('Z', '+00:00'))
                needs_refresh = expires_at < datetime.now(timezone.utc) + timedelta(minutes=5)
            except ValueError:
                # Can't parse expiry, assume token needs refresh
                needs_refresh = True
        else:
            # No expiry recorded, assume token might be stale
            needs_refresh = True

        # Only require refresh_token if we actually need to refresh
        if needs_refresh and not refresh_token:
            logger.warning(f"⚠️ Cannot refresh token for user {user_id} (missing refresh_token)")
            return None, None, None

        # Build credentials object
        credentials = Credentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret
        )

        # Refresh and persist token if needed
        if needs_refresh:
            try:
                refresh_credentials(credentials, Request(), context=connection_id[:8])

                # Use actual expiry from Google credentials (make timezone-aware if needed)
                if credentials.expiry:
                    if credentials.expiry.tzinfo is None:
                        new_expires_at = credentials.expiry.replace(tzinfo=timezone.utc)
                    else:
                        new_expires_at = credentials.expiry
                else:
                    # Fallback to 1 hour if expiry not provided
                    new_expires_at = datetime.now(timezone.utc) + timedelta(hours=1)

                update_data = {
                    'access_token': credentials.token,
                    'token_expires_at': new_expires_at.isoformat(),
                    'updated_at': datetime.now(timezone.utc).isoformat()
                }

                # Save new refresh_token if Google issued one
                if credentials.refresh_token and credentials.refresh_token != refresh_token:
                    update_data['refresh_token'] = credentials.refresh_token
                    logger.info(f"🔄 Google issued new refresh token for connection {connection_id[:8]}...")

                service_supabase.table('ext_connections')\
                    .update(encrypt_token_fields(update_data))\
                    .eq('id', connection_id)\
                    .execute()

                logger.info(f"🔄 Refreshed and saved token for connection {connection_id[:8]}...")

            except Exception as e:
                if is_permanent_google_oauth_error(e):
                    logger.warning(
                        f"🚫 Permanent OAuth failure for connection {connection_id[:8]}... "
                        f"(user {user_id[:8]}...): {e} — deactivating connection"
                    )
                    try:
                        # Guard against concurrent refresh race: re-read the row
                        # and skip deactivation if another process just refreshed
                        # successfully (token_expires_at well into the future).
                        fresh = service_supabase.table('ext_connections')\
                            .select('token_expires_at')\
                            .eq('id', connection_id)\
                            .maybe_single()\
                            .execute()
                        if fresh and fresh.data:
                            fresh_expiry = fresh.data.get('token_expires_at')
                            if fresh_expiry:
                                try:
                                    exp = datetime.fromisoformat(fresh_expiry.replace('Z', '+00:00'))
                                    if exp > datetime.now(timezone.utc) + timedelta(minutes=10):
                                        logger.info(
                                            f"⏭️ Skipping deactivation for {connection_id[:8]}... "
                                            f"— token was refreshed by another process (expires {fresh_expiry})"
                                        )
                                        return None, None, None
                                except (ValueError, TypeError):
                                    pass  # Can't parse — proceed with deactivation

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
                    return None, None, None

                logger.warning(f"⚠️ Token refresh failed for {connection_id[:8]}...: {e}")
                # Continue anyway for transient errors - Google client library may still auto-refresh

        gmail_service = build('gmail', 'v1', credentials=credentials)
        calendar_service = build('calendar', 'v3', credentials=credentials)

        return gmail_service, calendar_service, user_id

    except Exception as e:
        logger.error(f"❌ Error getting Google services for connection {connection_id}: {str(e)}")
        return None, None, None
