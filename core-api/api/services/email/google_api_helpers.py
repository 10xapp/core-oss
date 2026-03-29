"""
Gmail API helper functions
Shared utilities for interacting with Gmail API
"""
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from lib.supabase_client import get_authenticated_supabase_client, get_service_role_client
from lib.token_encryption import (
    decrypt_ext_connection_tokens,
    encrypt_token_fields,
)
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

logger = logging.getLogger(__name__)


def get_email_owner_connection_id(user_id: str, email_id: str, user_jwt: str) -> Optional[str]:
    """
    Look up which account (ext_connection_id) owns an email.

    This is critical for multi-account support: when operating on an email
    (archive, delete, label, etc.), we must use the correct account's credentials.

    Args:
        user_id: User's ID
        email_id: External email ID (Gmail message ID)
        user_jwt: User's Supabase JWT

    Returns:
        The ext_connection_id that owns this email, or None if not found
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    result = auth_supabase.table('emails')\
        .select('ext_connection_id')\
        .eq('user_id', user_id)\
        .eq('external_id', email_id)\
        .limit(1)\
        .execute()

    if result.data and len(result.data) > 0:
        return result.data[0].get('ext_connection_id')

    return None


def _looks_like_gmail_draft_id(identifier: Optional[str]) -> bool:
    """Best-effort check for Gmail draft IDs (usually prefixed with 'r')."""
    return bool(identifier) and identifier.startswith('r')


def _extract_gmail_draft_id_from_raw_item(raw_item: Any) -> Optional[str]:
    """
    Extract Gmail draft ID from a stored raw_item payload.

    We only trust raw_item.id when the object is a Gmail draft wrapper
    (contains top-level "message"), not a plain Gmail message payload.
    """
    if not isinstance(raw_item, dict):
        return None

    explicit = raw_item.get('gmail_draft_id')
    if explicit:
        return explicit

    if raw_item.get('message') and raw_item.get('id'):
        return raw_item.get('id')

    return None


def _get_google_connection_ids(user_id: str, user_jwt: str) -> List[str]:
    """Return active Google connection IDs for a user, ordered by account_order."""
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    result = auth_supabase.table('ext_connections')\
        .select('id')\
        .eq('user_id', user_id)\
        .eq('provider', 'google')\
        .eq('is_active', True)\
        .order('account_order', desc=False)\
        .execute()

    return [r.get('id') for r in (result.data or []) if r.get('id')]


def find_gmail_draft_id_by_message_id(service: Any, message_id: str, max_pages: int = 10) -> Optional[str]:
    """
    Resolve Gmail draft ID from Gmail message ID by scanning drafts.list().

    This is the legacy fallback path for rows that only stored external_id.
    """
    if not service or not message_id:
        return None

    page_token = None
    pages_scanned = 0

    while pages_scanned < max_pages:
        params = {'userId': 'me', 'maxResults': 100}
        if page_token:
            params['pageToken'] = page_token

        drafts_result = service.users().drafts().list(**params).execute()
        for draft in drafts_result.get('drafts', []) or []:
            draft_message = draft.get('message') or {}
            if draft_message.get('id') == message_id:
                return draft.get('id')

        page_token = drafts_result.get('nextPageToken')
        if not page_token:
            break

        pages_scanned += 1

    return None


def list_active_gmail_drafts_by_message_id(
    service: Any,
    max_pages: int = 20
) -> Dict[str, str]:
    """
    Build a map of active Gmail draft message IDs to Gmail draft IDs.

    Returns:
        Dict mapping {gmail_message_id: gmail_draft_id}
    """
    draft_map: Dict[str, str] = {}
    if not service:
        return draft_map

    page_token = None
    pages_scanned = 0

    while pages_scanned < max_pages:
        params = {"userId": "me", "maxResults": 100}
        if page_token:
            params["pageToken"] = page_token

        drafts_result = service.users().drafts().list(**params).execute()
        for draft in drafts_result.get("drafts", []) or []:
            draft_id = draft.get("id")
            message_id = (draft.get("message") or {}).get("id")
            if draft_id and message_id:
                draft_map[message_id] = draft_id

        page_token = drafts_result.get("nextPageToken")
        if not page_token:
            break

        pages_scanned += 1

    return draft_map


def _find_gmail_draft_by_id_across_accounts(
    user_id: str,
    user_jwt: str,
    gmail_draft_id: str
) -> Optional[Dict[str, Optional[str]]]:
    """
    Find which Google account owns a Gmail draft ID.

    Returns:
        Dict with ext_connection_id, gmail_draft_id, message_id
    """
    for connection_id in _get_google_connection_ids(user_id, user_jwt):
        service, _ = get_gmail_service_for_account(user_id, user_jwt, connection_id)
        if not service:
            continue

        try:
            draft = service.users().drafts().get(
                userId='me',
                id=gmail_draft_id,
                format='minimal'
            ).execute()
            return {
                'ext_connection_id': connection_id,
                'gmail_draft_id': gmail_draft_id,
                'message_id': (draft.get('message') or {}).get('id')
            }
        except HttpError as e:
            if e.resp.status in (400, 404):
                continue
            logger.warning(
                f"Error probing draft {gmail_draft_id} on connection {connection_id[:8]}...: {e}"
            )
        except Exception as e:
            logger.warning(
                f"Unexpected error probing draft {gmail_draft_id} on connection {connection_id[:8]}...: {e}"
            )

    return None


def _find_gmail_draft_by_message_id_across_accounts(
    user_id: str,
    user_jwt: str,
    message_id: str
) -> Optional[Dict[str, Optional[str]]]:
    """
    Find Gmail draft metadata from a Gmail message ID across all Google accounts.

    Returns:
        Dict with ext_connection_id, gmail_draft_id, message_id
    """
    for connection_id in _get_google_connection_ids(user_id, user_jwt):
        service, _ = get_gmail_service_for_account(user_id, user_jwt, connection_id)
        if not service:
            continue

        try:
            gmail_draft_id = find_gmail_draft_id_by_message_id(service, message_id)
            if gmail_draft_id:
                return {
                    'ext_connection_id': connection_id,
                    'gmail_draft_id': gmail_draft_id,
                    'message_id': message_id
                }
        except Exception as e:
            logger.warning(
                f"Error resolving message {message_id} to draft on connection {connection_id[:8]}...: {e}"
            )

    return None


def resolve_gmail_draft_reference(
    user_id: str,
    user_jwt: str,
    draft_identifier: str
) -> Optional[Dict[str, Optional[str]]]:
    """
    Resolve a draft identifier to Gmail draft ID + owning connection.

    `draft_identifier` may be:
    - Gmail draft ID (required by drafts.send/update/delete)
    - Gmail message ID (legacy client path using emails.external_id)
    """
    if not draft_identifier:
        return None

    auth_supabase = get_authenticated_supabase_client(user_jwt)
    draft_row = None

    # First: direct match by persisted gmail_draft_id.
    by_gmail_draft_id = auth_supabase.table('emails')\
        .select('id, external_id, ext_connection_id, gmail_draft_id, raw_item')\
        .eq('user_id', user_id)\
        .eq('is_draft', True)\
        .eq('gmail_draft_id', draft_identifier)\
        .limit(1)\
        .execute()
    if by_gmail_draft_id.data:
        draft_row = by_gmail_draft_id.data[0]

    # Second: legacy match by external_id (Gmail message ID).
    if not draft_row:
        by_message_id = auth_supabase.table('emails')\
            .select('id, external_id, ext_connection_id, gmail_draft_id, raw_item')\
            .eq('user_id', user_id)\
            .eq('is_draft', True)\
            .eq('external_id', draft_identifier)\
            .limit(1)\
            .execute()
        if by_message_id.data:
            draft_row = by_message_id.data[0]

    if draft_row:
        row_id = draft_row.get('id')
        message_id = draft_row.get('external_id')
        connection_id = draft_row.get('ext_connection_id')
        gmail_draft_id = (
            draft_row.get('gmail_draft_id')
            or _extract_gmail_draft_id_from_raw_item(draft_row.get('raw_item'))
        )

        # If caller already provided a Gmail draft ID, prefer it.
        if not gmail_draft_id and _looks_like_gmail_draft_id(draft_identifier):
            gmail_draft_id = draft_identifier

        # Legacy fallback: resolve message ID -> draft ID via drafts.list.
        if not gmail_draft_id and connection_id and message_id:
            service, _ = get_gmail_service_for_account(user_id, user_jwt, connection_id)
            gmail_draft_id = find_gmail_draft_id_by_message_id(service, message_id)

        # Last-resort account probe if row exists but account/draft metadata is incomplete.
        if not connection_id and gmail_draft_id:
            discovered = _find_gmail_draft_by_id_across_accounts(user_id, user_jwt, gmail_draft_id)
            if discovered:
                connection_id = discovered.get('ext_connection_id')
                message_id = message_id or discovered.get('message_id')
        elif connection_id and not gmail_draft_id and message_id:
            discovered = _find_gmail_draft_by_message_id_across_accounts(user_id, user_jwt, message_id)
            if discovered:
                gmail_draft_id = discovered.get('gmail_draft_id')

        # Backfill persisted Gmail draft ID for future calls.
        if row_id and gmail_draft_id and draft_row.get('gmail_draft_id') != gmail_draft_id:
            try:
                auth_supabase.table('emails')\
                    .update({'gmail_draft_id': gmail_draft_id})\
                    .eq('id', row_id)\
                    .execute()
            except Exception as e:
                logger.warning(f"Failed to backfill gmail_draft_id on row {row_id}: {e}")

        if connection_id and gmail_draft_id:
            return {
                'ext_connection_id': connection_id,
                'gmail_draft_id': gmail_draft_id,
                'message_id': message_id
            }

    # No usable draft row. Probe Gmail directly as a fallback.
    if _looks_like_gmail_draft_id(draft_identifier):
        return _find_gmail_draft_by_id_across_accounts(user_id, user_jwt, draft_identifier)

    return _find_gmail_draft_by_message_id_across_accounts(user_id, user_jwt, draft_identifier)


def get_gmail_service_for_account(user_id: str, user_jwt: str, account_id: str):
    """
    Get an authenticated Gmail API service instance for a specific account.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        account_id: The ext_connection_id to use for sending

    Returns:
        Tuple of (service, connection_id) or (None, None) if no connection
    """
    try:
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        logger.info(f"🔍 Getting Gmail service for account {account_id[:8]}...")

        # Get specific connection by ID
        connection_result = auth_supabase.table('ext_connections')\
            .select('id, access_token, refresh_token, token_expires_at, metadata')\
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

        logger.info(f"✅ Found Google connection (ID: {connection_id[:8]}...)")

        # Get valid credentials (refresh if needed)
        credentials = _get_google_credentials(connection_data)

        if not credentials:
            logger.error(f"❌ Unable to get valid credentials for account {account_id}")
            return None, None

        # Build Gmail API client
        service = build('gmail', 'v1', credentials=credentials)

        return service, connection_id

    except Exception as e:
        logger.error(f"❌ Error getting Gmail service for account: {str(e)}")
        return None, None


def build_gmail_service_from_connection_data(connection_data: Dict[str, Any]):
    """
    Build Gmail service from pre-fetched connection data (no DB query).

    Use this when you already have connection data from a JOIN query
    to avoid redundant database lookups.

    Args:
        connection_data: Dict with id, access_token, refresh_token,
                        token_expires_at, metadata, user_id

    Returns:
        Gmail service object, or None if token refresh fails
    """
    try:
        connection_id = connection_data.get('id')
        if not connection_id:
            logger.error("❌ No connection_id in connection_data")
            return None

        # Get valid credentials (refresh if needed)
        credentials = _get_google_credentials(connection_data)

        if not credentials:
            logger.error(f"❌ Unable to get valid credentials for connection {connection_id[:8]}...")
            return None

        # Build Gmail API client
        service = build('gmail', 'v1', credentials=credentials)

        return service

    except Exception as e:
        logger.error(f"❌ Error building Gmail service: {str(e)}")
        return None


def get_gmail_service(user_id: str, user_jwt: str, account_id: str = None):
    """
    Get an authenticated Gmail API service instance.

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
        return get_gmail_service_for_account(user_id, user_jwt, account_id)

    try:
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        logger.info(f"🔍 Looking for Google connection for user {user_id}")

        # Get user's Google OAuth connection, ordered by account_order
        # This handles all cases:
        # - Primary Google account (account_order=0) is returned first
        # - If no primary, the lowest account_order Google account is returned
        # - Works regardless of which provider the user signed up with
        # Note: is_primary is a GLOBAL flag across all providers, not per-provider,
        # so we don't filter by it - account_order handles ordering correctly
        connection_result = auth_supabase.table('ext_connections')\
            .select('id, access_token, refresh_token, token_expires_at, metadata')\
            .eq('user_id', user_id)\
            .eq('provider', 'google')\
            .eq('is_active', True)\
            .order('account_order', desc=False)\
            .limit(1)\
            .execute()

        # Extract single result from array
        if connection_result.data:
            connection_result.data = connection_result.data[0]
        
        if not connection_result.data:
            logger.warning(f"❌ No active Google connection found for user {user_id}")
            logger.info("💡 User needs to connect their Google account via OAuth")
            return None, None
        
        connection_data = decrypt_ext_connection_tokens(connection_result.data)
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

        # Build Gmail API client
        service = build('gmail', 'v1', credentials=credentials)
        
        logger.info("✅ Built Gmail API service")
        
        return service, connection_id
        
    except Exception as e:
        logger.error(f"❌ Error getting Gmail service: {str(e)}")
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


def create_message(to: str, subject: str, body: str, from_email: str = None, 
                  cc: List[str] = None, bcc: List[str] = None, 
                  html_body: str = None, attachments: List[Dict[str, Any]] = None,
                  in_reply_to: str = None, references: str = None) -> Dict[str, Any]:
    """
    Create a MIME message for sending via Gmail API
    
    Args:
        to: Recipient email address
        subject: Email subject
        body: Plain text email body
        from_email: Optional sender email (defaults to authenticated user)
        cc: Optional list of CC recipients
        bcc: Optional list of BCC recipients
        html_body: Optional HTML version of email body
        attachments: Optional list of attachments [{'filename': 'name', 'content': 'base64_data', 'mime_type': 'type'}]
        in_reply_to: Optional Message-ID of the email being replied to (required for threading)
        references: Optional chain of Message-IDs for the thread (required for threading)
        
    Returns:
        Dict with 'raw' key containing base64url-encoded message
    """
    # Create message container
    if attachments:
        message = MIMEMultipart('mixed')
    elif html_body:
        message = MIMEMultipart('alternative')
    else:
        message = MIMEText(body)
    
    message['to'] = to
    message['subject'] = subject
    if from_email:
        message['from'] = from_email
    if cc:
        message['cc'] = ', '.join(cc)
    if bcc:
        message['bcc'] = ', '.join(bcc)
    
    # Threading headers - CRITICAL for Gmail to properly chain messages
    if in_reply_to:
        message['In-Reply-To'] = in_reply_to
    if references:
        message['References'] = references
    elif in_reply_to:
        # If no references provided but we have in_reply_to, use it as references
        message['References'] = in_reply_to

    # If we have attachments, we need a body part (alternative or plain) inside the mixed part
    if attachments:
        if html_body:
            body_part = MIMEMultipart('alternative')
            part1 = MIMEText(body, 'plain')
            part2 = MIMEText(html_body, 'html')
            body_part.attach(part1)
            body_part.attach(part2)
            message.attach(body_part)
        else:
            message.attach(MIMEText(body, 'plain'))
            
        # Process attachments
        for attachment in attachments:
            try:
                part = MIMEBase(*attachment.get('mime_type', 'application/octet-stream').split('/'))
                part.set_payload(base64.b64decode(attachment['content']))
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition', 
                    f'attachment; filename="{attachment["filename"]}"'
                )
                message.attach(part)
            except Exception as e:
                logger.error(f"Failed to attach file {attachment.get('filename')}: {str(e)}")
                
    elif html_body:
        # No attachments, just HTML/Plain alternative
        part1 = MIMEText(body, 'plain')
        part2 = MIMEText(html_body, 'html')
        message.attach(part1)
        message.attach(part2)
    
    # If neither attachments nor html_body, it's already a MIMEText (handled at top)
        
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw_message}


def parse_email_headers(headers: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Parse email headers into a more accessible dictionary
    
    Args:
        headers: List of header dicts from Gmail API
        
    Returns:
        Dict with common headers (From, To, Subject, Date, etc.)
    """
    parsed = {}
    for header in headers:
        name = header.get('name', '').lower()
        value = header.get('value', '')
        
        if name in ['from', 'to', 'subject', 'date', 'cc', 'bcc', 'message-id', 'in-reply-to', 'references']:
            parsed[name] = value
    
    return parsed


def decode_email_body(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    Decode email body from Gmail API payload
    
    Args:
        payload: Email payload from Gmail API
        
    Returns:
        Dict with 'plain' and 'html' keys containing decoded body content
    """
    result = {'plain': '', 'html': ''}
    
    def get_body_from_part(part: Dict[str, Any]) -> None:
        """Recursively extract body from message parts"""
        mime_type = part.get('mimeType', '')
        
        if mime_type == 'text/plain':
            body_data = part.get('body', {}).get('data')
            if body_data:
                result['plain'] = base64.urlsafe_b64decode(body_data).decode('utf-8', errors='ignore')
        elif mime_type == 'text/html':
            body_data = part.get('body', {}).get('data')
            if body_data:
                result['html'] = base64.urlsafe_b64decode(body_data).decode('utf-8', errors='ignore')
        elif mime_type.startswith('multipart/'):
            # Recursively process multipart messages
            for subpart in part.get('parts', []):
                get_body_from_part(subpart)
    
    # Start processing from top-level payload
    if 'parts' in payload:
        for part in payload['parts']:
            get_body_from_part(part)
    else:
        # Single-part message
        get_body_from_part(payload)
    
    return result


def get_attachment_info(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract attachment information from email payload
    
    Args:
        payload: Email payload from Gmail API
        
    Returns:
        List of attachment info dicts with filename, mimeType, size, attachmentId
    """
    attachments = []
    
    def extract_attachments(part: Dict[str, Any]) -> None:
        """Recursively extract attachment info"""
        filename = part.get('filename')
        body = part.get('body', {})
        
        if filename and body.get('attachmentId'):
            attachments.append({
                'filename': filename,
                'mimeType': part.get('mimeType'),
                'size': body.get('size', 0),
                'attachmentId': body.get('attachmentId')
            })
        
        # Process nested parts
        for subpart in part.get('parts', []):
            extract_attachments(subpart)
    
    extract_attachments(payload)
    return attachments


def convert_to_gmail_label_ids(label_names: List[str], service) -> List[str]:
    """
    Convert label names to Gmail label IDs
    
    Args:
        label_names: List of label names (e.g., ['INBOX', 'IMPORTANT'])
        service: Gmail API service instance
        
    Returns:
        List of label IDs
    """
    # System labels are uppercase and can be used directly
    system_labels = ['INBOX', 'SPAM', 'TRASH', 'UNREAD', 'STARRED', 'IMPORTANT', 
                     'SENT', 'DRAFT', 'CATEGORY_PERSONAL', 'CATEGORY_SOCIAL', 
                     'CATEGORY_PROMOTIONS', 'CATEGORY_UPDATES', 'CATEGORY_FORUMS']
    
    label_ids = []
    
    # Get all labels to find custom label IDs
    labels_result = service.users().labels().list(userId='me').execute()
    all_labels = labels_result.get('labels', [])
    
    # Create mapping of name to ID
    label_map = {label['name']: label['id'] for label in all_labels}
    
    for name in label_names:
        if name.upper() in system_labels:
            label_ids.append(name.upper())
        elif name in label_map:
            label_ids.append(label_map[name])
        else:
            logger.warning(f"Label '{name}' not found")
    
    return label_ids

