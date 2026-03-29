"""
Email service - Get full email details
Supports both Gmail and Outlook.
"""
from typing import Dict, Any
from datetime import datetime, timezone
from fastapi import HTTPException, status
from lib.supabase_client import get_authenticated_supabase_client, get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens
import logging
import requests
from googleapiclient.errors import HttpError
from .google_api_helpers import (
    get_gmail_service,
    build_gmail_service_from_connection_data,
    parse_email_headers,
    decode_email_body,
    get_attachment_info
)
from api.services.microsoft.microsoft_oauth_provider import get_valid_microsoft_credentials
from api.services.microsoft.microsoft_email_sync_provider import MicrosoftEmailSyncProvider

logger = logging.getLogger(__name__)

# Microsoft Graph API base URL
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

# Fields to request from Microsoft Graph
OUTLOOK_MESSAGE_FIELDS = ",".join([
    "id",
    "subject",
    "from",
    "toRecipients",
    "ccRecipients",
    "bccRecipients",
    "body",
    "bodyPreview",
    "isRead",
    "isDraft",
    "flag",
    "conversationId",
    "receivedDateTime",
    "hasAttachments",
    "importance",
    "parentFolderId",
    "categories",
    "internetMessageId",
    "internetMessageHeaders"
])


def _get_email_with_connection(user_id: str, email_id: str, user_jwt: str) -> tuple[dict, str, dict, str]:
    """
    Get full email data with connection info in a single query.

    Args:
        user_id: User's ID
        email_id: External email ID (Gmail message ID or Outlook message ID)
        user_jwt: User's Supabase JWT

    Returns:
        Tuple of (email_data, connection_id, connection_data, provider)
        email_data contains all email fields including body (if synced)
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get FULL email with connection info in ONE query
    # This avoids needing to call Gmail API if body is already synced
    result = auth_supabase.table('emails')\
        .select('''
            id, external_id, thread_id, subject, "from", "to", cc, bcc,
            body, snippet, labels, is_read, is_starred, is_draft, is_trashed,
            received_at, has_attachments, attachments, raw_item, gmail_draft_id,
            ext_connection_id,
            ext_connections(id, provider, access_token, refresh_token, token_expires_at, metadata)
        ''')\
        .eq('user_id', user_id)\
        .eq('external_id', email_id)\
        .maybe_single()\
        .execute()

    if not result.data:
        # Email not in DB yet
        return None, None, None, None

    email_data = result.data
    connection = decrypt_ext_connection_tokens(email_data.get('ext_connections', {}))
    provider = connection.get('provider', 'google')
    connection_id = connection.get('id')

    # Build connection_data for token refresh (include user_id for refresh logic)
    connection_data = {
        'id': connection_id,
        'user_id': user_id,
        'access_token': connection.get('access_token'),
        'refresh_token': connection.get('refresh_token'),
        'token_expires_at': connection.get('token_expires_at'),
        'metadata': connection.get('metadata', {}),
    }

    return email_data, connection_id, connection_data, provider


def _format_cached_email(email_data: dict, provider: str) -> Dict[str, Any]:
    """
    Format cached email data from DB into API response format.

    Args:
        email_data: Email row from database
        provider: 'google' or 'microsoft'

    Returns:
        Formatted email details dict
    """
    # Parse body - check if it's HTML or plain
    body_content = email_data.get('body', '')
    body_html = ''
    body_plain = ''

    if body_content:
        # If body contains HTML tags, treat as HTML
        if '<' in body_content and '>' in body_content:
            body_html = body_content
        else:
            body_plain = body_content

    # Format to/cc/bcc - they're stored as arrays in DB
    to_list = email_data.get('to', []) or []
    cc_list = email_data.get('cc', []) or []
    bcc_list = email_data.get('bcc', []) or []

    to_str = ', '.join(to_list) if isinstance(to_list, list) else str(to_list)
    cc_str = ', '.join(cc_list) if isinstance(cc_list, list) else str(cc_list) if cc_list else None
    bcc_str = ', '.join(bcc_list) if isinstance(bcc_list, list) else str(bcc_list) if bcc_list else None

    labels = email_data.get('labels', []) or []
    is_unread = not email_data.get('is_read', True)
    is_starred = email_data.get('is_starred', False)
    is_draft = email_data.get('is_draft', False)
    is_important = 'IMPORTANT' in labels if labels else False

    # NOTE: Threading headers (message_id, in_reply_to, references) are not stored in DB.
    # This is intentional - they're only needed for replies, and the reply flow in
    # send_email.py auto-fetches these headers when needed (lines 69-94).
    # For display purposes, these fields are not required.
    return {
        'id': email_data.get('external_id'),
        'thread_id': email_data.get('thread_id'),
        'subject': email_data.get('subject', '(No Subject)'),
        'from': email_data.get('from', ''),
        'to': to_str,
        'cc': cc_str,
        'bcc': bcc_str,
        'date': email_data.get('received_at'),
        'message_id': None,  # Not stored - fetched on-demand when replying
        'in_reply_to': None,  # Not stored - fetched on-demand when replying
        'references': None,  # Not stored - fetched on-demand when replying
        'snippet': email_data.get('snippet', ''),
        'body_plain': body_plain,
        'body_html': body_html,
        'labels': labels,
        'is_unread': is_unread,
        'is_starred': is_starred,
        'is_important': is_important,
        'is_draft': is_draft,
        'internal_date': email_data.get('received_at'),
        'size_estimate': 0,
        'attachments': email_data.get('attachments', []) or [],
        'has_attachments': email_data.get('has_attachments', False),
        'raw_item': email_data.get('raw_item'),
        'gmail_draft_id': email_data.get('gmail_draft_id')
    }


def _build_outlook_labels(msg: Dict[str, Any]) -> list:
    """Build labels array for Outlook email."""
    labels = ['Inbox']  # Default folder

    if msg.get('isRead') is False:
        labels.append('UNREAD')

    flag = msg.get('flag', {})
    if flag.get('flagStatus') == 'flagged':
        labels.append('STARRED')

    if msg.get('isDraft') is True:
        labels.append('Drafts')

    if msg.get('importance') == 'high':
        labels.append('IMPORTANT')

    categories = msg.get('categories', [])
    if categories:
        labels.extend(categories)

    return labels


def _get_outlook_email_details(
    email_id: str,
    connection_id: str,
    connection_data: dict,
    user_id: str,
    user_jwt: str
) -> Dict[str, Any]:
    """
    Get full email details from Microsoft Graph API.

    Args:
        email_id: Outlook message ID
        connection_id: Connection ID
        connection_data: Connection data with tokens
        user_id: User's ID
        user_jwt: User's Supabase JWT

    Returns:
        Dict with complete email details
    """
    supabase = get_service_role_client()
    auth_supabase = get_authenticated_supabase_client(user_jwt)
    access_token = get_valid_microsoft_credentials(connection_data, supabase)

    url = f"{GRAPH_API_URL}/me/messages/{email_id}?$select={OUTLOOK_MESSAGE_FIELDS}"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code != 200:
        error_msg = response.text
        try:
            error_msg = response.json().get('error', {}).get('message', response.text)
        except Exception:
            pass
        raise ValueError(f"Failed to get Outlook email: {error_msg}")

    msg = response.json()

    # Parse using the provider
    parser = MicrosoftEmailSyncProvider()
    parsed = parser.parse_email(msg)

    # Build labels
    labels = _build_outlook_labels(msg)

    # Parse from field for display
    from_field = msg.get('from', {}).get('emailAddress', {})
    from_email = from_field.get('address', '')
    from_name = from_field.get('name', '')
    from_str = f"{from_name} <{from_email}>" if from_name else from_email

    # Parse to recipients
    to_list = []
    for recipient in msg.get('toRecipients', []):
        addr = recipient.get('emailAddress', {})
        email = addr.get('address', '')
        name = addr.get('name', '')
        to_list.append(f"{name} <{email}>" if name else email)

    # Parse CC recipients
    cc_list = []
    for recipient in msg.get('ccRecipients', []):
        addr = recipient.get('emailAddress', {})
        email = addr.get('address', '')
        name = addr.get('name', '')
        cc_list.append(f"{name} <{email}>" if name else email)

    # Parse BCC recipients
    bcc_list = []
    for recipient in msg.get('bccRecipients', []):
        addr = recipient.get('emailAddress', {})
        email = addr.get('address', '')
        name = addr.get('name', '')
        bcc_list.append(f"{name} <{email}>" if name else email)

    # Body
    body_obj = msg.get('body', {})
    body_content = body_obj.get('content', '')
    body_type = body_obj.get('contentType', 'text')  # "text" or "html"

    # Check flags
    is_unread = not msg.get('isRead', True)
    flag = msg.get('flag', {})
    is_starred = flag.get('flagStatus') == 'flagged'
    is_draft = msg.get('isDraft', False)
    is_important = msg.get('importance') == 'high'

    # Parse internet message headers for message-id, in-reply-to, references
    headers_list = msg.get('internetMessageHeaders', [])
    headers_dict = {h.get('name', '').lower(): h.get('value', '') for h in headers_list}

    email_details = {
        'id': email_id,
        'thread_id': msg.get('conversationId'),
        'subject': msg.get('subject', '(No Subject)'),
        'from': from_str,
        'to': ', '.join(to_list),
        'cc': ', '.join(cc_list) if cc_list else None,
        'bcc': ', '.join(bcc_list) if bcc_list else None,
        'date': msg.get('receivedDateTime'),
        'message_id': msg.get('internetMessageId') or headers_dict.get('message-id'),
        'in_reply_to': headers_dict.get('in-reply-to'),
        'references': headers_dict.get('references'),
        'snippet': msg.get('bodyPreview', ''),
        'body_plain': body_content if body_type == 'text' else '',
        'body_html': body_content if body_type == 'html' else '',
        'labels': labels,
        'is_unread': is_unread,
        'is_starred': is_starred,
        'is_important': is_important,
        'is_draft': is_draft,
        'internal_date': msg.get('receivedDateTime'),
        'size_estimate': 0,  # Microsoft doesn't provide this directly
        'attachments': parsed['attachments'],
        'has_attachments': msg.get('hasAttachments', False),
        'raw_item': msg
    }

    # Store/update in database
    existing = auth_supabase.table('emails')\
        .select('id')\
        .eq('user_id', user_id)\
        .eq('external_id', email_id)\
        .execute()

    # Parse addresses into arrays for DB
    to_addresses = [addr.strip() for addr in email_details['to'].split(',')] if email_details['to'] else []
    cc_addresses = [addr.strip() for addr in email_details.get('cc', '').split(',')] if email_details.get('cc') else []
    bcc_addresses = [addr.strip() for addr in email_details.get('bcc', '').split(',')] if email_details.get('bcc') else []

    # Convert received date
    received_at = msg.get('receivedDateTime', datetime.now(timezone.utc).isoformat())

    db_data = {
        'user_id': user_id,
        'ext_connection_id': connection_id,
        'external_id': email_id,
        'thread_id': msg.get('conversationId'),
        'subject': email_details['subject'],
        'from': from_str,
        'to': to_addresses,
        'cc': cc_addresses if cc_addresses else None,
        'bcc': bcc_addresses if bcc_addresses else None,
        'body': body_content,
        'snippet': email_details['snippet'],
        'labels': labels,
        'is_read': not is_unread,
        'is_starred': is_starred,
        'is_draft': is_draft,
        'received_at': received_at,
        'has_attachments': email_details['has_attachments'],
        'attachments': parsed['attachments'],
        'synced_at': datetime.now(timezone.utc).isoformat(),
        'raw_item': msg
    }

    if existing.data:
        auth_supabase.table('emails')\
            .update(db_data)\
            .eq('id', existing.data[0]['id'])\
            .execute()
    else:
        auth_supabase.table('emails')\
            .insert(db_data)\
            .execute()

    logger.info(f"✅ [Outlook] Retrieved email details for message {email_id}")

    return {
        "message": "Email details retrieved successfully",
        "email": email_details,
        "provider": "microsoft"
    }


def get_email_details(
    user_id: str,
    user_jwt: str,
    email_id: str,
    format: str = 'full'
) -> Dict[str, Any]:
    """
    Get full details of a specific email including body content.
    Supports both Gmail and Outlook.

    OPTIMIZED: Returns cached email from DB if body exists (no API call needed).
    Only calls Gmail/Outlook API if body is missing (rare edge case).

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        email_id: Email message ID (Gmail or Outlook)
        format: Message format ('full', 'metadata', 'minimal', 'raw')

    Returns:
        Dict with complete email details including body content
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get full email data + connection info in ONE query
    email_data, connection_id, connection_data, provider = _get_email_with_connection(user_id, email_id, user_jwt)

    # For drafts, always trust cached DB row, even when body is empty.
    # Gmail messages.get() returns the message object (no draft wrapper), which can drop draft metadata.
    has_cached_body = email_data is not None and email_data.get('body') not in (None, '')
    if email_data and (email_data.get('is_draft') or has_cached_body):
        logger.info(f"✅ [CACHE HIT] Returning cached email {email_id} (no API call)")
        email_details = _format_cached_email(email_data, provider or 'google')
        return {
            "message": "Email details retrieved from cache",
            "email": email_details,
            "provider": provider or "google",
            "cached": True
        }

    # Body not in cache - need to fetch from API
    logger.info(f"📧 [CACHE MISS] Body not cached for {email_id}, fetching from API")

    if provider == 'microsoft':
        return _get_outlook_email_details(
            email_id=email_id,
            connection_id=connection_id,
            connection_data=connection_data,
            user_id=user_id,
            user_jwt=user_jwt
        )

    # Gmail path - use pre-fetched connection data to avoid redundant DB query
    service = None
    if connection_data:
        service = build_gmail_service_from_connection_data(connection_data)

    if not service:
        # Fallback: email not in DB yet, query for connection
        service, returned_conn_id = get_gmail_service(user_id, user_jwt, account_id=connection_id)
        if not connection_id:
            connection_id = returned_conn_id

    if not service:
        # No Gmail connection - check if user has Microsoft connection
        connections_result = auth_supabase.table('ext_connections')\
            .select('id, provider, access_token, refresh_token, token_expires_at, metadata')\
            .eq('user_id', user_id)\
            .eq('is_active', True)\
            .execute()

        if connections_result.data:
            for conn in connections_result.data:
                if conn.get('provider') == 'microsoft':
                    # Try with Microsoft
                    decrypted_conn = decrypt_ext_connection_tokens(conn)
                    connection_data = {
                        'id': decrypted_conn['id'],
                        'access_token': decrypted_conn.get('access_token'),
                        'refresh_token': decrypted_conn.get('refresh_token'),
                        'token_expires_at': decrypted_conn.get('token_expires_at'),
                        'metadata': decrypted_conn.get('metadata', {}),
                    }
                    return _get_outlook_email_details(
                        email_id=email_id,
                        connection_id=conn['id'],
                        connection_data=connection_data,
                        user_id=user_id,
                        user_jwt=user_jwt
                    )

        raise ValueError("No active email connection found for user. Please sign in with Google or Microsoft first.")

    try:
        # Fetch full message details from Gmail
        full_msg = service.users().messages().get(
            userId='me',
            id=email_id,
            format=format
        ).execute()

        # Parse headers
        headers = parse_email_headers(full_msg.get('payload', {}).get('headers', []))

        # Get basic info
        snippet = full_msg.get('snippet', '')
        labels = full_msg.get('labelIds', [])
        thread_id = full_msg.get('threadId')
        internal_date = full_msg.get('internalDate')
        size_estimate = full_msg.get('sizeEstimate', 0)

        # Decode body content
        body = decode_email_body(full_msg.get('payload', {}))

        # Get attachments info
        attachments = get_attachment_info(full_msg.get('payload', {}))

        # Check various flags
        is_unread = 'UNREAD' in labels
        is_starred = 'STARRED' in labels
        is_important = 'IMPORTANT' in labels
        is_draft = 'DRAFT' in labels

        email_details = {
            'id': email_id,
            'thread_id': thread_id,
            'subject': headers.get('subject', '(No Subject)'),
            'from': headers.get('from', ''),
            'to': headers.get('to', ''),
            'cc': headers.get('cc'),
            'bcc': headers.get('bcc'),
            'date': headers.get('date'),
            'message_id': headers.get('message-id'),
            'in_reply_to': headers.get('in-reply-to'),
            'references': headers.get('references'),
            'snippet': snippet,
            'body_plain': body.get('plain', ''),
            'body_html': body.get('html', ''),
            'labels': labels,
            'is_unread': is_unread,
            'is_starred': is_starred,
            'is_important': is_important,
            'is_draft': is_draft,
            'internal_date': internal_date,
            'size_estimate': size_estimate,
            'attachments': attachments,
            'has_attachments': len(attachments) > 0,
            'raw_item': full_msg
        }

        # Store/update in database for caching
        # Use email_db_id from initial query if available (avoids redundant DB lookup)

        # Parse addresses into arrays
        to_addresses = [addr.strip() for addr in email_details['to'].split(',')] if email_details['to'] else []
        cc_addresses = [addr.strip() for addr in email_details.get('cc', '').split(',')] if email_details.get('cc') else []
        bcc_addresses = [addr.strip() for addr in email_details.get('bcc', '').split(',')] if email_details.get('bcc') else []

        # Convert internal date to received_at
        if internal_date:
            received_at = datetime.fromtimestamp(
                int(internal_date) / 1000,
                tz=timezone.utc
            ).isoformat()
        else:
            received_at = datetime.now(timezone.utc).isoformat()

        # Use HTML body if available, otherwise fallback to plain text
        body_content = body.get('html') or body.get('plain', '')

        existing_raw_item = email_data.get('raw_item') if email_data else None
        # Preserve draft wrapper payload when available so draft.id is not lost.
        if is_draft and isinstance(existing_raw_item, dict) and existing_raw_item.get('message'):
            raw_item_to_store = existing_raw_item
        else:
            raw_item_to_store = full_msg

        db_data = {
            'user_id': user_id,
            'ext_connection_id': connection_id,
            'external_id': email_id,
            'gmail_draft_id': email_data.get('gmail_draft_id') if email_data else None,
            'thread_id': thread_id,
            'subject': email_details['subject'],
            'from': email_details['from'],
            'to': to_addresses,
            'cc': cc_addresses if cc_addresses else None,
            'bcc': bcc_addresses if bcc_addresses else None,
            'body': body_content,
            'snippet': snippet,
            'labels': labels,
            'is_read': not is_unread,
            'is_starred': is_starred,
            'is_draft': is_draft,
            'received_at': received_at,
            'has_attachments': len(attachments) > 0,
            'attachments': attachments,
            'synced_at': datetime.now(timezone.utc).isoformat(),
            'raw_item': raw_item_to_store
        }

        # Get email_db_id from email_data if available
        email_db_id = email_data.get('id') if email_data else None

        if email_db_id:
            # Update existing email (we have the id from initial query)
            auth_supabase.table('emails')\
                .update(db_data)\
                .eq('id', email_db_id)\
                .execute()
        else:
            # Insert new email (wasn't in DB before)
            auth_supabase.table('emails')\
                .insert(db_data)\
                .execute()

        logger.info(f"✅ [Gmail] Retrieved email details for message {email_id}")

        return {
            "message": "Email details retrieved successfully",
            "email": email_details,
            "provider": "google"
        }

    except HttpError as e:
        logger.error(f"Gmail API error: {str(e)}")
        # Stale IDs can happen right after draft send (old draft message ID no longer exists).
        # Treat provider 404/400 as not-found instead of bubbling as a 500.
        provider_status = getattr(getattr(e, 'resp', None), 'status', None)
        if provider_status in (400, 404):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Email not found",
            )
        raise ValueError(f"Failed to get email details: {str(e)}")
    except Exception as e:
        logger.error(f"Error getting email details: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise ValueError(f"Failed to retrieve email: {str(e)}")


def get_email_attachment(
    user_id: str,
    user_jwt: str,
    email_id: str,
    attachment_id: str
) -> Dict[str, Any]:
    """
    Get a specific email attachment.
    Supports both Gmail and Outlook.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        email_id: Email message ID
        attachment_id: Attachment ID

    Returns:
        Dict with attachment data (base64 encoded)
    """
    # Try to determine provider
    email_data, connection_id, connection_data, provider = _get_email_with_connection(user_id, email_id, user_jwt)

    if provider == 'microsoft':
        # Fetch attachment from Microsoft Graph
        supabase = get_service_role_client()
        access_token = get_valid_microsoft_credentials(connection_data, supabase)

        url = f"{GRAPH_API_URL}/me/messages/{email_id}/attachments/{attachment_id}"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            error_msg = response.text
            try:
                error_msg = response.json().get('error', {}).get('message', response.text)
            except Exception:
                pass
            raise ValueError(f"Failed to get Outlook attachment: {error_msg}")

        attachment_data = response.json()

        return {
            "message": "Attachment retrieved successfully",
            "attachment": {
                'attachmentId': attachment_id,
                'name': attachment_data.get('name'),
                'contentType': attachment_data.get('contentType'),
                'data': attachment_data.get('contentBytes'),  # Base64 encoded
                'size': attachment_data.get('size', 0)
            },
            "provider": "microsoft"
        }

    # Gmail path - use pre-fetched connection data to avoid redundant DB query
    service = None
    if connection_data:
        service = build_gmail_service_from_connection_data(connection_data)

    if not service:
        # Fallback if no connection_data
        service, _ = get_gmail_service(user_id, user_jwt, account_id=connection_id)

    if not service:
        raise ValueError("No active email connection found for user.")

    try:
        attachment = service.users().messages().attachments().get(
            userId='me',
            messageId=email_id,
            id=attachment_id
        ).execute()

        return {
            "message": "Attachment retrieved successfully",
            "attachment": {
                'attachmentId': attachment_id,
                'data': attachment.get('data'),  # Base64url encoded
                'size': attachment.get('size', 0)
            },
            "provider": "google"
        }

    except HttpError as e:
        logger.error(f"Gmail API error: {str(e)}")
        raise ValueError(f"Failed to get attachment: {str(e)}")
