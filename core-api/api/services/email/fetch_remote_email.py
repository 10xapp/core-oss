"""
Fetch Remote Email — Lazy sync for provider-only search results.

When a user clicks on a search result that exists only on the provider (not locally synced),
this module fetches the full email from Gmail or Microsoft Graph and upserts it to the database.
"""
import asyncio
import logging
from typing import Dict, Any
from datetime import datetime, timezone

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import httpx

from api.services.google_auth import get_valid_credentials
from api.services.microsoft.microsoft_oauth_provider import get_valid_microsoft_credentials
from api.services.email.google_api_helpers import (
    parse_email_headers,
    decode_email_body,
    get_attachment_info,
)
from api.services.syncs.sync_outlook import build_outlook_labels
from api.services.email.label_normalization import normalize_labels_canonical
from lib.supabase_client import get_authenticated_async_client, get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens

logger = logging.getLogger(__name__)


async def fetch_remote_email(
    user_id: str,
    user_jwt: str,
    external_id: str,
    connection_id: str,
) -> Dict[str, Any]:
    """
    Fetch a single email from the provider, upsert to DB, and return it.

    Looks up the connection to determine provider, then dispatches.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT
        external_id: Provider's message ID
        connection_id: ext_connection_id

    Returns:
        Dict with full email data
    """
    auth_supabase = await get_authenticated_async_client(user_jwt)

    # Look up connection to determine provider
    conn_result = await auth_supabase.table('ext_connections')\
        .select('id, provider, provider_email, access_token, refresh_token, token_expires_at, metadata')\
        .eq('id', connection_id)\
        .eq('user_id', user_id)\
        .eq('is_active', True)\
        .maybe_single()\
        .execute()

    if not conn_result.data:
        raise ValueError(f"No active connection found for id={connection_id}")

    connection_data = decrypt_ext_connection_tokens(conn_result.data)
    connection_data['user_id'] = user_id
    provider = connection_data['provider']

    if provider == 'google':
        return await fetch_remote_gmail(connection_data, external_id, user_id)
    elif provider == 'microsoft':
        return await fetch_remote_outlook(connection_data, external_id, user_id)
    else:
        raise ValueError(f"Unsupported provider: {provider}")


async def fetch_remote_gmail(
    connection_data: Dict[str, Any],
    external_id: str,
    user_id: str,
) -> Dict[str, Any]:
    """
    Fetch full email from Gmail, upsert to DB, and return it.
    """
    connection_id = connection_data['id']
    logger.info(f"[Fetch Gmail] external_id={external_id} conn={connection_id[:8]}...")

    def _fetch_full():
        creds = get_valid_credentials(connection_data)
        service = build('gmail', 'v1', credentials=creds)
        return service.users().messages().get(
            userId='me',
            id=external_id,
            format='full',
        ).execute()

    try:
        msg = await asyncio.to_thread(_fetch_full)
    except HttpError as e:
        logger.error(f"[Fetch Gmail] API error: {e}")
        raise ValueError(f"Failed to fetch email from Gmail: {e}")

    payload = msg.get('payload', {})
    headers = parse_email_headers(payload.get('headers', []))
    body = decode_email_body(payload)
    attachments = get_attachment_info(payload)
    labels = msg.get('labelIds', [])
    normalized_labels = normalize_labels_canonical(labels)

    to_raw = headers.get('to', '')
    to_list = [addr.strip() for addr in to_raw.split(',')] if to_raw else []
    cc_raw = headers.get('cc', '')
    cc_list = [addr.strip() for addr in cc_raw.split(',')] if cc_raw else []

    email_data = {
        'user_id': user_id,
        'ext_connection_id': connection_id,
        'external_id': external_id,
        'thread_id': msg.get('threadId'),
        'subject': headers.get('subject', '(No Subject)'),
        'from': headers.get('from', ''),
        'to': to_list,
        'cc': cc_list if cc_list else None,
        'body': body.get('html', '') or body.get('plain', ''),
        'snippet': msg.get('snippet', ''),
        'labels': labels,
        'normalized_labels': normalized_labels,
        'is_read': 'UNREAD' not in labels,
        'is_starred': 'STARRED' in labels,
        'is_draft': 'DRAFT' in labels,
        'received_at': headers.get('date'),
        'has_attachments': len(attachments) > 0,
        'attachments': attachments,
        'synced_at': datetime.now(timezone.utc).isoformat(),
    }

    # Upsert to database
    supabase = get_service_role_client()
    try:
        supabase.table('emails')\
            .upsert(email_data, on_conflict='user_id,external_id')\
            .execute()
        logger.info(f"[Fetch Gmail] Upserted email {external_id}")
    except Exception as e:
        logger.warning(f"[Fetch Gmail] Upsert failed (non-fatal): {e}")

    # Return in API format
    return {
        'external_id': external_id,
        'thread_id': msg.get('threadId'),
        'subject': headers.get('subject', '(No Subject)'),
        'from': headers.get('from', ''),
        'to': to_raw,
        'cc': cc_raw,
        'snippet': msg.get('snippet', ''),
        'body_plain': body.get('plain', ''),
        'body_html': body.get('html', ''),
        'labels': labels,
        'normalized_labels': normalized_labels,
        'is_unread': 'UNREAD' in labels,
        'is_starred': 'STARRED' in labels,
        'has_attachments': len(attachments) > 0,
        'attachments': attachments,
        'received_at': headers.get('date'),
        'ext_connection_id': connection_id,
        'account_email': connection_data.get('provider_email'),
        'account_provider': 'google',
        'source': 'remote',
    }


# Outlook fields for full message fetch
_OUTLOOK_FULL_FIELDS = ",".join([
    "id", "subject", "from", "toRecipients", "ccRecipients",
    "body", "bodyPreview", "receivedDateTime", "hasAttachments",
    "isRead", "isDraft", "flag", "conversationId", "importance",
    "parentFolderId", "categories", "attachments",
])


async def fetch_remote_outlook(
    connection_data: Dict[str, Any],
    external_id: str,
    user_id: str,
) -> Dict[str, Any]:
    """
    Fetch full email from Microsoft Graph, upsert to DB, and return it.
    """
    connection_id = connection_data['id']
    logger.info(f"[Fetch Outlook] external_id={external_id[:20]}... conn={connection_id[:8]}...")

    supabase = get_service_role_client()
    access_token = get_valid_microsoft_credentials(connection_data, supabase)

    url = f"https://graph.microsoft.com/v1.0/me/messages/{external_id}"
    params = {
        "$select": _OUTLOOK_FULL_FIELDS,
        "$expand": "attachments",
    }
    headers_req = {"Authorization": f"Bearer {access_token}"}

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, params=params, headers=headers_req)

    if resp.status_code != 200:
        error_msg = resp.text[:200]
        logger.error(f"[Fetch Outlook] API error {resp.status_code}: {error_msg}")
        raise ValueError(f"Failed to fetch email from Outlook: {error_msg}")

    msg = resp.json()

    # Parse fields
    from_field = msg.get('from', {}).get('emailAddress', {})
    from_email = from_field.get('address', '')
    from_name = from_field.get('name', '')
    from_str = f"{from_name} <{from_email}>" if from_name else from_email

    to_list = [
        r.get('emailAddress', {}).get('address', '')
        for r in msg.get('toRecipients', [])
    ]
    cc_list = [
        r.get('emailAddress', {}).get('address', '')
        for r in msg.get('ccRecipients', [])
    ]

    body_obj = msg.get('body', {})
    body_content = body_obj.get('content', '')
    body_type = body_obj.get('contentType', 'text')

    labels = build_outlook_labels(msg)
    normalized_labels = normalize_labels_canonical(labels)

    flag = msg.get('flag', {})
    is_starred = flag.get('flagStatus') == 'flagged'

    # Parse attachments
    attachments = []
    for att in msg.get('attachments', []):
        attachments.append({
            'id': att.get('id'),
            'name': att.get('name'),
            'contentType': att.get('contentType'),
            'size': att.get('size'),
        })

    email_data = {
        'user_id': user_id,
        'ext_connection_id': connection_id,
        'external_id': external_id,
        'thread_id': msg.get('conversationId'),
        'subject': msg.get('subject', '(No Subject)'),
        'from': from_str,
        'to': to_list,
        'cc': cc_list if cc_list else None,
        'body': body_content,
        'snippet': msg.get('bodyPreview', ''),
        'labels': labels,
        'normalized_labels': normalized_labels,
        'is_read': msg.get('isRead', False),
        'is_starred': is_starred,
        'is_draft': msg.get('isDraft', False),
        'received_at': msg.get('receivedDateTime'),
        'has_attachments': msg.get('hasAttachments', False),
        'attachments': attachments,
        'synced_at': datetime.now(timezone.utc).isoformat(),
    }

    # Upsert to database
    try:
        supabase.table('emails')\
            .upsert(email_data, on_conflict='user_id,external_id')\
            .execute()
        logger.info(f"[Fetch Outlook] Upserted email {external_id[:20]}...")
    except Exception as e:
        logger.warning(f"[Fetch Outlook] Upsert failed (non-fatal): {e}")

    # Return in API format
    return {
        'external_id': external_id,
        'thread_id': msg.get('conversationId'),
        'subject': msg.get('subject', '(No Subject)'),
        'from': from_str,
        'to': ', '.join(to_list),
        'cc': ', '.join(cc_list),
        'snippet': msg.get('bodyPreview', ''),
        'body_plain': body_content if body_type == 'text' else '',
        'body_html': body_content if body_type == 'html' else '',
        'labels': labels,
        'normalized_labels': normalized_labels,
        'is_unread': not msg.get('isRead', True),
        'is_starred': is_starred,
        'has_attachments': msg.get('hasAttachments', False),
        'attachments': attachments,
        'received_at': msg.get('receivedDateTime'),
        'ext_connection_id': connection_id,
        'account_email': connection_data.get('provider_email'),
        'account_provider': 'microsoft',
        'source': 'remote',
    }
