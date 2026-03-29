"""
Email Search Providers — Provider-side search via Gmail API and Microsoft Graph API.

Enables searching the user's entire mailbox (not just locally synced emails).
Results from local DB and provider APIs are fetched in parallel, merged, and deduplicated.
"""
import asyncio
import re
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional, Dict, Any, List

import httpx
from googleapiclient.discovery import build

from api.services.google_auth import get_valid_credentials
from api.services.microsoft.microsoft_oauth_provider import get_valid_microsoft_credentials
from api.services.email.google_api_helpers import parse_email_headers
from api.services.email.fetch_emails import fetch_emails
from api.services.syncs.sync_outlook import build_outlook_labels
from lib.supabase_client import get_authenticated_async_client, get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# QueryTranslator
# ---------------------------------------------------------------------------

class QueryTranslator:
    """Converts unified search syntax to provider-specific query formats."""

    # Operators recognized in our unified syntax
    _OPERATOR_RE = re.compile(
        r'(has:\S+|is:\S+|after:\S+|before:\S+|from:\S+|to:\S+|subject:\S+|in:\S+|label:\S+)',
        re.IGNORECASE,
    )

    @staticmethod
    def to_gmail(query: str) -> str:
        """
        Convert unified syntax to Gmail search query.

        Mostly pass-through. Converts date formats:
        after:2024-01-15 -> after:2024/01/15
        before:2024-01-15 -> before:2024/01/15
        """
        def _fix_date(m):
            token = m.group(0)
            if token.startswith(('after:', 'before:')):
                return token.replace('-', '/')
            return token

        return QueryTranslator._OPERATOR_RE.sub(_fix_date, query)

    @staticmethod
    def to_kql(query: str) -> str:
        """
        Convert unified syntax to Microsoft KQL (Keyword Query Language).

        Mappings:
        - has:attachment   -> hasAttachments:true
        - is:unread        -> isRead:false
        - is:read          -> isRead:true
        - after:YYYY-MM-DD -> received>=YYYY-MM-DD
        - before:YYYY-MM-DD-> received<=YYYY-MM-DD
        - from:X           -> from:X  (same)
        - to:X             -> to:X    (same)
        - subject:X        -> subject:X (same)
        - in:X / label:X   -> dropped (not supported in KQL search)
        """
        result = query

        # Map operators
        result = re.sub(r'has:attachment\b', 'hasAttachments:true', result, flags=re.IGNORECASE)
        result = re.sub(r'is:unread\b', 'isRead:false', result, flags=re.IGNORECASE)
        result = re.sub(r'is:read\b', 'isRead:true', result, flags=re.IGNORECASE)

        # Date operators
        result = re.sub(r'after:(\S+)', r'received>=\1', result, flags=re.IGNORECASE)
        result = re.sub(r'before:(\S+)', r'received<=\1', result, flags=re.IGNORECASE)

        # Drop unsupported operators
        result = re.sub(r'(in:\S+|label:\S+)', '', result, flags=re.IGNORECASE)

        # Clean up extra whitespace
        result = ' '.join(result.split())
        return result

    @staticmethod
    def extract_bare_text(query: str) -> str:
        """Strip all operator tokens, return free text for local ILIKE search."""
        text = QueryTranslator._OPERATOR_RE.sub('', query)
        return ' '.join(text.split())


# ---------------------------------------------------------------------------
# Gmail provider search
# ---------------------------------------------------------------------------

async def search_gmail(
    connection_data: Dict[str, Any],
    query: str,
    max_results: int = 25,
) -> List[Dict[str, Any]]:
    """
    Search Gmail via API and return normalized email dicts.

    Args:
        connection_data: ext_connections row with tokens
        query: Unified search query
        max_results: Maximum results to return

    Returns:
        List of normalized email dicts with source='remote'
    """
    gmail_query = QueryTranslator.to_gmail(query)
    connection_id = connection_data.get('id', '')

    logger.info(f"[Gmail Search] query='{gmail_query}' max={max_results} conn={connection_id[:8]}...")

    def _list_and_fetch():
        # Build credentials + service inside thread (httplib2 not thread-safe)
        creds = get_valid_credentials(connection_data)
        service = build('gmail', 'v1', credentials=creds)

        # Step 1: list message IDs
        list_result = service.users().messages().list(
            userId='me',
            q=gmail_query,
            maxResults=max_results,
        ).execute()

        message_ids = [m['id'] for m in list_result.get('messages', [])]
        if not message_ids:
            return []

        # Step 2: batch-fetch metadata using Gmail batch API (groups of 50)
        emails = []

        def _batch_callback(request_id, response, exception):
            if exception:
                logger.warning(f"[Gmail Search] Failed to fetch message {request_id}: {exception}")
            else:
                emails.append(response)

        for i in range(0, len(message_ids), 50):
            batch = service.new_batch_http_request(callback=_batch_callback)
            for mid in message_ids[i:i + 50]:
                batch.add(
                    service.users().messages().get(
                        userId='me',
                        id=mid,
                        format='metadata',
                        metadataHeaders=['From', 'To', 'Cc', 'Subject', 'Date'],
                    ),
                    request_id=mid,
                )
            batch.execute()

        return emails

    try:
        raw_messages = await asyncio.to_thread(_list_and_fetch)
    except Exception as e:
        logger.error(f"[Gmail Search] Error: {e}")
        raise

    # Parse into normalized format
    results = []
    for msg in raw_messages:
        headers = parse_email_headers(msg.get('payload', {}).get('headers', []))
        labels = msg.get('labelIds', [])

        results.append({
            'external_id': msg['id'],
            'thread_id': msg.get('threadId'),
            'subject': headers.get('subject', '(No Subject)'),
            'from': headers.get('from', ''),
            'to': headers.get('to', ''),
            'cc': headers.get('cc', ''),
            'snippet': msg.get('snippet', ''),
            'labels': labels,
            'is_unread': 'UNREAD' in labels,
            # METADATA format doesn't reliably indicate attachments;
            # accurate value is set when the full email is fetched via fetch-remote
            'has_attachments': False,
            'received_at': headers.get('date'),
            'ext_connection_id': connection_data.get('id'),
            'account_email': connection_data.get('provider_email'),
            'account_provider': 'google',
            'source': 'remote',
        })

    logger.info(f"[Gmail Search] Found {len(results)} results")
    return results


# ---------------------------------------------------------------------------
# Outlook provider search
# ---------------------------------------------------------------------------

# Fields to request from search results
_OUTLOOK_SEARCH_FIELDS = ",".join([
    "id", "subject", "from", "toRecipients", "ccRecipients",
    "receivedDateTime", "bodyPreview", "hasAttachments",
    "isRead", "conversationId",
])

async def search_outlook(
    connection_data: Dict[str, Any],
    query: str,
    max_results: int = 25,
) -> List[Dict[str, Any]]:
    """
    Search Outlook via Microsoft Graph API $search and return normalized email dicts.

    Args:
        connection_data: ext_connections row with tokens
        query: Unified search query
        max_results: Maximum results to return

    Returns:
        List of normalized email dicts with source='remote'
    """
    kql_query = QueryTranslator.to_kql(query)
    connection_id = connection_data.get('id', '')

    logger.info(f"[Outlook Search] kql='{kql_query}' max={max_results} conn={connection_id[:8]}...")

    supabase = get_service_role_client()
    access_token = get_valid_microsoft_credentials(connection_data, supabase)

    url = "https://graph.microsoft.com/v1.0/me/messages"
    params = {
        "$search": f'"{kql_query}"',
        "$select": _OUTLOOK_SEARCH_FIELDS,
        "$top": str(max_results),
    }
    headers = {
        "Authorization": f"Bearer {access_token}",
        "ConsistencyLevel": "eventual",  # Required for $search
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, params=params, headers=headers)

    if resp.status_code != 200:
        error_msg = resp.text[:200]
        logger.error(f"[Outlook Search] API error {resp.status_code}: {error_msg}")
        raise ValueError(f"Microsoft Graph search failed: {error_msg}")

    data = resp.json()
    messages = data.get("value", [])

    results = []
    for msg in messages:
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

        labels = build_outlook_labels(msg)

        results.append({
            'external_id': msg['id'],
            'thread_id': msg.get('conversationId'),
            'subject': msg.get('subject', '(No Subject)'),
            'from': from_str,
            'to': ', '.join(to_list),
            'cc': ', '.join(cc_list),
            'snippet': msg.get('bodyPreview', ''),
            'labels': labels,
            'is_unread': not msg.get('isRead', True),
            'has_attachments': msg.get('hasAttachments', False),
            'received_at': msg.get('receivedDateTime'),
            'ext_connection_id': connection_data.get('id'),
            'account_email': connection_data.get('provider_email'),
            'account_provider': 'microsoft',
            'source': 'remote',
        })

    logger.info(f"[Outlook Search] Found {len(results)} results")
    return results


# ---------------------------------------------------------------------------
# Local DB search
# ---------------------------------------------------------------------------

async def search_local_db(
    user_id: str,
    user_jwt: str,
    query: str,
    account_ids: Optional[List[str]] = None,
    max_results: int = 25,
) -> List[Dict[str, Any]]:
    """
    Search locally synced emails via the existing fetch_emails() function.

    Uses the bare text from the query (operators stripped) for ILIKE matching.
    """
    bare_text = QueryTranslator.extract_bare_text(query)
    if not bare_text:
        # Operator-only queries (e.g. "from:user@example.com") can't be matched
        # via ILIKE — provider search handles these natively
        return []
    search_term = bare_text

    result = await fetch_emails(
        user_id=user_id,
        user_jwt=user_jwt,
        max_results=max_results,
        query=search_term,
        account_ids=account_ids,
        group_by_thread=False,
    )

    emails = result.get('emails', [])
    for email in emails:
        email['source'] = 'local'

    return emails


# ---------------------------------------------------------------------------
# Merge & deduplicate
# ---------------------------------------------------------------------------

def merge_and_deduplicate(
    local_results: List[Dict[str, Any]],
    remote_results: List[Dict[str, Any]],
    max_results: int = 25,
) -> List[Dict[str, Any]]:
    """
    Merge local and remote results, deduplicate by external_id.
    Local results win on conflict. Sort by received_at descending.
    """
    seen: Dict[str, Dict[str, Any]] = {}

    # Local first — local wins
    for email in local_results:
        eid = email.get('external_id')
        if eid and eid not in seen:
            seen[eid] = email

    # Remote — only add if not already seen
    for email in remote_results:
        eid = email.get('external_id')
        if eid and eid not in seen:
            seen[eid] = email

    merged = list(seen.values())

    # Sort by received_at descending — parse to datetime for consistent ordering
    # across providers (Gmail uses RFC 2822, Outlook uses ISO 8601)
    def _parse_date(email: Dict[str, Any]) -> datetime:
        raw = email.get('received_at')
        if not raw:
            return datetime.min
        # Try ISO 8601 first (Outlook: "2025-02-10T15:30:00Z")
        for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S%z'):
            try:
                return datetime.strptime(raw, fmt)
            except (ValueError, TypeError):
                continue
        # Fall back to RFC 2822 (Gmail: "Mon, 10 Feb 2025 15:30:00 +0000")
        try:
            return parsedate_to_datetime(raw)
        except (ValueError, TypeError):
            return datetime.min

    merged.sort(key=_parse_date, reverse=True)

    return merged[:max_results]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def search_emails_with_providers(
    user_id: str,
    user_jwt: str,
    query: str,
    account_ids: Optional[List[str]] = None,
    provider_search: bool = True,
    max_results: int = 25,
) -> Dict[str, Any]:
    """
    Orchestrate local + provider-side email search.

    Fetches from local DB and each active provider account in parallel,
    merges and deduplicates results.

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT
        query: Search query string
        account_ids: Optional filter to specific accounts
        provider_search: Whether to search provider APIs (default True)
        max_results: Maximum results to return

    Returns:
        Dict with emails, counts, and provider_errors
    """
    logger.info(f"[Search] user={user_id[:8]}... query='{query}' provider_search={provider_search}")

    # Always search local DB
    tasks = [search_local_db(user_id, user_jwt, query, account_ids, max_results)]

    # Optionally search provider APIs
    provider_connections = []
    if provider_search:
        try:
            auth_supabase = await get_authenticated_async_client(user_jwt)
            conn_query = auth_supabase.table('ext_connections')\
                .select('id, provider, provider_email, access_token, refresh_token, token_expires_at, metadata')\
                .eq('user_id', user_id)\
                .eq('is_active', True)

            if account_ids:
                conn_query = conn_query.in_('id', account_ids)

            conn_result = await conn_query.execute()
            provider_connections = [decrypt_ext_connection_tokens(c) for c in (conn_result.data or [])]
        except Exception as e:
            logger.warning(f"[Search] Failed to fetch connections: {e}")

        for conn in provider_connections:
            conn['user_id'] = user_id
            if conn['provider'] == 'google':
                tasks.append(search_gmail(conn, query, max_results))
            elif conn['provider'] == 'microsoft':
                tasks.append(search_outlook(conn, query, max_results))

    # Run all searches in parallel
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # First result is always local
    local_result = results[0]
    local_emails = local_result if isinstance(local_result, list) else []
    if isinstance(local_result, Exception):
        logger.error(f"[Search] Local search failed: {local_result}")
        local_emails = []

    # Remaining results are from providers
    all_remote_emails = []
    provider_errors = {}

    for i, conn in enumerate(provider_connections):
        result_idx = i + 1  # offset by 1 because local is at index 0
        result = results[result_idx]
        if isinstance(result, Exception):
            account_label = conn.get('provider_email', conn.get('id', 'unknown'))
            provider_errors[account_label] = str(result)
            logger.warning(f"[Search] Provider search failed for {account_label}: {result}")
        elif isinstance(result, list):
            all_remote_emails.extend(result)

    # Merge and deduplicate
    merged = merge_and_deduplicate(local_emails, all_remote_emails, max_results)

    local_count = sum(1 for e in merged if e.get('source') == 'local')
    remote_count = sum(1 for e in merged if e.get('source') == 'remote')

    logger.info(f"[Search] Done: {len(merged)} total ({local_count} local, {remote_count} remote)")

    return {
        "emails": merged,
        "count": len(merged),
        "local_count": local_count,
        "remote_count": remote_count,
        "query": query,
        "provider_errors": provider_errors if provider_errors else None,
        "has_provider_errors": bool(provider_errors),
    }
