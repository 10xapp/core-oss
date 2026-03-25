"""
Gmail sync for cron jobs - bypasses RLS using service role
"""
from typing import Dict, Any, List
from datetime import datetime, timezone, timedelta
import logging
from googleapiclient.errors import HttpError

from lib.batch_utils import batch_upsert, get_existing_external_ids
from api.services.syncs.connection_state import (
    batch_has_orphaned_user_error,
    deactivate_connection_with_subscriptions,
)
from api.services.syncs.google_error_utils import is_permanent_google_api_error

logger = logging.getLogger(__name__)


def sync_gmail_cron(
    gmail_service,
    connection_id: str,
    user_id: str,
    service_supabase,
    days_back: int = 7
) -> Dict[str, Any]:
    """
    Sync Gmail emails for cron jobs.
    Uses service role Supabase client to bypass RLS.

    Args:
        gmail_service: Gmail API service
        connection_id: External connection ID
        user_id: User's ID
        service_supabase: Service role Supabase client (bypasses RLS)
        days_back: Number of days back to sync (default 7)

    Returns:
        Dict with sync results
    """
    from api.services.email.google_api_helpers import (
        parse_email_headers,
        decode_email_body,
        get_attachment_info
    )

    synced_count = 0
    updated_count = 0
    error_count = 0
    total_processed = 0
    connection_deactivated = False

    try:
        # Get last sync time from connection
        connection = service_supabase.table('ext_connections')\
            .select('last_synced')\
            .eq('id', connection_id)\
            .single()\
            .execute()

        last_synced = connection.data.get('last_synced') if connection.data else None

        # Determine sync date
        if last_synced:
            # Parse last sync date and subtract 1 hour buffer for safety
            last_sync_dt = datetime.fromisoformat(last_synced.replace('Z', '+00:00'))
            sync_since_dt = last_sync_dt - timedelta(hours=1)
            sync_since = sync_since_dt.strftime('%Y/%m/%d')
        else:
            # First sync - get last N days
            sync_since_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
            sync_since = sync_since_dt.strftime('%Y/%m/%d')

        query = f"after:{sync_since}"
        logger.info(f"📧 Gmail query: {query}")

        page_token = None
        all_emails_data: List[Dict[str, Any]] = []
        all_external_ids: List[str] = []

        # Handle pagination - fetch all messages first
        while True:
            # Fetch message list
            messages_result = gmail_service.users().messages().list(
                userId='me',
                maxResults=100,
                q=query,
                pageToken=page_token
            ).execute()

            messages = messages_result.get('messages', [])

            if not messages:
                break

            total_processed += len(messages)
            logger.info(f"📦 Processing {len(messages)} messages (total: {total_processed})")

            for msg in messages:
                try:
                    # Get full message details
                    full_msg = gmail_service.users().messages().get(
                        userId='me',
                        id=msg['id'],
                        format='full'
                    ).execute()

                    email_data = _parse_email_message(
                        full_msg, user_id, connection_id,
                        parse_email_headers, decode_email_body, get_attachment_info
                    )
                    all_emails_data.append(email_data)
                    all_external_ids.append(email_data['external_id'])

                except Exception as e:
                    logger.error(f"❌ Error processing message {msg.get('id')}: {str(e)}")
                    error_count += 1
                    continue

            # Check if there are more pages
            page_token = messages_result.get('nextPageToken')
            if not page_token:
                break

            # Safety limit: stop after processing 500 messages in one cron run
            if total_processed >= 500:
                logger.warning("⚠️ Reached 500 message limit, stopping pagination")
                break

        # Get existing IDs to calculate new vs updated counts
        if all_external_ids:
            existing_ids = get_existing_external_ids(
                service_supabase, 'emails', user_id, all_external_ids
            )
            synced_count = len([eid for eid in all_external_ids if eid not in existing_ids])
            updated_count = len(all_external_ids) - synced_count

        # Batch upsert all emails
        # Note: AI analysis is skipped here - handled by /api/cron/analyze-emails hourly
        batch_had_errors = False
        if all_emails_data:
            logger.info(f"📤 Batch upserting {len(all_emails_data)} emails...")
            result = batch_upsert(
                service_supabase,
                'emails',
                all_emails_data,
                'user_id,external_id'
            )
            if result['errors']:
                logger.warning(f"⚠️ Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True
                error_count += result['error_count']
                if batch_has_orphaned_user_error(result['errors']):
                    deactivate_connection_with_subscriptions(
                        service_supabase,
                        connection_id,
                        reason="orphaned user detected during Gmail sync",
                    )
                    connection_deactivated = True

        # Update last synced timestamp only if no errors occurred
        if not batch_had_errors and error_count == 0:
            service_supabase.table('ext_connections')\
                .update({'last_synced': datetime.now(timezone.utc).isoformat()})\
                .eq('id', connection_id)\
                .execute()
        else:
            logger.warning(f"⚠️ Skipping last_synced update due to {error_count} errors")

        if connection_deactivated:
            return {
                "status": "quarantined",
                "message": "Connection deactivated because its user no longer exists",
                "new_emails": synced_count,
                "updated_emails": updated_count,
                "error_count": error_count,
                "total_processed": total_processed,
            }

        logger.info(f"✅ Gmail sync complete: {synced_count} new, {updated_count} updated, {error_count} errors")

        return {
            "status": "success",
            "new_emails": synced_count,
            "updated_emails": updated_count,
            "total_emails": synced_count + updated_count,
            "error_count": error_count,
            "total_processed": total_processed
        }

    except HttpError as e:
        if is_permanent_google_api_error(e):
            logger.warning(f"⚠️ Gmail API permanently unavailable for connection {connection_id[:8]}...: {str(e)}")
        else:
            logger.error(f"❌ Gmail API error: {str(e)}")
        return {
            "status": "error",
            "error": f"Gmail API error: {str(e)}",
            "new_emails": synced_count,
            "updated_emails": updated_count
        }
    except Exception as e:
        logger.error(f"❌ Error syncing Gmail: {str(e)}")
        logger.exception("Full traceback:")
        return {
            "status": "error",
            "error": str(e),
            "new_emails": synced_count,
            "updated_emails": updated_count
        }


def _parse_email_message(
    full_msg: Dict[str, Any],
    user_id: str,
    connection_id: str,
    parse_email_headers,
    decode_email_body,
    get_attachment_info
) -> Dict[str, Any]:
    """Parse Gmail message into database format."""
    # Parse headers
    headers = parse_email_headers(full_msg.get('payload', {}).get('headers', []))

    # Decode body
    body = decode_email_body(full_msg.get('payload', {}))

    # Get metadata
    message_id = full_msg.get('id')
    thread_id = full_msg.get('threadId')
    labels = full_msg.get('labelIds', [])
    internal_date = full_msg.get('internalDate')

    # Convert internal date to ISO format (don't inject fake timestamps)
    received_at = None
    if internal_date:
        received_at = datetime.fromtimestamp(
            int(internal_date) / 1000,
            tz=timezone.utc
        ).isoformat()

    # Get attachments info
    attachments = get_attachment_info(full_msg.get('payload', {}))

    # Parse to/cc/bcc into arrays
    to_addrs = [addr.strip() for addr in headers.get('to', '').split(',')] if headers.get('to') else []
    cc_addrs = [addr.strip() for addr in headers.get('cc', '').split(',')] if headers.get('cc') else []
    bcc_addrs = [addr.strip() for addr in headers.get('bcc', '').split(',')] if headers.get('bcc') else []

    # Use HTML body if available, otherwise fallback to plain text (consistent with sync_gmail.py)
    body_content = body.get('html') or body.get('plain', '')

    # Check flags
    is_draft = 'DRAFT' in labels
    is_trashed = 'TRASH' in labels

    return {
        'user_id': user_id,
        'ext_connection_id': connection_id,
        'external_id': message_id,
        'thread_id': thread_id,
        'subject': headers.get('subject', '(No subject)'),
        'from': headers.get('from', ''),
        'to': to_addrs,
        'cc': cc_addrs if cc_addrs else None,
        'bcc': bcc_addrs if bcc_addrs else None,
        'body': body_content,
        'received_at': received_at,
        'labels': labels,
        'is_read': 'UNREAD' not in labels,
        'is_starred': 'STARRED' in labels,
        'is_draft': is_draft,
        'is_trashed': is_trashed,
        'has_attachments': len(attachments) > 0,
        'attachments': attachments if attachments else None,
        'synced_at': datetime.now(timezone.utc).isoformat(),
        'raw_item': full_msg  # Store full Gmail message
    }
