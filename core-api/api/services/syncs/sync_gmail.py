"""
Gmail sync service - Sync emails from Gmail to database
"""
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta
from lib.supabase_client import get_authenticated_supabase_client
from lib.batch_utils import batch_upsert, get_existing_external_ids
import logging
from googleapiclient.errors import HttpError
from api.services.email.google_api_helpers import (
    get_gmail_service,
    parse_email_headers,
    decode_email_body,
    get_attachment_info,
    list_active_gmail_drafts_by_message_id,
)
from api.services.email.draft_cleanup import cleanup_inactive_draft_rows_for_connection
from lib.google_retry import GOOGLE_API_NUM_RETRIES
# Note: normalize_labels no longer needed - normalized_labels is now a generated column
# Note: AI analysis is now deferred to cron to avoid Groq rate limits

logger = logging.getLogger(__name__)


def _preserve_user_read_status(
    emails_data: List[Dict[str, Any]],
    existing_ids: set
) -> None:
    """
    Remove is_read from existing emails to preserve user's read status.

    When syncing emails, we don't want to overwrite the user's local read/unread
    state with the provider's state. New emails keep is_read from the provider
    (correct initial state), but existing emails preserve whatever the user set.

    Args:
        emails_data: List of email dicts to modify in-place
        existing_ids: Set of external_ids that already exist in the database
    """
    for email_data in emails_data:
        if email_data.get('external_id') in existing_ids:
            email_data.pop('is_read', None)


def _get_active_draft_map(service: Any) -> Optional[Dict[str, str]]:
    """Best-effort lookup of active Gmail drafts for this account."""
    try:
        return list_active_gmail_drafts_by_message_id(service)
    except Exception as e:
        logger.warning(f"Could not fetch active Gmail drafts map: {e}")
        return None


def _reconcile_inactive_drafts(
    supabase_client: Any,
    user_id: str,
    connection_id: str,
    active_draft_map: Optional[Dict[str, str]],
) -> int:
    """Delete local draft revisions that are no longer active in Gmail."""
    # Distinguish "no active drafts" ({}) from "draft lookup failed" (None).
    if active_draft_map is None:
        logger.warning("Skipping draft reconciliation because active draft map is unavailable")
        return 0

    try:
        return cleanup_inactive_draft_rows_for_connection(
            supabase_client=supabase_client,
            user_id=user_id,
            ext_connection_id=connection_id,
            active_external_ids=active_draft_map.keys(),
        )
    except Exception as e:
        logger.warning(f"Draft reconciliation failed (non-fatal): {e}")
        return 0


def sync_gmail(
    user_id: str,
    user_jwt: str,
    max_results: int = 100,
    sync_since: Optional[str] = None
) -> Dict[str, Any]:
    """
    Sync emails from Gmail to database

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        max_results: Maximum number of emails to sync (default 100)
        sync_since: Optional date to sync from (ISO format). If not provided, syncs last 20 days

    Returns:
        Dict with sync results
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get Gmail service
    service, connection_id = get_gmail_service(user_id, user_jwt)

    if not service or not connection_id:
        raise ValueError("No active Google connection found for user. Please sign in with Google first.")

    try:
        active_draft_map = _get_active_draft_map(service)

        # Build query for recent emails
        if sync_since:
            # Use provided sync date
            query = f"after:{sync_since}"
        else:
            # Default to last 20 days
            since_date = (datetime.now(timezone.utc) - timedelta(days=20)).strftime('%Y/%m/%d')
            query = f"after:{since_date}"

        logger.info(f"🔄 Starting Gmail sync for user {user_id} with query: {query}")

        # Fetch message list
        messages_result = service.users().messages().list(
            userId='me',
            maxResults=max_results,
            q=query
        ).execute(num_retries=GOOGLE_API_NUM_RETRIES)

        messages = messages_result.get('messages', [])

        if not messages:
            logger.info(f"ℹ️ No new emails to sync for user {user_id}")
            return {
                "message": "No new emails to sync",
                "status": "completed",
                "user_id": user_id,
                "new_emails": 0,
                "updated_emails": 0,
                "total_emails": 0
            }

        logger.info(f"📧 Found {len(messages)} messages to sync")

        # Parse all messages first
        all_emails_data: List[Dict[str, Any]] = []
        all_external_ids: List[str] = []
        error_count = 0

        for msg in messages:
            try:
                # Get full message details
                full_msg = service.users().messages().get(
                    userId='me',
                    id=msg['id'],
                    format='full'
                ).execute(num_retries=GOOGLE_API_NUM_RETRIES)

                email_data = _parse_email_message(
                    full_msg,
                    user_id,
                    connection_id,
                    draft_message_to_draft_id=active_draft_map,
                )
                if not email_data:
                    continue
                all_emails_data.append(email_data)
                all_external_ids.append(email_data['external_id'])

            except HttpError as e:
                logger.error(f"❌ Error syncing message {msg['id']}: {str(e)}")
                error_count += 1
                continue
            except Exception as e:
                logger.error(f"❌ Unexpected error syncing message {msg['id']}: {str(e)}")
                error_count += 1
                continue

        # Get existing IDs to calculate new vs updated counts
        existing_ids = get_existing_external_ids(
            auth_supabase, 'emails', user_id, all_external_ids
        )
        synced_count = len([eid for eid in all_external_ids if eid not in existing_ids])
        updated_count = len(all_external_ids) - synced_count

        _preserve_user_read_status(all_emails_data, existing_ids)

        # Batch upsert all emails
        # Note: AI analysis is deferred to /api/cron/analyze-emails to avoid rate limits
        batch_had_errors = False
        if all_emails_data:
            logger.info(f"📤 Batch upserting {len(all_emails_data)} emails...")
            result = batch_upsert(
                auth_supabase,
                'emails',
                all_emails_data,
                'user_id,external_id'
            )
            if result['errors']:
                logger.warning(f"⚠️ Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True
                error_count += result['error_count']

        _reconcile_inactive_drafts(
            supabase_client=auth_supabase,
            user_id=user_id,
            connection_id=connection_id,
            active_draft_map=active_draft_map,
        )

        # Update last synced timestamp only if no errors occurred
        if not batch_had_errors and error_count == 0:
            auth_supabase.table('ext_connections')\
                .update({'last_synced': datetime.now(timezone.utc).isoformat()})\
                .eq('id', connection_id)\
                .execute()
        else:
            logger.warning(f"⚠️ Skipping last_synced update due to {error_count} errors")

        total_synced = synced_count + updated_count

        logger.info(f"✅ Gmail sync completed for user {user_id}: {synced_count} new, {updated_count} updated, {error_count} errors")

        return {
            "message": "Gmail sync completed successfully",
            "status": "completed",
            "user_id": user_id,
            "new_emails": synced_count,
            "updated_emails": updated_count,
            "total_emails": total_synced,
            "errors": error_count
        }

    except HttpError as e:
        logger.error(f"❌ Gmail API error during sync: {str(e)}")
        raise ValueError(f"Failed to sync Gmail: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Error syncing Gmail: {str(e)}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise ValueError(f"Gmail sync failed: {str(e)}")


def sync_gmail_incremental(
    user_id: str,
    user_jwt: str
) -> Dict[str, Any]:
    """
    Perform incremental Gmail sync based on last sync time
    Only syncs emails since the last successful sync

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests

    Returns:
        Dict with sync results
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    try:
        # Get last sync time from connection (primary first for multi-account support)
        connection_result = auth_supabase.table('ext_connections')\
            .select('id, last_synced')\
            .eq('user_id', user_id)\
            .eq('provider', 'google')\
            .eq('is_active', True)\
            .order('is_primary', desc=True)\
            .order('created_at', desc=True)\
            .limit(1)\
            .execute()

        if not connection_result.data or len(connection_result.data) == 0:
            raise ValueError("No active Google connection found")

        last_synced = connection_result.data[0].get('last_synced')

        # Determine sync date
        if last_synced:
            # Parse last sync date and subtract 1 hour buffer for safety
            last_sync_dt = datetime.fromisoformat(last_synced.replace('Z', '+00:00'))
            sync_since_dt = last_sync_dt - timedelta(hours=1)
            sync_since = sync_since_dt.strftime('%Y/%m/%d')
        else:
            # First sync - get last 20 days
            sync_since_dt = datetime.now(timezone.utc) - timedelta(days=20)
            sync_since = sync_since_dt.strftime('%Y/%m/%d')

        logger.info(f"🔄 Performing incremental sync since {sync_since}")

        return sync_gmail(
            user_id=user_id,
            user_jwt=user_jwt,
            max_results=200,
            sync_since=sync_since
        )

    except Exception as e:
        logger.error(f"❌ Error in incremental sync: {str(e)}")
        raise ValueError(f"Incremental sync failed: {str(e)}")


def sync_gmail_full(
    user_id: str,
    user_jwt: str,
    days_back: int = 20
) -> Dict[str, Any]:
    """
    Perform full Gmail sync for a specified number of days

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        days_back: Number of days to sync back (default 20)

    Returns:
        Dict with sync results
    """
    since_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime('%Y/%m/%d')

    logger.info(f"🔄 Performing full sync for {days_back} days (since {since_date})")

    return sync_gmail(
        user_id=user_id,
        user_jwt=user_jwt,
        max_results=500,
        sync_since=since_date
    )


def process_gmail_history(
    user_id: str,
    user_jwt: str,
    start_history_id: str
) -> Dict[str, Any]:
    """
    Process Gmail changes using the history API (much more efficient than full sync)
    This is called when we receive a push notification from Google

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        start_history_id: The historyId to start fetching changes from

    Returns:
        Dict with sync results
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get Gmail service
    service, connection_id = get_gmail_service(user_id, user_jwt)

    if not service or not connection_id:
        raise ValueError("No active Google connection found for user")

    try:
        logger.info(f"📜 Processing Gmail history for user {user_id} from historyId {start_history_id}")
        active_draft_map = _get_active_draft_map(service)

        # Fetch history changes
        history_result = service.users().history().list(
            userId='me',
            startHistoryId=start_history_id,
            historyTypes=['messageAdded', 'messageDeleted', 'labelAdded', 'labelRemoved']
        ).execute()

        history_records = history_result.get('history', [])
        new_history_id = history_result.get('historyId', start_history_id)

        if not history_records:
            logger.info(f"ℹ️ No history changes found for user {user_id}")
            return {
                "message": "No changes to sync",
                "status": "completed",
                "user_id": user_id,
                "new_emails": 0,
                "updated_emails": 0,
                "deleted_emails": 0,
                "new_history_id": new_history_id
            }

        logger.info(f"📊 Found {len(history_records)} history records")

        # Collect all messages to add/update
        messages_to_add: List[Dict[str, Any]] = []
        messages_to_delete: List[str] = []
        label_changes: List[Dict[str, Any]] = []

        # Process each history record
        for record in history_records:
            # Handle messages added
            if 'messagesAdded' in record:
                for msg_added in record['messagesAdded']:
                    message = msg_added.get('message', {})
                    if message.get('id'):
                        messages_to_add.append(message)

            # Handle messages deleted
            if 'messagesDeleted' in record:
                for msg_deleted in record['messagesDeleted']:
                    message = msg_deleted.get('message', {})
                    if message.get('id'):
                        messages_to_delete.append(message['id'])

            # Handle label changes
            if 'labelsAdded' in record:
                for label_change in record['labelsAdded']:
                    label_changes.append({
                        'type': 'add',
                        'message_id': label_change.get('message', {}).get('id'),
                        'labels': label_change.get('labelIds', [])
                    })

            if 'labelsRemoved' in record:
                for label_change in record['labelsRemoved']:
                    label_changes.append({
                        'type': 'remove',
                        'message_id': label_change.get('message', {}).get('id'),
                        'labels': label_change.get('labelIds', [])
                    })

        # Fetch and parse all added messages
        all_emails_data: List[Dict[str, Any]] = []
        all_external_ids: List[str] = []

        for msg in messages_to_add:
            try:
                full_msg = service.users().messages().get(
                    userId='me',
                    id=msg['id'],
                    format='full'
                ).execute(num_retries=GOOGLE_API_NUM_RETRIES)

                email_data = _parse_email_message(
                    full_msg,
                    user_id,
                    connection_id,
                    draft_message_to_draft_id=active_draft_map,
                )
                if not email_data:
                    continue
                all_emails_data.append(email_data)
                all_external_ids.append(email_data['external_id'])

            except Exception as e:
                logger.error(f"❌ Error processing added message: {str(e)}")
                continue

        # Get existing IDs to calculate counts
        existing_ids = get_existing_external_ids(
            auth_supabase, 'emails', user_id, all_external_ids
        ) if all_external_ids else set()
        added_count = len([eid for eid in all_external_ids if eid not in existing_ids])
        updated_count = len(all_external_ids) - added_count

        _preserve_user_read_status(all_emails_data, existing_ids)

        # Batch upsert all emails
        batch_had_errors = False
        if all_emails_data:
            logger.info(f"📤 Batch upserting {len(all_emails_data)} emails from history...")
            result = batch_upsert(
                auth_supabase,
                'emails',
                all_emails_data,
                'user_id,external_id'
            )
            if result['errors']:
                logger.warning(f"⚠️ Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True

        _reconcile_inactive_drafts(
            supabase_client=auth_supabase,
            user_id=user_id,
            connection_id=connection_id,
            active_draft_map=active_draft_map,
        )

        # Handle deleted messages
        deleted_count = 0
        delete_errors = 0
        for message_id in messages_to_delete:
            try:
                result = auth_supabase.table('emails')\
                    .delete()\
                    .eq('user_id', user_id)\
                    .eq('external_id', message_id)\
                    .execute()

                if result.data:
                    deleted_count += 1

            except Exception as e:
                logger.error(f"❌ Error processing deleted message: {str(e)}")
                delete_errors += 1
                continue

        # Handle label changes
        label_errors = 0
        for change in label_changes:
            try:
                message_id = change['message_id']
                if not message_id:
                    continue

                # Get current email
                existing = auth_supabase.table('emails')\
                    .select('labels')\
                    .eq('user_id', user_id)\
                    .eq('external_id', message_id)\
                    .execute()

                if existing.data:
                    current_labels = existing.data[0].get('labels', [])

                    if change['type'] == 'add':
                        new_labels = list(set(current_labels + change['labels']))
                    else:
                        new_labels = [label for label in current_labels if label not in change['labels']]

                    # Update read/starred status based on labels
                    is_read = 'UNREAD' not in new_labels
                    is_starred = 'STARRED' in new_labels

                    auth_supabase.table('emails')\
                        .update({
                            'labels': new_labels,
                            # normalized_labels auto-computed by generated column
                            'is_read': is_read,
                            'is_starred': is_starred
                        })\
                        .eq('user_id', user_id)\
                        .eq('external_id', message_id)\
                        .execute()
                    updated_count += 1

            except Exception as e:
                logger.error(f"❌ Error processing label changes: {str(e)}")
                label_errors += 1
                continue

        # Update last synced timestamp and history ID only if no errors occurred
        had_errors = batch_had_errors or delete_errors > 0 or label_errors > 0
        if not had_errors:
            auth_supabase.table('ext_connections')\
                .update({'last_synced': datetime.now(timezone.utc).isoformat()})\
                .eq('id', connection_id)\
                .execute()

            # Update history ID in push subscription
            auth_supabase.table('push_subscriptions')\
                .update({
                    'history_id': new_history_id,
                    'last_notification_at': datetime.now(timezone.utc).isoformat()
                })\
                .eq('user_id', user_id)\
                .eq('provider', 'gmail')\
                .eq('is_active', True)\
                .execute()
        else:
            logger.warning(f"⚠️ Skipping last_synced/history_id update due to errors (batch={batch_had_errors}, delete={delete_errors}, label={label_errors})")

        logger.info(f"✅ History sync completed: {added_count} added, {updated_count} updated, {deleted_count} deleted")

        return {
            "message": "History sync completed successfully",
            "status": "completed",
            "user_id": user_id,
            "new_emails": added_count,
            "updated_emails": updated_count,
            "deleted_emails": deleted_count,
            "new_history_id": new_history_id
        }

    except HttpError as e:
        logger.error(f"❌ Gmail API error during history sync: {str(e)}")
        raise ValueError(f"Failed to sync Gmail history: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Error syncing Gmail history: {str(e)}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise ValueError(f"Gmail history sync failed: {str(e)}")


def sync_gmail_for_connection(
    gmail_service,
    user_id: str,
    connection_id: str,
    supabase_client,
    max_results: int = 50,
    days_back: int = 20
) -> Dict[str, Any]:
    """
    Sync emails for a specific connection (used for initial sync of secondary accounts).

    Args:
        gmail_service: Authenticated Gmail API service instance
        user_id: User's ID
        connection_id: The ext_connection_id
        supabase_client: Authenticated or service role Supabase client
        max_results: Maximum number of emails to sync (default 50 for initial sync)
        days_back: Number of days to sync (default 20)

    Returns:
        Dict with sync results
    """
    try:
        active_draft_map = _get_active_draft_map(gmail_service)

        # Build query for recent emails
        since_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime('%Y/%m/%d')
        query = f"after:{since_date}"

        logger.info(f"🔄 Starting initial Gmail sync for connection {connection_id[:8]}... with query: {query}")

        # Fetch message list
        messages_result = gmail_service.users().messages().list(
            userId='me',
            maxResults=max_results,
            q=query
        ).execute()

        messages = messages_result.get('messages', [])

        if not messages:
            logger.info(f"ℹ️ No emails to sync for connection {connection_id[:8]}...")
            return {
                "success": True,
                "message": "No emails to sync",
                "new_emails": 0,
                "updated_emails": 0
            }

        logger.info(f"📧 Found {len(messages)} messages to sync for initial sync")

        # Parse all messages first
        all_emails_data: List[Dict[str, Any]] = []
        all_external_ids: List[str] = []
        error_count = 0

        for msg in messages:
            try:
                # Get full message details
                full_msg = gmail_service.users().messages().get(
                    userId='me',
                    id=msg['id'],
                    format='full'
                ).execute()

                email_data = _parse_email_message(
                    full_msg,
                    user_id,
                    connection_id,
                    draft_message_to_draft_id=active_draft_map,
                )
                if not email_data:
                    continue
                all_emails_data.append(email_data)
                all_external_ids.append(email_data['external_id'])

            except HttpError as e:
                logger.error(f"❌ Error syncing message {msg['id']}: {str(e)}")
                error_count += 1
                continue
            except Exception as e:
                logger.error(f"❌ Unexpected error syncing message {msg['id']}: {str(e)}")
                error_count += 1
                continue

        # Get existing IDs to calculate new vs updated counts
        existing_ids = get_existing_external_ids(
            supabase_client, 'emails', user_id, all_external_ids
        )
        synced_count = len([eid for eid in all_external_ids if eid not in existing_ids])
        updated_count = len(all_external_ids) - synced_count

        _preserve_user_read_status(all_emails_data, existing_ids)

        # Batch upsert all emails
        # Note: AI analysis is skipped for initial sync - handled by cron
        batch_had_errors = False
        if all_emails_data:
            logger.info(f"📤 Batch upserting {len(all_emails_data)} emails...")
            result = batch_upsert(
                supabase_client,
                'emails',
                all_emails_data,
                'user_id,external_id'
            )
            if result['errors']:
                logger.warning(f"⚠️ Some batch errors: {result['errors'][:3]}")
                batch_had_errors = True
                error_count += result['error_count']

        _reconcile_inactive_drafts(
            supabase_client=supabase_client,
            user_id=user_id,
            connection_id=connection_id,
            active_draft_map=active_draft_map,
        )

        # Update last synced timestamp only if no errors occurred
        if not batch_had_errors and error_count == 0:
            supabase_client.table('ext_connections')\
                .update({'last_synced': datetime.now(timezone.utc).isoformat()})\
                .eq('id', connection_id)\
                .execute()
        else:
            logger.warning(f"⚠️ Skipping last_synced update due to {error_count} errors")

        logger.info(f"✅ Initial Gmail sync completed for connection {connection_id[:8]}...: {synced_count} new, {updated_count} updated, {error_count} errors")

        return {
            "success": True,
            "message": "Initial sync completed",
            "new_emails": synced_count,
            "updated_emails": updated_count,
            "errors": error_count
        }

    except HttpError as e:
        logger.error(f"❌ Gmail API error during initial sync: {str(e)}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"❌ Error during initial sync: {str(e)}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}


def _parse_email_message(
    full_msg: Dict[str, Any],
    user_id: str,
    connection_id: str,
    draft_message_to_draft_id: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """Parse Gmail message into database format."""
    # Parse headers
    headers = parse_email_headers(full_msg.get('payload', {}).get('headers', []))

    # Decode body
    body = decode_email_body(full_msg.get('payload', {}))

    # Get metadata
    message_id = full_msg.get('id')
    thread_id = full_msg.get('threadId')
    snippet = full_msg.get('snippet', '')
    labels = full_msg.get('labelIds', [])
    internal_date = full_msg.get('internalDate')

    # Convert internal date
    if internal_date:
        received_at = datetime.fromtimestamp(
            int(internal_date) / 1000,
            tz=timezone.utc
        ).isoformat()
    else:
        received_at = None

    # Check various flags
    is_unread = 'UNREAD' in labels
    is_starred = 'STARRED' in labels
    is_draft = 'DRAFT' in labels
    gmail_draft_id = draft_message_to_draft_id.get(message_id) if (is_draft and draft_message_to_draft_id) else None

    # Ignore stale draft revisions that are no longer active in Gmail.
    if is_draft and draft_message_to_draft_id is not None and not gmail_draft_id:
        logger.debug(f"Skipping stale Gmail draft revision {message_id}")
        return None

    # Get attachments
    attachments = get_attachment_info(full_msg.get('payload', {}))

    # Parse addresses into arrays
    to_addresses = [addr.strip() for addr in headers.get('to', '').split(',')] if headers.get('to') else []
    cc_addresses = [addr.strip() for addr in headers.get('cc', '').split(',')] if headers.get('cc') else []

    # Use HTML body if available, otherwise fallback to plain text
    body_content = body.get('html') or body.get('plain', '')

    return {
        'user_id': user_id,
        'ext_connection_id': connection_id,
        'external_id': message_id,
        'thread_id': thread_id,
        'subject': headers.get('subject', '(No Subject)'),
        'from': headers.get('from', ''),
        'to': to_addresses,
        'cc': cc_addresses if cc_addresses else None,
        'body': body_content,
        'snippet': snippet,
        'labels': labels,
        # normalized_labels is auto-computed by PostgreSQL generated column
        'provider_ids': {'label_ids': labels},
        'is_read': not is_unread,
        'is_starred': is_starred,
        'is_draft': is_draft,
        'gmail_draft_id': gmail_draft_id,
        'received_at': received_at,
        'has_attachments': len(attachments) > 0,
        'attachments': attachments,
        'synced_at': datetime.now(timezone.utc).isoformat(),
        'raw_item': full_msg  # Store full lossless Gmail message
    }
