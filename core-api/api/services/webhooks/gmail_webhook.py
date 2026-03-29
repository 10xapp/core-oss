"""
Gmail Webhook Service - Processes Gmail push notifications

Handles real-time sync when Gmail sends push notifications via Google Pub/Sub.
Includes proper token refresh, historyId recovery, and batch message fetching.
"""
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from googleapiclient.errors import HttpError

from lib.supabase_client import get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens
from api.services.google_auth import (
    get_gmail_service_for_webhook,
    get_current_gmail_history_id,
    GoogleAuthError
)
from api.services.email.google_api_helpers import (
    parse_email_headers,
    decode_email_body,
    get_attachment_info,
    list_active_gmail_drafts_by_message_id,
)
from api.services.email.draft_cleanup import cleanup_inactive_draft_rows_for_connection
from api.services.email.analyze_email_ai import analyze_email_with_ai

logger = logging.getLogger(__name__)

# Maximum messages to fetch in fallback mode
FALLBACK_MAX_MESSAGES = 20

# Maximum messages to process per batch request
# Keep small to avoid Gmail's concurrent request limits (429 errors)
BATCH_SIZE = 10

# Fallback query window (hours) - limits how far back we sync on fallback
FALLBACK_WINDOW_HOURS = 24


def process_gmail_notification(
    email_address: str,
    history_id: str
) -> Dict[str, Any]:
    """
    Process a Gmail push notification and sync new emails.

    Args:
        email_address: The Gmail address that received changes
        history_id: The new historyId from the notification

    Returns:
        Dict with processing results
    """
    logger.info(f"📧 Processing Gmail notification for {email_address}, historyId: {history_id}")

    supabase = get_service_role_client()

    # Find active subscription for this email address
    # Uses provider_email in ext_connections to match the correct user (#7 fix)
    subscription = supabase.table('push_subscriptions')\
        .select(
            '*, ext_connections!push_subscriptions_ext_connection_id_fkey!inner('
            'user_id, access_token, refresh_token, token_expires_at, metadata, provider_email'
            ')'
        )\
        .eq('provider', 'gmail')\
        .eq('is_active', True)\
        .eq('ext_connections.provider_email', email_address)\
        .execute()

    if not subscription.data:
        logger.warning(f"⚠️ No active Gmail subscription found for {email_address}")
        return {"status": "ok", "message": f"No active subscription for {email_address}"}

    # Handle multiple subscriptions for same email (shouldn't happen, but be safe)
    if len(subscription.data) > 1:
        logger.warning(f"⚠️ Multiple subscriptions found for {email_address}, using first")

    sub_data = subscription.data[0]
    if sub_data.get('ext_connections'):
        sub_data['ext_connections'] = decrypt_ext_connection_tokens(sub_data['ext_connections'])
    user_id = sub_data['ext_connections']['user_id']
    old_history_id = sub_data.get('history_id')
    subscription_id = sub_data['id']
    connection_id = sub_data['ext_connection_id']

    logger.info(f"🔄 Processing Gmail changes for user {user_id}")
    logger.info(f"   Old historyId: {old_history_id}, New historyId: {history_id}")

    # Update notification count
    notification_count = sub_data.get('notification_count', 0) + 1
    supabase.table('push_subscriptions')\
        .update({
            'notification_count': notification_count,
            'last_notification_at': datetime.now(timezone.utc).isoformat()
        })\
        .eq('id', subscription_id)\
        .execute()

    # Get valid credentials with automatic refresh
    try:
        gmail_service, _ = get_gmail_service_for_webhook(sub_data, supabase)
    except GoogleAuthError as e:
        logger.error(f"❌ Authentication failed for user {user_id}: {str(e)}")
        return {"status": "error", "message": f"Auth failed: {str(e)}"}

    # Process based on history state
    if old_history_id and old_history_id != history_id:
        # Normal case: we have a baseline, sync the delta
        result = _sync_with_history_api(
            gmail_service, supabase, user_id, connection_id,
            subscription_id, old_history_id, history_id
        )
    elif not old_history_id:
        # First notification or missing history_id - do fallback sync
        # to catch any emails that triggered this notification
        logger.info(f"ℹ️ No previous history_id, performing fallback sync for user {user_id[:8]}...")
        result = _fallback_sync_with_recovery(
            gmail_service, supabase, user_id, connection_id, subscription_id
        )
    else:
        # Same history_id (old_history_id == history_id) - no changes
        logger.info(f"ℹ️ No history delta (history_id unchanged: {history_id})")
        result = {"status": "ok", "message": "No changes detected"}

    return result


def _sync_with_history_api(
    gmail_service,
    supabase,
    user_id: str,
    connection_id: str,
    subscription_id: str,
    old_history_id: str,
    new_history_id: str
) -> Dict[str, Any]:
    """
    Sync emails using Gmail History API.
    Falls back to recent messages if history is unavailable.
    """
    logger.info(f"📜 Fetching history from {old_history_id}")

    try:
        history_result = gmail_service.users().history().list(
            userId='me',
            startHistoryId=old_history_id,
            historyTypes=['messageAdded']
        ).execute()

        # Extract new messages from history
        messages_to_fetch = []
        for record in history_result.get('history', []):
            messages_added = record.get('messagesAdded', [])
            for msg_added in messages_added:
                msg = msg_added.get('message')
                if msg:
                    messages_to_fetch.append(msg)

        logger.info(f"📧 Found {len(messages_to_fetch)} new messages in history")

        # Sync messages using batch API (#12 fix)
        synced_count = _sync_messages_batch(
            gmail_service, supabase, user_id, connection_id, messages_to_fetch
        )

        # Update history_id after successful sync (monotonic, concurrency-safe in DB)
        response_history_id = str(history_result.get('historyId', new_history_id))
        try:
            supabase.rpc(
                'update_push_subscription_history_id',
                {
                    'p_subscription_id': subscription_id,
                    'p_history_id': response_history_id
                }
            ).execute()
        except Exception as e:
            logger.warning(f"⚠️ RPC update_history_id failed, falling back to direct update: {e}")
            supabase.table('push_subscriptions')\
                .update({
                    'history_id': response_history_id,
                    'updated_at': datetime.now(timezone.utc).isoformat()
                })\
                .eq('id', subscription_id)\
                .execute()

        logger.info(f"✅ Gmail history sync completed - synced {synced_count} emails")
        return {"status": "ok", "synced": synced_count}

    except HttpError as e:
        if e.resp.status == 404:
            logger.warning(f"⚠️ History ID {old_history_id} expired/invalid (404)")
            return _fallback_sync_with_recovery(
                gmail_service, supabase, user_id, connection_id, subscription_id
            )
        elif e.resp.status == 401:
            logger.error("❌ Authentication error (401) - token may have been revoked")
            return {"status": "error", "message": "Authentication failed", "code": 401}
        else:
            logger.error(f"❌ History API failed with status {e.resp.status}: {str(e)}")
            return {"status": "error", "message": str(e)}
    except Exception as e:
        logger.error(f"❌ Error processing history: {str(e)}")
        return {"status": "error", "message": str(e)}


def _fallback_sync_with_recovery(
    gmail_service,
    supabase,
    user_id: str,
    connection_id: str,
    subscription_id: str
) -> Dict[str, Any]:
    """
    Fallback sync with proper historyId recovery.

    1. Gets current historyId from Gmail profile
    2. Fetches recent messages with time-bound query
    3. Updates subscription with valid historyId

    This prevents the spam request loop by:
    - Using time-bound queries (not just maxResults)
    - Recovering valid historyId before updating
    """
    logger.info(f"🔄 Fallback sync with historyId recovery for user {user_id}")

    try:
        # Step 1: Get current valid historyId from Gmail
        current_history_id = get_current_gmail_history_id(gmail_service)
        if not current_history_id:
            logger.error("❌ Could not recover historyId from Gmail profile")
            return {"status": "error", "message": "Failed to recover historyId"}

        logger.info(f"📧 Recovered current historyId: {current_history_id}")

        # Step 2: Fetch recent messages with time-bound query
        # This prevents fetching the same old messages repeatedly
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=FALLBACK_WINDOW_HOURS)
        cutoff_str = cutoff_time.strftime('%Y/%m/%d')

        messages_result = gmail_service.users().messages().list(
            userId='me',
            maxResults=FALLBACK_MAX_MESSAGES,
            q=f'after:{cutoff_str}',  # Time-bound query
            labelIds=['INBOX']
        ).execute()

        messages = messages_result.get('messages', [])
        logger.info(f"📧 Found {len(messages)} messages in last {FALLBACK_WINDOW_HOURS}h")

        # Step 3: Sync messages using batch API
        synced_count = _sync_messages_batch(
            gmail_service, supabase, user_id, connection_id, messages
        )

        # Step 4: Update with recovered historyId (monotonic, concurrency-safe in DB)
        try:
            supabase.rpc(
                'update_push_subscription_history_id',
                {
                    'p_subscription_id': subscription_id,
                    'p_history_id': str(current_history_id)
                }
            ).execute()
        except Exception as e:
            logger.warning(f"⚠️ RPC update_history_id failed, falling back to direct update: {e}")
            supabase.table('push_subscriptions')\
                .update({
                    'history_id': str(current_history_id),
                    'updated_at': datetime.now(timezone.utc).isoformat()
                })\
                .eq('id', subscription_id)\
                .execute()

        logger.info(f"✅ Fallback sync completed - synced {synced_count}, recovered historyId")
        return {
            "status": "ok",
            "synced": synced_count,
            "fallback": True,
            "recovered_history_id": current_history_id
        }

    except HttpError as e:
        logger.error(f"❌ Fallback sync failed with HTTP error: {str(e)}")
        return {"status": "error", "message": str(e)}
    except Exception as e:
        logger.error(f"❌ Fallback sync failed: {str(e)}")
        return {"status": "error", "message": str(e)}


def _sync_messages_batch(
    gmail_service,
    supabase,
    user_id: str,
    connection_id: str,
    messages: List[Dict[str, Any]]
) -> int:
    """
    Sync messages using Gmail batch API to avoid N+1 queries (#12 fix).

    Args:
        gmail_service: Authenticated Gmail service
        supabase: Supabase client
        user_id: User ID
        connection_id: Connection ID
        messages: List of message stubs with 'id' field

    Returns:
        Number of successfully synced messages
    """
    if not messages:
        return 0

    # Deduplicate by message ID
    seen_ids = set()
    unique_messages = []
    for msg in messages:
        msg_id = msg.get('id')
        if msg_id and msg_id not in seen_ids:
            seen_ids.add(msg_id)
            unique_messages.append(msg)

    logger.info(f"📧 Syncing {len(unique_messages)} unique messages (batch mode)")
    try:
        active_draft_map = list_active_gmail_drafts_by_message_id(gmail_service)
    except Exception as e:
        logger.warning(f"Could not fetch active Gmail drafts map: {e}")
        active_draft_map = None

    # Collect full message data from batch request
    fetched_messages: List[Dict[str, Any]] = []
    errors: List[str] = []

    def handle_message_response(request_id: str, response: Dict, exception: HttpError):
        """Callback for batch request responses"""
        if exception:
            if exception.resp.status == 404:
                # Message deleted - this is normal
                logger.debug(f"⏭️ Message {request_id} was deleted")
            else:
                errors.append(f"{request_id}: {str(exception)}")
        else:
            fetched_messages.append(response)

    # Process in batches to respect API limits
    for i in range(0, len(unique_messages), BATCH_SIZE):
        batch_slice = unique_messages[i:i + BATCH_SIZE]
        batch = gmail_service.new_batch_http_request(callback=handle_message_response)

        for msg in batch_slice:
            batch.add(
                gmail_service.users().messages().get(
                    userId='me',
                    id=msg['id'],
                    format='full'
                ),
                request_id=msg['id']
            )

        try:
            batch.execute()
            # Small delay between batches to avoid Gmail rate limits (429 errors)
            if i + BATCH_SIZE < len(unique_messages):
                time.sleep(0.3)
        except Exception as e:
            logger.error(f"❌ Batch request failed: {str(e)}")
            # Fall back to individual requests for this batch
            for msg in batch_slice:
                try:
                    full_msg = gmail_service.users().messages().get(
                        userId='me',
                        id=msg['id'],
                        format='full'
                    ).execute()
                    fetched_messages.append(full_msg)
                except HttpError as he:
                    if he.resp.status != 404:
                        errors.append(f"{msg['id']}: {str(he)}")

    if errors:
        logger.warning(f"⚠️ {len(errors)} errors during batch fetch")
        for err in errors[:5]:  # Log first 5 errors
            logger.warning(f"   {err}")

    # Save fetched messages to database and analyze with AI
    synced_count = 0
    analyzed_count = 0
    
    for full_msg in fetched_messages:
        try:
            email_data = _parse_email_to_data(
                full_msg,
                user_id,
                connection_id,
                draft_message_to_draft_id=active_draft_map,
            )
            if not email_data:
                continue

            # Upsert the email
            result = supabase.table('emails')\
                .upsert(email_data, on_conflict='user_id,external_id')\
                .execute()

            synced_count += 1

            # Embed for semantic search (fire-and-forget)
            if result.data and len(result.data) > 0:
                from lib.embed_hooks import embed_email
                embed_email(
                    result.data[0].get("id"),
                    email_data.get("subject"),
                    email_data.get("snippet"),
                )

            # Analyze with AI if not already analyzed
            # Check if this email needs AI analysis
            if result.data and len(result.data) > 0:
                email_record = result.data[0]
                email_id = email_record.get('id')
                
                # Only analyze if not already analyzed
                if not email_record.get('ai_analyzed'):
                    try:
                        logger.info(f"🤖 Analyzing email {email_id[:8]}... with AI")
                        analysis = analyze_email_with_ai(
                            subject=email_data.get("subject"),
                            from_address=email_data.get("from"),
                            body=email_data.get("body"),
                            snippet=email_data.get("snippet")
                        )
                        
                        # Update with AI analysis
                        supabase.table('emails').update({
                            'ai_analyzed': True,
                            'ai_summary': analysis['summary'],
                            'ai_important': analysis['important']
                        }).eq('id', email_id).execute()
                        
                        analyzed_count += 1
                        logger.info(f"   ✅ AI summary: {analysis['summary']}")
                        
                    except Exception as ai_err:
                        logger.error(f"⚠️ Failed to analyze email {email_id[:8]}... with AI: {str(ai_err)}")

        except Exception as e:
            logger.error(f"❌ Error saving email {full_msg.get('id')}: {str(e)}")

    if active_draft_map is not None:
        try:
            cleanup_inactive_draft_rows_for_connection(
                supabase_client=supabase,
                user_id=user_id,
                ext_connection_id=connection_id,
                active_external_ids=active_draft_map.keys(),
            )
        except Exception as e:
            logger.warning(f"Draft reconciliation failed in webhook sync (non-fatal): {e}")
    else:
        logger.warning("Skipping webhook draft reconciliation because active draft map is unavailable")

    logger.info(f"✅ Batch sync complete: {synced_count}/{len(fetched_messages)} saved, {analyzed_count} analyzed")
    return synced_count


def _parse_email_to_data(
    full_msg: Dict[str, Any],
    user_id: str,
    connection_id: str,
    draft_message_to_draft_id: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Parse Gmail message into database format.
    """
    headers = parse_email_headers(full_msg.get('payload', {}).get('headers', []))
    body_dict = decode_email_body(full_msg.get('payload', {}))
    body_content = body_dict.get('html') or body_dict.get('plain', '')

    message_id = full_msg.get('id')
    thread_id = full_msg.get('threadId')
    snippet = full_msg.get('snippet', '')
    labels = full_msg.get('labelIds', [])
    internal_date = full_msg.get('internalDate')

    # Convert internal date
    received_at = None
    if internal_date:
        received_at = datetime.fromtimestamp(
            int(internal_date) / 1000,
            tz=timezone.utc
        ).isoformat()

    # Check flags
    is_unread = 'UNREAD' in labels
    is_starred = 'STARRED' in labels
    is_draft = 'DRAFT' in labels
    is_trashed = 'TRASH' in labels
    gmail_draft_id = draft_message_to_draft_id.get(message_id) if (is_draft and draft_message_to_draft_id) else None

    # Ignore stale draft revisions that are no longer active in Gmail.
    if is_draft and draft_message_to_draft_id is not None and not gmail_draft_id:
        logger.debug(f"Skipping stale Gmail draft revision {message_id}")
        return None

    # Get attachments
    attachments = get_attachment_info(full_msg.get('payload', {}))

    # Parse email addresses
    to_emails = _parse_email_list(headers.get('to', ''))
    cc_emails = _parse_email_list(headers.get('cc', ''))
    bcc_emails = _parse_email_list(headers.get('bcc', ''))

    return {
        'user_id': user_id,
        'ext_connection_id': connection_id,
        'external_id': message_id,
        'thread_id': thread_id,
        'subject': headers.get('subject', '(No subject)'),
        'from': headers.get('from', ''),
        'to': to_emails,
        'cc': cc_emails,
        'bcc': bcc_emails,
        'body': body_content,
        'snippet': snippet,
        'labels': labels,
        'is_read': not is_unread,
        'is_draft': is_draft,
        'gmail_draft_id': gmail_draft_id,
        'is_trashed': is_trashed,
        'is_starred': is_starred,
        'received_at': received_at,
        'sent_at': received_at,
        'has_attachments': len(attachments) > 0,
        'attachments': attachments,
        'raw_item': full_msg,
        'synced_at': datetime.now(timezone.utc).isoformat(),
        'updated_at': datetime.now(timezone.utc).isoformat()
    }


def _parse_email_list(email_str: str) -> List[str]:
    """Parse comma-separated email string into list."""
    if not email_str:
        return []
    return [e.strip() for e in email_str.split(',') if e.strip()]
