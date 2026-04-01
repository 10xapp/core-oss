"""
Watch Manager - Manages Google push notification subscriptions
Handles starting, renewing, and stopping watches for Gmail and Calendar
"""
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from lib.supabase_client import get_authenticated_supabase_client
import logging
import uuid
from googleapiclient.errors import HttpError
from api.services.email.google_api_helpers import get_gmail_service
from api.services.calendar.google_api_helpers import get_google_calendar_service
from api.config import settings
from api.services.syncs.google_error_utils import is_permanent_google_api_error

logger = logging.getLogger(__name__)

# Gmail watch subscriptions expire after 7 days
GMAIL_WATCH_EXPIRATION_DAYS = 7

# Calendar watch subscriptions - we'll set to 7 days for consistency
CALENDAR_WATCH_EXPIRATION_DAYS = 7


def start_gmail_watch(
    user_id: str,
    user_jwt: str,
    webhook_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Start watching a user's Gmail for changes using push notifications
    
    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        webhook_url: Optional webhook URL (defaults to production URL)
        
    Returns:
        Dict with watch information including historyId and expiration
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)
    
    # Get Gmail service and connection
    service, connection_id = get_gmail_service(user_id, user_jwt)
    
    if not service or not connection_id:
        raise ValueError("No active Google connection found for user")
    
    try:
        # Check if there's an existing active watch
        existing = auth_supabase.table('push_subscriptions')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('provider', 'gmail')\
            .eq('is_active', True)\
            .execute()
        
        if existing.data:
            # Check if watch is still valid (not expiring soon)
            expiration = datetime.fromisoformat(existing.data[0]['expiration'].replace('Z', '+00:00'))
            time_until_expiry = expiration - datetime.now(timezone.utc)
            hours_until_expiry = time_until_expiry.total_seconds() / 3600
            
            if hours_until_expiry > 24:
                logger.info(f"✅ Gmail watch already exists and is healthy for user {user_id[:8]}... (expires in {hours_until_expiry:.1f}h)")
                return {
                    'success': True,
                    'provider': 'gmail',
                    'channel_id': existing.data[0]['channel_id'],
                    'history_id': existing.data[0].get('history_id'),
                    'expiration': existing.data[0]['expiration'],
                    'subscription_id': existing.data[0]['id'],
                    'message': 'Watch already exists and is healthy'
                }
            
            logger.info(f"🔄 Gmail watch exists but expiring soon for user {user_id[:8]}..., will renew")
            # Stop existing watch first
            try:
                stop_gmail_watch(user_id, user_jwt)
            except Exception as e:
                logger.warning(f"⚠️ Could not stop existing watch: {e}")
        
        # Generate unique channel ID
        channel_id = str(uuid.uuid4())
        
        # Set webhook URL (use configured base URL from settings)
        if not webhook_url:
            webhook_url = f"{settings.webhook_base_url}/api/webhooks/gmail"
        
        # Gmail watch requires Google Cloud Pub/Sub setup
        # Check if GOOGLE_PUBSUB_TOPIC is configured
        if not settings.google_pubsub_topic:
            logger.error("❌ GOOGLE_PUBSUB_TOPIC not configured")
            raise ValueError(
                "Gmail push notifications require GOOGLE_PUBSUB_TOPIC environment variable. "
                "Example: GOOGLE_PUBSUB_TOPIC=projects/YOUR_PROJECT/topics/gmail-sync-topic"
            )
        
        topic_name = settings.google_pubsub_topic
        logger.info(f"📢 Using Pub/Sub topic: {topic_name}")
        
        request_body = {
            'labelIds': ['INBOX'],  # Watch all messages in inbox
            'topicName': topic_name
        }
        
        logger.info(f"🔔 Starting Gmail watch for user {user_id} with channel {channel_id}")
        
        # Start the watch
        watch_response = service.users().watch(
            userId='me',
            body=request_body
        ).execute()
        
        # Extract response data
        history_id = watch_response.get('historyId')
        expiration_ms = watch_response.get('expiration')
        
        # Convert expiration from milliseconds to datetime
        if expiration_ms:
            expiration = datetime.fromtimestamp(
                int(expiration_ms) / 1000,
                tz=timezone.utc
            )
        else:
            # Default to 7 days if not provided
            expiration = datetime.now(timezone.utc) + timedelta(days=GMAIL_WATCH_EXPIRATION_DAYS)
        
        # Store watch subscription in database
        # First deactivate any existing subscriptions for this user/provider
        auth_supabase.table('push_subscriptions')\
            .update({'is_active': False})\
            .eq('user_id', user_id)\
            .eq('provider', 'gmail')\
            .execute()
        
        subscription_data = {
            'user_id': user_id,
            'ext_connection_id': connection_id,
            'provider': 'gmail',
            'channel_id': channel_id,
            'resource_id': None,  # Gmail doesn't return this
            'history_id': history_id,
            'sync_token': None,
            'expiration': expiration.isoformat(),
            'is_active': True,
            'metadata': {
                'watch_response': watch_response,
                'webhook_url': webhook_url
            }
        }
        
        result = auth_supabase.table('push_subscriptions')\
            .insert(subscription_data)\
            .execute()
        
        logger.info(f"✅ Gmail watch started successfully for user {user_id}")
        logger.info(f"📅 Watch expires at: {expiration.isoformat()}")
        
        return {
            'success': True,
            'provider': 'gmail',
            'channel_id': channel_id,
            'history_id': history_id,
            'expiration': expiration.isoformat(),
            'subscription_id': result.data[0]['id']
        }
        
    except HttpError as e:
        error_msg = str(e)
        logger.error(f"❌ Gmail API error starting watch: {error_msg}")
        
        # Check for common errors
        if 'Pub/Sub' in error_msg or 'topic' in error_msg.lower():
            logger.error("💡 Gmail push notifications require Google Cloud Pub/Sub setup")
            logger.error("💡 See: https://developers.google.com/gmail/api/guides/push")
            raise ValueError(
                "Gmail push notifications require Pub/Sub configuration. "
                "Please set up Google Cloud Pub/Sub topic and grant permissions."
            )
        
        raise ValueError(f"Failed to start Gmail watch: {error_msg}")
    except Exception as e:
        logger.error(f"❌ Error starting Gmail watch: {str(e)}")
        raise ValueError(f"Gmail watch setup failed: {str(e)}")


def start_calendar_watch(
    user_id: str,
    user_jwt: str,
    webhook_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Start watching a user's Google Calendar for changes using push notifications
    
    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        webhook_url: Optional webhook URL (defaults to production URL)
        
    Returns:
        Dict with watch information including sync token and expiration
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)
    
    # Get Calendar service and connection
    service, connection_id = get_google_calendar_service(user_id, user_jwt)
    
    if not service or not connection_id:
        raise ValueError("No active Google connection found for user")
    
    try:
        # Check if there's an existing active watch
        existing = auth_supabase.table('push_subscriptions')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('provider', 'calendar')\
            .eq('is_active', True)\
            .execute()
        
        if existing.data:
            # Check if watch is still valid (not expiring soon)
            expiration = datetime.fromisoformat(existing.data[0]['expiration'].replace('Z', '+00:00'))
            time_until_expiry = expiration - datetime.now(timezone.utc)
            hours_until_expiry = time_until_expiry.total_seconds() / 3600
            
            if hours_until_expiry > 24:
                logger.info(f"✅ Calendar watch already exists and is healthy for user {user_id[:8]}... (expires in {hours_until_expiry:.1f}h)")
                return {
                    'success': True,
                    'provider': 'calendar',
                    'channel_id': existing.data[0]['channel_id'],
                    'resource_id': existing.data[0].get('resource_id'),
                    'sync_token': existing.data[0].get('sync_token'),
                    'expiration': existing.data[0]['expiration'],
                    'subscription_id': existing.data[0]['id'],
                    'message': 'Watch already exists and is healthy'
                }
            
            logger.info(f"🔄 Calendar watch exists but expiring soon for user {user_id[:8]}..., will renew")
            # Stop existing watch first
            try:
                stop_calendar_watch(user_id, user_jwt)
            except Exception as e:
                logger.warning(f"⚠️ Could not stop existing watch: {e}")
        
        # Generate unique channel ID
        channel_id = str(uuid.uuid4())
        
        # Set webhook URL (use configured base URL from settings)
        if not webhook_url:
            webhook_url = f"{settings.webhook_base_url}/api/webhooks/calendar"
        
        # Calculate expiration (7 days from now)
        expiration = datetime.now(timezone.utc) + timedelta(days=CALENDAR_WATCH_EXPIRATION_DAYS)
        expiration_ms = int(expiration.timestamp() * 1000)
        
        request_body = {
            'id': channel_id,
            'type': 'web_hook',
            'address': webhook_url,
            'expiration': expiration_ms
        }
        
        logger.info(f"🔔 Starting Calendar watch for user {user_id} with channel {channel_id}")
        
        # Start the watch
        watch_response = service.events().watch(
            calendarId='primary',
            body=request_body
        ).execute()
        
        # Extract response data
        resource_id = watch_response.get('resourceId')
        returned_expiration = watch_response.get('expiration')
        
        # Use returned expiration if available
        if returned_expiration:
            expiration = datetime.fromtimestamp(
                int(returned_expiration) / 1000,
                tz=timezone.utc
            )
        
        # Get sync token for incremental updates
        try:
            sync_result = service.events().list(
                calendarId='primary',
                maxResults=1
            ).execute()
            sync_token = sync_result.get('nextSyncToken')
        except Exception as e:
            logger.warning(f"⚠️ Could not get sync token: {e}")
            sync_token = None
        
        # Store watch subscription in database
        # First deactivate any existing subscriptions for this user/provider
        auth_supabase.table('push_subscriptions')\
            .update({'is_active': False})\
            .eq('user_id', user_id)\
            .eq('provider', 'calendar')\
            .execute()
        
        subscription_data = {
            'user_id': user_id,
            'ext_connection_id': connection_id,
            'provider': 'calendar',
            'channel_id': channel_id,
            'resource_id': resource_id,
            'history_id': None,
            'sync_token': sync_token,
            'expiration': expiration.isoformat(),
            'is_active': True,
            'metadata': {
                'watch_response': watch_response,
                'webhook_url': webhook_url
            }
        }
        
        result = auth_supabase.table('push_subscriptions')\
            .insert(subscription_data)\
            .execute()
        
        logger.info(f"✅ Calendar watch started successfully for user {user_id}")
        logger.info(f"📅 Watch expires at: {expiration.isoformat()}")
        
        return {
            'success': True,
            'provider': 'calendar',
            'channel_id': channel_id,
            'resource_id': resource_id,
            'sync_token': sync_token,
            'expiration': expiration.isoformat(),
            'subscription_id': result.data[0]['id']
        }
        
    except HttpError as e:
        logger.error(f"❌ Calendar API error starting watch: {str(e)}")
        raise ValueError(f"Failed to start Calendar watch: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Error starting Calendar watch: {str(e)}")
        raise ValueError(f"Calendar watch setup failed: {str(e)}")


def stop_gmail_watch(user_id: str, user_jwt: str) -> Dict[str, Any]:
    """
    Stop watching a user's Gmail for changes
    
    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT
        
    Returns:
        Dict with success status
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)
    service, _ = get_gmail_service(user_id, user_jwt)
    
    if not service:
        raise ValueError("No active Google connection found")
    
    try:
        # Get active subscription
        subscription = auth_supabase.table('push_subscriptions')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('provider', 'gmail')\
            .eq('is_active', True)\
            .execute()
        
        if not subscription.data:
            logger.info(f"ℹ️ No active Gmail watch found for user {user_id}")
            return {'success': True, 'message': 'No active watch to stop'}
        
        # Stop the watch with Google
        try:
            service.users().stop(userId='me').execute()
            logger.info(f"🛑 Gmail watch stopped with Google for user {user_id}")
        except HttpError as e:
            logger.warning(f"⚠️ Could not stop watch with Google: {e}")
        
        # Mark as inactive in database
        auth_supabase.table('push_subscriptions')\
            .update({'is_active': False})\
            .eq('id', subscription.data[0]['id'])\
            .execute()
        
        logger.info(f"✅ Gmail watch stopped for user {user_id}")
        return {'success': True, 'message': 'Gmail watch stopped'}
        
    except Exception as e:
        logger.error(f"❌ Error stopping Gmail watch: {str(e)}")
        raise ValueError(f"Failed to stop Gmail watch: {str(e)}")


def stop_calendar_watch(user_id: str, user_jwt: str) -> Dict[str, Any]:
    """
    Stop watching a user's Google Calendar for changes
    
    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT
        
    Returns:
        Dict with success status
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)
    service, _ = get_google_calendar_service(user_id, user_jwt)
    
    if not service:
        raise ValueError("No active Google connection found")
    
    try:
        # Get active subscription
        subscription = auth_supabase.table('push_subscriptions')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('provider', 'calendar')\
            .eq('is_active', True)\
            .execute()
        
        if not subscription.data:
            logger.info(f"ℹ️ No active Calendar watch found for user {user_id}")
            return {'success': True, 'message': 'No active watch to stop'}
        
        sub_data = subscription.data[0]
        channel_id = sub_data.get('channel_id')
        resource_id = sub_data.get('resource_id')
        
        # Stop the watch with Google
        if channel_id and resource_id:
            try:
                service.channels().stop(body={
                    'id': channel_id,
                    'resourceId': resource_id
                }).execute()
                logger.info(f"🛑 Calendar watch stopped with Google for user {user_id}")
            except HttpError as e:
                logger.warning(f"⚠️ Could not stop watch with Google: {e}")
        
        # Mark as inactive in database
        auth_supabase.table('push_subscriptions')\
            .update({'is_active': False})\
            .eq('id', sub_data['id'])\
            .execute()
        
        logger.info(f"✅ Calendar watch stopped for user {user_id}")
        return {'success': True, 'message': 'Calendar watch stopped'}
        
    except Exception as e:
        logger.error(f"❌ Error stopping Calendar watch: {str(e)}")
        raise ValueError(f"Failed to stop Calendar watch: {str(e)}")


def renew_watch(user_id: str, user_jwt: str, provider: str) -> Dict[str, Any]:
    """
    Renew a watch subscription that's about to expire
    
    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT
        provider: 'gmail' or 'calendar'
        
    Returns:
        Dict with new watch information
    """
    logger.info(f"🔄 Renewing {provider} watch for user {user_id}")
    
    if provider == 'gmail':
        return start_gmail_watch(user_id, user_jwt)
    elif provider == 'calendar':
        return start_calendar_watch(user_id, user_jwt)
    else:
        raise ValueError(f"Unknown provider: {provider}")


def get_expiring_subscriptions(hours_threshold: int = 24) -> list:
    """
    Get all watch subscriptions that will expire within the threshold
    
    Args:
        hours_threshold: Hours before expiration to consider (default 24)
        
    Returns:
        List of subscriptions needing renewal
    """
    from lib.supabase_client import get_supabase_client
    
    supabase = get_supabase_client()
    threshold_time = datetime.now(timezone.utc) + timedelta(hours=hours_threshold)
    
    try:
        result = supabase.table('push_subscriptions')\
            .select('*, ext_connections!push_subscriptions_ext_connection_id_fkey!inner(user_id, is_active)')\
            .eq('is_active', True)\
            .lt('expiration', threshold_time.isoformat())\
            .execute()
        
        logger.info(f"📋 Found {len(result.data)} subscriptions expiring within {hours_threshold} hours")
        return result.data
        
    except Exception as e:
        logger.error(f"❌ Error getting expiring subscriptions: {str(e)}")
        return []


def setup_watches_for_user(user_id: str, user_jwt: str) -> Dict[str, Any]:
    """
    Set up both Gmail and Calendar watches for a user
    Useful for initial setup or recovery

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT

    Returns:
        Dict with results for both watches
    """
    results = {
        'user_id': user_id,
        'gmail': None,
        'calendar': None
    }

    # Try Gmail watch
    try:
        gmail_result = start_gmail_watch(user_id, user_jwt)
        results['gmail'] = gmail_result
        logger.info(f"✅ Gmail watch set up for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Failed to set up Gmail watch: {str(e)}")
        results['gmail'] = {'success': False, 'error': str(e)}

    # Try Calendar watch
    try:
        calendar_result = start_calendar_watch(user_id, user_jwt)
        results['calendar'] = calendar_result
        logger.info(f"✅ Calendar watch set up for user {user_id}")
    except Exception as e:
        logger.error(f"❌ Failed to set up Calendar watch: {str(e)}")
        results['calendar'] = {'success': False, 'error': str(e)}

    return results


# =============================================================================
# Service Role Variants for Cron Jobs (#20 fix)
# These functions don't require user_jwt - they use service role credentials
# =============================================================================

def _check_and_renew_existing_gmail_watch(
    watch_row: Dict[str, Any],
    user_id: str,
    connection_id: str,
    gmail_service,
    service_supabase,
) -> Optional[Dict[str, Any]]:
    """Check an existing watch. Return a healthy-result dict to short-circuit, or None to proceed with renewal."""
    expiration = datetime.fromisoformat(watch_row['expiration'].replace('Z', '+00:00'))
    hours_until_expiry = (expiration - datetime.now(timezone.utc)).total_seconds() / 3600

    if hours_until_expiry > 24:
        logger.info(f"✅ Gmail watch healthy for connection {connection_id[:8]}... ({hours_until_expiry:.1f}h remaining)")
        return {
            'success': True,
            'provider': 'gmail',
            'message': 'Watch already exists and is healthy',
            'hours_remaining': hours_until_expiry,
        }

    logger.info(f"🔄 Gmail watch expiring soon for user {user_id[:8]}..., renewing")
    # Stop the old watch with Gmail first to prevent duplicate notifications
    old_watch_stopped = False
    try:
        gmail_service.users().stop(userId='me').execute()
        logger.info(f"🛑 Stopped old Gmail watch before renewal for user {user_id[:8]}...")
        old_watch_stopped = True
    except HttpError as e:
        status_code = getattr(getattr(e, "resp", None), "status", None)
        if status_code in (404, 410):
            logger.info(f"ℹ️ Old Gmail watch already expired for user {user_id[:8]}...: {e}")
            old_watch_stopped = True
        else:
            logger.error(f"❌ Failed to stop old Gmail watch for user {user_id[:8]}..., skipping renewal: {e}")
    except Exception as e:
        logger.error(f"❌ Failed to stop old Gmail watch for user {user_id[:8]}..., skipping renewal: {e}")

    if not old_watch_stopped:
        return {
            'success': False,
            'provider': 'gmail',
            'error': 'Failed to stop old watch; skipping renewal to avoid untracked watches',
        }

    # Deactivate old watch in DB
    service_supabase.table('push_subscriptions')\
        .update({'is_active': False})\
        .eq('id', watch_row['id'])\
        .execute()
    return None


def start_gmail_watch_service_role(
    user_id: str,
    gmail_service,
    connection_id: str,
    service_supabase
) -> Dict[str, Any]:
    """
    Start Gmail watch using service role (for cron jobs).

    Args:
        user_id: User's ID
        gmail_service: Pre-built Gmail API service
        connection_id: ext_connections.id
        service_supabase: Service role Supabase client

    Returns:
        Dict with watch information
    """
    try:
        # Check if there's an existing active watch FOR THIS CONNECTION
        # (not just user - important for multi-account/secondary account support)
        existing = service_supabase.table('push_subscriptions')\
            .select('*')\
            .eq('ext_connection_id', connection_id)\
            .eq('provider', 'gmail')\
            .eq('is_active', True)\
            .execute()

        if existing.data:
            healthy_result = _check_and_renew_existing_gmail_watch(
                existing.data[0], user_id, connection_id, gmail_service, service_supabase
            )
            if healthy_result is not None:
                return healthy_result

        # Check Pub/Sub topic configuration
        if not settings.google_pubsub_topic:
            raise ValueError("GOOGLE_PUBSUB_TOPIC not configured")

        # Generate unique channel ID
        channel_id = str(uuid.uuid4())
        webhook_url = f"{settings.webhook_base_url}/api/webhooks/gmail"

        request_body = {
            'labelIds': ['INBOX'],
            'topicName': settings.google_pubsub_topic
        }

        logger.info(f"🔔 Starting Gmail watch for user {user_id[:8]}...")

        # Start the watch
        watch_response = gmail_service.users().watch(
            userId='me',
            body=request_body
        ).execute()

        history_id = watch_response.get('historyId')
        expiration_ms = watch_response.get('expiration')

        if expiration_ms:
            expiration = datetime.fromtimestamp(int(expiration_ms) / 1000, tz=timezone.utc)
        else:
            expiration = datetime.now(timezone.utc) + timedelta(days=GMAIL_WATCH_EXPIRATION_DAYS)

        # Store subscription
        subscription_data = {
            'user_id': user_id,
            'ext_connection_id': connection_id,
            'provider': 'gmail',
            'channel_id': channel_id,
            'resource_id': None,
            'history_id': history_id,
            'sync_token': None,
            'expiration': expiration.isoformat(),
            'is_active': True,
            'metadata': {'watch_response': watch_response, 'webhook_url': webhook_url}
        }

        result = service_supabase.table('push_subscriptions')\
            .insert(subscription_data)\
            .execute()

        logger.info(f"✅ Gmail watch started for user {user_id[:8]}..., expires {expiration.isoformat()}")

        return {
            'success': True,
            'provider': 'gmail',
            'history_id': history_id,
            'expiration': expiration.isoformat(),
            'subscription_id': result.data[0]['id'] if result.data else None
        }

    except HttpError as e:
        if is_permanent_google_api_error(e):
            logger.warning(f"⚠️ Gmail watch permanently unavailable for connection {connection_id[:8]}...: {e}")
        else:
            logger.error(f"❌ Gmail API error: {str(e)}")
        return {'success': False, 'provider': 'gmail', 'error': str(e)}
    except Exception as e:
        logger.error(f"❌ Error starting Gmail watch: {str(e)}")
        return {'success': False, 'provider': 'gmail', 'error': str(e)}


def start_calendar_watch_service_role(
    user_id: str,
    calendar_service,
    connection_id: str,
    service_supabase
) -> Dict[str, Any]:
    """
    Start Calendar watch using service role (for cron jobs).

    Args:
        user_id: User's ID
        calendar_service: Pre-built Calendar API service
        connection_id: ext_connections.id
        service_supabase: Service role Supabase client

    Returns:
        Dict with watch information
    """
    try:
        # Check if there's an existing active watch FOR THIS CONNECTION
        # (not just user - important for multi-account/secondary account support)
        existing = service_supabase.table('push_subscriptions')\
            .select('*')\
            .eq('ext_connection_id', connection_id)\
            .eq('provider', 'calendar')\
            .eq('is_active', True)\
            .execute()

        if existing.data:
            expiration = datetime.fromisoformat(existing.data[0]['expiration'].replace('Z', '+00:00'))
            time_until_expiry = expiration - datetime.now(timezone.utc)
            hours_until_expiry = time_until_expiry.total_seconds() / 3600

            if hours_until_expiry > 24:
                logger.info(f"✅ Calendar watch healthy for connection {connection_id[:8]}... ({hours_until_expiry:.1f}h remaining)")
                return {
                    'success': True,
                    'provider': 'calendar',
                    'message': 'Watch already exists and is healthy',
                    'hours_remaining': hours_until_expiry
                }

            logger.info(f"🔄 Calendar watch expiring soon for user {user_id[:8]}..., renewing")

            # Try to stop existing watch with Google
            sub_data = existing.data[0]
            old_watch_stopped = False
            if sub_data.get('channel_id') and sub_data.get('resource_id'):
                try:
                    calendar_service.channels().stop(body={
                        'id': sub_data['channel_id'],
                        'resourceId': sub_data['resource_id']
                    }).execute()
                    old_watch_stopped = True
                except HttpError as e:
                    status_code = getattr(getattr(e, "resp", None), "status", None)
                    if status_code in (404, 410):
                        logger.info(f"ℹ️ Old Calendar watch already expired for connection {connection_id[:8]}...: {e}")
                        old_watch_stopped = True
                    else:
                        logger.error(f"❌ Failed to stop old Calendar watch for connection {connection_id[:8]}..., skipping renewal: {e}")
                except Exception as e:
                    logger.error(f"❌ Failed to stop old Calendar watch for connection {connection_id[:8]}..., skipping renewal: {e}")
            else:
                # No channel/resource to stop — treat as already gone
                old_watch_stopped = True

            if not old_watch_stopped:
                return {
                    'success': False,
                    'provider': 'calendar',
                    'error': 'Failed to stop old watch; skipping renewal to avoid untracked watches'
                }

            # Deactivate old watch
            service_supabase.table('push_subscriptions')\
                .update({'is_active': False})\
                .eq('id', sub_data['id'])\
                .execute()

        # Generate unique channel ID
        channel_id = str(uuid.uuid4())
        webhook_url = f"{settings.webhook_base_url}/api/webhooks/calendar"

        # Calculate expiration
        expiration = datetime.now(timezone.utc) + timedelta(days=CALENDAR_WATCH_EXPIRATION_DAYS)
        expiration_ms = int(expiration.timestamp() * 1000)

        request_body = {
            'id': channel_id,
            'type': 'web_hook',
            'address': webhook_url,
            'expiration': expiration_ms
        }

        logger.info(f"🔔 Starting Calendar watch for user {user_id[:8]}...")

        # Start the watch
        watch_response = calendar_service.events().watch(
            calendarId='primary',
            body=request_body
        ).execute()

        resource_id = watch_response.get('resourceId')
        returned_expiration = watch_response.get('expiration')

        if returned_expiration:
            expiration = datetime.fromtimestamp(int(returned_expiration) / 1000, tz=timezone.utc)

        # Get sync token
        try:
            sync_result = calendar_service.events().list(calendarId='primary', maxResults=1).execute()
            sync_token = sync_result.get('nextSyncToken')
        except Exception as e:
            logger.warning(f"⚠️ Could not get sync token: {e}")
            sync_token = None

        # Store subscription
        subscription_data = {
            'user_id': user_id,
            'ext_connection_id': connection_id,
            'provider': 'calendar',
            'channel_id': channel_id,
            'resource_id': resource_id,
            'history_id': None,
            'sync_token': sync_token,
            'expiration': expiration.isoformat(),
            'is_active': True,
            'metadata': {'watch_response': watch_response, 'webhook_url': webhook_url}
        }

        result = service_supabase.table('push_subscriptions')\
            .insert(subscription_data)\
            .execute()

        logger.info(f"✅ Calendar watch started for user {user_id[:8]}..., expires {expiration.isoformat()}")

        return {
            'success': True,
            'provider': 'calendar',
            'resource_id': resource_id,
            'sync_token': sync_token,
            'expiration': expiration.isoformat(),
            'subscription_id': result.data[0]['id'] if result.data else None
        }

    except HttpError as e:
        if is_permanent_google_api_error(e):
            logger.warning(f"⚠️ Calendar watch permanently unavailable for connection {connection_id[:8]}...: {e}")
        else:
            logger.error(f"❌ Calendar API error: {str(e)}")
        return {'success': False, 'provider': 'calendar', 'error': str(e)}
    except Exception as e:
        logger.error(f"❌ Error starting Calendar watch: {str(e)}")
        return {'success': False, 'provider': 'calendar', 'error': str(e)}


def renew_watch_service_role(
    user_id: str,
    provider: str,
    gmail_service,
    calendar_service,
    connection_id: str,
    service_supabase
) -> Dict[str, Any]:
    """
    Renew a watch using service role (for cron jobs).

    Args:
        user_id: User's ID
        provider: 'gmail' or 'calendar'
        gmail_service: Gmail API service (can be None if not renewing Gmail)
        calendar_service: Calendar API service (can be None if not renewing Calendar)
        connection_id: ext_connections.id
        service_supabase: Service role Supabase client

    Returns:
        Dict with renewal result
    """
    if provider == 'gmail':
        if not gmail_service:
            return {'success': False, 'provider': 'gmail', 'error': 'No Gmail service available'}
        return start_gmail_watch_service_role(user_id, gmail_service, connection_id, service_supabase)
    elif provider == 'calendar':
        if not calendar_service:
            return {'success': False, 'provider': 'calendar', 'error': 'No Calendar service available'}
        return start_calendar_watch_service_role(user_id, calendar_service, connection_id, service_supabase)
    else:
        return {'success': False, 'error': f'Unknown provider: {provider}'}

