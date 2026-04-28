"""
Microsoft Webhook Provider Implementation

Implements WebhookProvider protocol for Microsoft Graph change notifications.

Key differences from Google:
- Webhook validation: Must return validationToken as plain text (GET request during creation)
- clientState: Secret value sent during creation, verified on each notification
- Subscription renewal: PATCH request (vs re-creating for Google)
- Max expiration: 4230 minutes (~3 days) for mail, 10080 minutes (7 days) for calendar
"""
from typing import Dict, Any
import hmac
import logging
import secrets
import httpx
from datetime import datetime, timedelta, timezone

from api.config import settings

logger = logging.getLogger(__name__)

# Microsoft Graph API base URL
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

# Subscription expiration limits (in minutes)
MAIL_MAX_EXPIRATION_MINUTES = 4230  # ~3 days
CALENDAR_MAX_EXPIRATION_MINUTES = 10080  # 7 days

# We'll renew a bit before expiration
MAIL_EXPIRATION_MINUTES = 4000  # ~2.8 days
CALENDAR_EXPIRATION_MINUTES = 9000  # ~6.3 days


class MicrosoftWebhookProvider:
    """
    Microsoft implementation of WebhookProvider protocol.

    Handles Microsoft Graph change notification subscriptions.
    """

    @property
    def provider_name(self) -> str:
        return "microsoft"

    async def create_subscription(
        self,
        access_token: str,
        resource_type: str,
        connection_id: str,
        user_id: str,
        supabase_client=None
    ) -> Dict[str, Any]:
        """
        Create a Microsoft Graph subscription for change notifications.

        Microsoft subscription creation:
        1. POST to /subscriptions with subscription details
        2. Microsoft sends GET to webhook_url with validationToken
        3. Webhook must return validationToken as plain text within 10 seconds
        4. If validation succeeds, subscription is created

        IMPORTANT: This is async to prevent blocking the event loop while
        Microsoft validates our webhook endpoint.

        Args:
            access_token: Valid Microsoft access token
            resource_type: 'mail' or 'calendar'
            connection_id: The ext_connection ID
            user_id: The user ID
            supabase_client: Supabase client for storing subscription

        Returns:
            Dict with success, subscription_id, expiration, etc.
        """
        webhook_url = settings.microsoft_webhook_url
        if not webhook_url:
            logger.error("❌ [Microsoft] MICROSOFT_WEBHOOK_URL not configured")
            return {
                "success": False,
                "error": "Microsoft webhook URL not configured"
            }

        # Get resource path and expiration based on type
        if resource_type == 'mail':
            resource = MICROSOFT_RESOURCES['mail']
            expiration_minutes = MAIL_EXPIRATION_MINUTES
        elif resource_type == 'calendar':
            resource = MICROSOFT_RESOURCES['calendar']
            expiration_minutes = CALENDAR_EXPIRATION_MINUTES
        else:
            return {
                "success": False,
                "error": f"Unknown resource type: {resource_type}"
            }

        # Generate client_state for validation
        client_state = self.generate_client_state()

        # Build subscription payload
        payload = self.build_subscription_payload(
            resource=resource,
            webhook_url=webhook_url,
            client_state=client_state,
            expiration_minutes=expiration_minutes
        )

        logger.info(f"📡 [Microsoft] Creating {resource_type} subscription for connection {connection_id[:8]}...")
        logger.debug(f"📡 [Microsoft] Webhook URL: {webhook_url}")
        logger.debug(f"📡 [Microsoft] Resource: {resource}")

        try:
            # Use async httpx to avoid blocking the event loop
            # This allows FastAPI to handle the validation callback while we wait
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{GRAPH_API_URL}/subscriptions",
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json"
                    },
                    json=payload
                )

            if response.status_code == 201:
                subscription = response.json()
                subscription_id = subscription.get('id')
                expiration = subscription.get('expirationDateTime')

                logger.info(f"✅ [Microsoft] {resource_type} subscription created: {subscription_id}")
                logger.info(f"📅 [Microsoft] Expires: {expiration}")

                # Store subscription in database
                if supabase_client:
                    try:
                        sub_data = {
                            'user_id': user_id,
                            'ext_connection_id': connection_id,
                            'provider': 'microsoft',
                            'resource_type': resource_type,
                            'channel_id': subscription_id,
                            'resource_id': subscription.get('resource'),
                            'client_state': client_state,
                            'expiration': expiration,
                            'is_active': True
                        }

                        # Check if subscription exists for this connection+resource_type
                        existing = supabase_client.table('push_subscriptions')\
                            .select('id')\
                            .eq('ext_connection_id', connection_id)\
                            .eq('resource_type', resource_type)\
                            .limit(1)\
                            .execute()

                        if existing.data:
                            # Update existing subscription
                            supabase_client.table('push_subscriptions')\
                                .update(sub_data)\
                                .eq('id', existing.data[0]['id'])\
                                .execute()
                        else:
                            # Insert new subscription
                            supabase_client.table('push_subscriptions')\
                                .insert(sub_data)\
                                .execute()

                        logger.info("✅ [Microsoft] Subscription stored in database")
                    except Exception as e:
                        logger.error(f"❌ [Microsoft] Failed to store subscription: {e}")

                return {
                    "success": True,
                    "subscription_id": subscription_id,
                    "expiration": expiration,
                    "resource": resource
                }

            else:
                error_data = response.json()
                error = error_data.get('error', {})
                error_code = error.get('code', 'Unknown')
                error_msg = error.get('message', response.text)

                logger.error(f"❌ [Microsoft] Subscription creation failed: {error_code} - {error_msg}")

                # Handle specific errors
                if error_code == 'InvalidRequest' and 'Subscription validation request failed' in error_msg:
                    logger.error("❌ [Microsoft] Webhook validation failed - check that the endpoint is accessible")

                return {
                    "success": False,
                    "error": error_msg,
                    "error_code": error_code
                }

        except httpx.TimeoutException:
            logger.error("❌ [Microsoft] Subscription creation timed out")
            return {
                "success": False,
                "error": "Request timed out"
            }
        except Exception as e:
            logger.error(f"❌ [Microsoft] Subscription creation error: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def renew_subscription(
        self,
        access_token: str,
        subscription_id: str,
        resource_type: str,
        supabase_client=None
    ) -> Dict[str, Any]:
        """
        Renew an expiring Microsoft Graph subscription.

        Microsoft uses PATCH to update expirationDateTime (unlike Google which re-creates).

        Args:
            access_token: Valid Microsoft access token
            subscription_id: The subscription ID to renew
            resource_type: 'mail' or 'calendar' (for expiration time)
            supabase_client: Optional Supabase client to update stored expiration

        Returns:
            Dict with success, new expiration, etc.
        """
        # Get expiration based on type
        if resource_type == 'mail':
            expiration_minutes = MAIL_EXPIRATION_MINUTES
        elif resource_type == 'calendar':
            expiration_minutes = CALENDAR_EXPIRATION_MINUTES
        else:
            expiration_minutes = MAIL_EXPIRATION_MINUTES  # Default to shorter

        new_expiration = datetime.now(timezone.utc) + timedelta(minutes=expiration_minutes)
        expiration_str = new_expiration.isoformat().replace('+00:00', 'Z')

        logger.info(f"🔄 [Microsoft] Renewing subscription {subscription_id[:20]}...")

        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.patch(
                    f"{GRAPH_API_URL}/subscriptions/{subscription_id}",
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json"
                    },
                    json={"expirationDateTime": expiration_str}
                )

            if response.status_code == 200:
                result = response.json()
                new_exp = result.get('expirationDateTime')

                logger.info(f"✅ [Microsoft] Subscription renewed until {new_exp}")

                # Update stored expiration
                if supabase_client:
                    try:
                        supabase_client.table('push_subscriptions')\
                            .update({'expiration': new_exp})\
                            .eq('channel_id', subscription_id)\
                            .execute()
                    except Exception as e:
                        logger.warning(f"⚠️ [Microsoft] Failed to update stored expiration: {e}")

                return {
                    "success": True,
                    "expiration": new_exp
                }

            else:
                error_data = response.json()
                error = error_data.get('error', {})
                error_code = error.get('code', 'Unknown')
                error_msg = error.get('message', response.text)

                logger.error(f"❌ [Microsoft] Subscription renewal failed: {error_code} - {error_msg}")

                # If subscription doesn't exist anymore, mark as inactive
                if error_code in ['InvalidRequest', 'ItemNotFound']:
                    if supabase_client:
                        try:
                            supabase_client.table('push_subscriptions')\
                                .update({'is_active': False})\
                                .eq('channel_id', subscription_id)\
                                .execute()
                        except Exception:
                            pass

                return {
                    "success": False,
                    "error": error_msg,
                    "error_code": error_code
                }

        except Exception as e:
            logger.error(f"❌ [Microsoft] Subscription renewal error: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def delete_subscription(
        self,
        access_token: str,
        subscription_id: str,
        supabase_client=None
    ) -> Dict[str, Any]:
        """
        Delete a Microsoft Graph subscription.

        Args:
            access_token: Valid Microsoft access token
            subscription_id: The subscription ID to delete
            supabase_client: Optional Supabase client to update database

        Returns:
            Dict with success status
        """
        logger.info(f"🗑️ [Microsoft] Deleting subscription {subscription_id[:20]}...")

        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.delete(
                    f"{GRAPH_API_URL}/subscriptions/{subscription_id}",
                    headers={"Authorization": f"Bearer {access_token}"}
                )

            # 204 No Content = success, 404 = already deleted
            if response.status_code in [204, 404]:
                logger.info("✅ [Microsoft] Subscription deleted")

                # Mark as inactive in database
                if supabase_client:
                    try:
                        supabase_client.table('push_subscriptions')\
                            .update({'is_active': False})\
                            .eq('channel_id', subscription_id)\
                            .execute()
                    except Exception as e:
                        logger.warning(f"⚠️ [Microsoft] Failed to update database: {e}")

                return {"success": True}

            else:
                error_data = response.json() if response.text else {}
                error_msg = error_data.get('error', {}).get('message', response.text)
                logger.error(f"❌ [Microsoft] Delete failed: {error_msg}")
                return {"success": False, "error": error_msg}

        except Exception as e:
            logger.error(f"❌ [Microsoft] Delete error: {e}")
            return {"success": False, "error": str(e)}

    def validate_notification(
        self,
        request_data: Dict[str, Any],
        subscription_data: Dict[str, Any]
    ) -> bool:
        """
        Validate an incoming Microsoft Graph notification.

        Verifies that the clientState in the notification matches
        what we stored during subscription creation.
        """
        notification_client_state = request_data.get('clientState')
        stored_client_state = subscription_data.get('client_state')

        if not stored_client_state or not notification_client_state:
            logger.warning("[Microsoft] Missing client_state on notification or subscription")
            return False

        if not hmac.compare_digest(
            str(notification_client_state).encode("utf-8"),
            str(stored_client_state).encode("utf-8"),
        ):
            logger.warning("[Microsoft] clientState mismatch - possible spoofed notification")
            return False

        return True

    def process_notification(
        self,
        notification: Dict[str, Any],
        connection_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process a Microsoft Graph change notification.

        Determines what changed and triggers appropriate sync.

        Notifications contain:
        - changeType: created, updated, deleted
        - resource: path to changed resource (e.g., "me/messages/{id}")
        - resourceData: info about the changed resource
        - subscriptionId: our subscription ID

        Args:
            notification: The notification from Microsoft
            connection_data: The ext_connection data

        Returns:
            Dict with success and sync result
        """
        change_type = notification.get('changeType')
        resource = notification.get('resource', '')
        subscription_id = notification.get('subscriptionId')

        logger.info(f"📬 [Microsoft] Processing notification: {change_type} on {resource[:50]}... (sub: {subscription_id[:20] if subscription_id else 'N/A'})")

        connection_id = connection_data.get('id')
        user_id = connection_data.get('user_id')

        # Determine resource type from resource path (case-insensitive)
        # Microsoft sends paths like "Users/{id}/Events/{id}" or "me/messages/{id}"
        resource_lower = resource.lower()
        if 'messages' in resource_lower or 'mailfolders' in resource_lower:
            resource_type = 'mail'
        elif 'events' in resource_lower or 'calendar' in resource_lower:
            resource_type = 'calendar'
        else:
            logger.warning(f"⚠️ [Microsoft] Unknown resource type in: {resource}")
            return {"success": False, "error": "Unknown resource type"}

        try:
            if resource_type == 'mail':
                # Trigger email delta sync
                from api.services.syncs.sync_outlook import sync_outlook_incremental
                from api.services.microsoft.microsoft_oauth_provider import get_valid_microsoft_credentials
                from lib.supabase_client import get_service_role_client

                service_supabase = get_service_role_client()

                # Get fresh access token
                access_token = get_valid_microsoft_credentials(connection_data, service_supabase)

                # Run incremental sync
                sync_result = sync_outlook_incremental(
                    user_id=user_id,
                    connection_id=connection_id,
                    connection_data={**connection_data, 'access_token': access_token}
                )

                logger.info(f"✅ [Microsoft] Mail sync complete: {sync_result.get('new_emails', 0)} new, "
                           f"{sync_result.get('updated_emails', 0)} updated, "
                           f"{sync_result.get('deleted_emails', 0)} deleted")

                return {
                    "success": True,
                    "resource_type": resource_type,
                    "sync_result": sync_result
                }

            elif resource_type == 'calendar':
                # Trigger calendar delta sync
                from api.services.syncs.sync_outlook_calendar import sync_outlook_calendar_incremental
                from api.services.microsoft.microsoft_oauth_provider import get_valid_microsoft_credentials
                from lib.supabase_client import get_service_role_client

                service_supabase = get_service_role_client()

                # Get fresh access token
                access_token = get_valid_microsoft_credentials(connection_data, service_supabase)

                # Run incremental sync
                sync_result = sync_outlook_calendar_incremental(
                    user_id=user_id,
                    connection_id=connection_id,
                    connection_data={**connection_data, 'access_token': access_token}
                )

                logger.info(f"✅ [Microsoft] Calendar sync complete: {sync_result.get('new_events', 0)} new, "
                           f"{sync_result.get('updated_events', 0)} updated")

                return {
                    "success": True,
                    "resource_type": resource_type,
                    "sync_result": sync_result
                }

        except Exception as e:
            logger.error(f"❌ [Microsoft] Notification processing error: {e}")
            import traceback
            logger.error(f"❌ [Microsoft] Traceback: {traceback.format_exc()}")
            return {
                "success": False,
                "error": str(e)
            }

    @staticmethod
    def generate_client_state() -> str:
        """
        Generate a secure random client_state for subscription validation.

        This value is sent during subscription creation and must be
        verified on each incoming notification.
        """
        return secrets.token_urlsafe(32)

    @staticmethod
    def build_subscription_payload(
        resource: str,
        webhook_url: str,
        client_state: str,
        expiration_minutes: int
    ) -> Dict[str, Any]:
        """
        Build the payload for creating a Microsoft Graph subscription.

        Args:
            resource: Resource to watch (e.g., "me/mailFolders/inbox/messages")
            webhook_url: URL to receive notifications
            client_state: Secret for validation
            expiration_minutes: Minutes until subscription expires

        Returns:
            Dict ready to POST to /subscriptions
        """
        expiration = datetime.now(timezone.utc) + timedelta(minutes=expiration_minutes)

        return {
            "changeType": "created,updated,deleted",
            "notificationUrl": webhook_url,
            "resource": resource,
            "expirationDateTime": expiration.isoformat().replace('+00:00', 'Z'),
            "clientState": client_state,
        }


# Resource paths for Microsoft Graph subscriptions
MICROSOFT_RESOURCES = {
    "mail": "me/mailFolders/inbox/messages",
    "calendar": "me/events",
    "contacts": "me/contacts",  # For future use
}


# Convenience function for creating subscriptions (async)
async def create_microsoft_subscription(
    access_token: str,
    resource_type: str,
    connection_id: str,
    user_id: str,
    supabase_client=None
) -> Dict[str, Any]:
    """
    Convenience function to create a Microsoft Graph subscription.

    IMPORTANT: This is async to prevent blocking the event loop while
    Microsoft validates our webhook endpoint.

    Args:
        access_token: Valid Microsoft access token
        resource_type: 'mail' or 'calendar'
        connection_id: The ext_connection ID
        user_id: The user ID
        supabase_client: Supabase client for storing subscription

    Returns:
        Dict with success, subscription_id, etc.
    """
    provider = MicrosoftWebhookProvider()
    return await provider.create_subscription(
        access_token=access_token,
        resource_type=resource_type,
        connection_id=connection_id,
        user_id=user_id,
        supabase_client=supabase_client
    )


def renew_microsoft_subscription(
    access_token: str,
    subscription_id: str,
    resource_type: str,
    supabase_client=None
) -> Dict[str, Any]:
    """
    Convenience function to renew a Microsoft Graph subscription.
    """
    provider = MicrosoftWebhookProvider()
    return provider.renew_subscription(
        access_token=access_token,
        subscription_id=subscription_id,
        resource_type=resource_type,
        supabase_client=supabase_client
    )
