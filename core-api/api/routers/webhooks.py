"""
Webhooks router - Receives push notifications from external services
Thin layer that handles HTTP concerns and delegates to services.
"""
from fastapi import APIRouter, HTTPException, Request, Header, Query, Response
from fastapi.responses import PlainTextResponse
from typing import Optional
import asyncio
import hmac
import logging
import json
import base64
from datetime import datetime, timezone

from api.config import settings
from api.services.webhooks import (
    process_gmail_notification,
    process_calendar_notification
)
from lib.token_encryption import decrypt_ext_connection_tokens

from pydantic import BaseModel
from api.schemas import HealthResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


def _verify_google_webhook_token(token: Optional[str]) -> bool:
    """Verify the token query parameter matches our configured secret.

    Google Pub/Sub sends a ?token= param on push URLs and Calendar
    sends the token set during watch creation in X-Goog-Channel-Token.
    We verify it matches GOOGLE_WEBHOOK_TOKEN to prevent forged requests.
    """
    expected = settings.google_webhook_token
    if not expected:
        logger.warning("GOOGLE_WEBHOOK_TOKEN not configured — rejecting Google webhook")
        return False
    if not token:
        return False
    return hmac.compare_digest(token, expected)


# ============================================================================
# Response Models
# ============================================================================

class WebhookProcessResponse(BaseModel):
    """Response from webhook processing endpoints."""
    status: str
    message: Optional[str] = None

    class Config:
        extra = "allow"


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/gmail", response_model=WebhookProcessResponse)
async def gmail_webhook(request: Request, token: Optional[str] = Query(None)):
    """
    Receive Gmail push notifications from Google Cloud Pub/Sub.

    Gmail notifications come via Pub/Sub in this format:
    {
        "message": {
            "data": "base64-encoded-json",
            "messageId": "...",
            "publishTime": "..."
        },
        "subscription": "..."
    }

    The decoded data contains:
    {
        "emailAddress": "user@example.com",
        "historyId": "12345"
    }

    The push URL must include ?token=<GOOGLE_WEBHOOK_TOKEN> for verification.

    Returns 200 immediately to acknowledge receipt (required by Pub/Sub).
    """
    # Verify BEFORE the try/except that returns 200 on all errors
    if not _verify_google_webhook_token(token):
        logger.warning("Gmail webhook rejected: invalid or missing token")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # Parse Pub/Sub message format
        body = await request.json()

        logger.info("Gmail webhook received")
        logger.debug(f"Body keys: {body.keys() if body else 'empty'}")
        
        # Validate Pub/Sub message format
        if not body.get('message') or not body['message'].get('data'):
            logger.error(f"❌ Invalid Pub/Sub message format: {body}")
            return {"status": "error", "message": "Invalid Pub/Sub format"}
        
        # Decode base64 data
        message_data = body['message']['data']
        try:
            decoded_data = base64.b64decode(message_data).decode('utf-8')
            payload = json.loads(decoded_data)
            logger.info(f"📩 Decoded payload: {payload}")
        except Exception as e:
            logger.error(f"❌ Failed to decode message data: {str(e)}")
            return {"status": "error", "message": "Failed to decode message"}
        
        # Extract notification data
        email_address = payload.get('emailAddress')
        raw_history_id = payload.get('historyId')
        history_id = str(raw_history_id) if raw_history_id is not None else None

        if not email_address or not history_id:
            logger.error(f"❌ Missing required fields in payload: {payload}")
            return {"status": "error", "message": "Missing required fields"}

        # Try to enqueue via QStash for async processing
        try:
            from lib.queue import queue_client
            if queue_client.available:
                from lib.supabase_client import get_service_role_client
                service_supabase = get_service_role_client()
                # Look up connection_id from provider_email
                conn_result = service_supabase.table('ext_connections')\
                    .select('id')\
                    .eq('provider_email', email_address)\
                    .eq('provider', 'google')\
                    .eq('is_active', True)\
                    .limit(1)\
                    .execute()
                if conn_result.data:
                    connection_id = conn_result.data[0]['id']
                    dedup_id = f"wh-sync-gmail-{connection_id}-{history_id}"
                    if queue_client.enqueue_sync_for_connection(
                        connection_id, "sync-gmail",
                        extra={
                            "history_id": history_id,
                            "email_address": email_address,
                        },
                        dedup_id=dedup_id,
                    ):
                        return {"status": "ok", "message": "Enqueued to worker"}
                    # enqueue returned False — fall through to inline
        except Exception as eq_err:
            logger.warning(f"⚠️ Queue enqueue failed, falling back to inline: {eq_err}")

        # Fallback: process inline (existing behavior)
        try:
            result = await asyncio.to_thread(process_gmail_notification, email_address, history_id)
            return {"status": "ok", **result}
        except Exception as e:
            logger.error(f"❌ Error processing Gmail notification: {str(e)}")
            logger.exception("Full traceback:")
            # Still return 200 to Pub/Sub - cron job will catch any missed emails
            return {"status": "ok", "message": "Notification received with errors"}
        
    except Exception as e:
        logger.error(f"❌ Error handling Gmail webhook: {str(e)}")
        logger.exception("Full traceback:")
        # Always return 200 to Pub/Sub, even on error
        # We don't want Pub/Sub to think our endpoint is down
        return {"status": "error", "message": str(e)}


@router.post("/calendar", response_model=WebhookProcessResponse)
async def calendar_webhook(
    request: Request,
    x_goog_channel_id: Optional[str] = Header(None),
    x_goog_resource_id: Optional[str] = Header(None),
    x_goog_resource_state: Optional[str] = Header(None),
    x_goog_message_number: Optional[str] = Header(None),
    x_goog_channel_token: Optional[str] = Header(None),
):
    """
    Receive Google Calendar push notifications.

    Google sends notifications when calendar events change.
    We use sync tokens to fetch only what changed.

    Headers from Google:
    - X-Goog-Channel-ID: The UUID of the notification channel
    - X-Goog-Resource-ID: Opaque ID for the watched resource
    - X-Goog-Resource-State: "sync" (initial) or "exists" (change notification)
    - X-Goog-Message-Number: Sequential message number
    - X-Goog-Channel-Token: Verification token set during watch creation

    Returns 200 immediately to acknowledge receipt (required by Google).
    """
    # Verify BEFORE the try/except that returns 200 on all errors
    if not _verify_google_webhook_token(x_goog_channel_token):
        logger.warning(f"Calendar webhook rejected: invalid or missing token (channel={x_goog_channel_id})")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        logger.info(f"Calendar webhook received: channel={x_goog_channel_id}, state={x_goog_resource_state}")

        # Try to enqueue via QStash for async processing
        try:
            from lib.queue import queue_client
            if queue_client.available and x_goog_channel_id:
                from lib.supabase_client import get_service_role_client
                service_supabase = get_service_role_client()
                # Look up connection_id from push_subscriptions by channel_id
                sub_result = service_supabase.table('push_subscriptions')\
                    .select('ext_connection_id')\
                    .eq('channel_id', x_goog_channel_id)\
                    .eq('is_active', True)\
                    .limit(1)\
                    .execute()
                if sub_result.data:
                    connection_id = sub_result.data[0]['ext_connection_id']
                    dedup_suffix = x_goog_message_number or datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
                    dedup_id = f"wh-sync-calendar-{connection_id}-{dedup_suffix}"
                    if queue_client.enqueue_sync_for_connection(
                        connection_id,
                        "sync-calendar",
                        extra={
                            "channel_id": x_goog_channel_id,
                            "resource_state": x_goog_resource_state,
                            "message_number": x_goog_message_number,
                        },
                        dedup_id=dedup_id,
                    ):
                        return {"status": "ok", "message": "Enqueued to worker"}
                    # enqueue returned False — fall through to inline
        except Exception as eq_err:
            logger.warning(f"⚠️ Queue enqueue failed, falling back to inline: {eq_err}")

        # Fallback: process inline (existing behavior)
        try:
            result = await asyncio.to_thread(
                process_calendar_notification,
                channel_id=x_goog_channel_id,
                resource_state=x_goog_resource_state
            )
            return {"status": "ok", **result}
        except Exception as e:
            logger.error(f"❌ Error processing Calendar notification: {str(e)}")
            logger.exception("Full traceback:")
            # Don't fail the webhook - cron will catch missed events
            return {"status": "ok", "message": "Notification received with errors"}
        
    except Exception as e:
        logger.error(f"❌ Error handling Calendar webhook: {str(e)}")
        # Always return 200 to Google
        return {"status": "error", "message": str(e)}


@router.get("/gmail/verify", response_model=HealthResponse)
async def verify_gmail_webhook():
    """
    Health check endpoint for Gmail webhook.
    Used to verify the endpoint is accessible.
    """
    return {
        "status": "healthy",
        "service": "gmail-webhook",
        "message": "Gmail webhook endpoint is ready to receive notifications"
    }


@router.get("/calendar/verify", response_model=HealthResponse)
async def verify_calendar_webhook():
    """
    Health check endpoint for Calendar webhook.
    Used to verify the endpoint is accessible.
    """
    return {
        "status": "healthy",
        "service": "calendar-webhook",
        "message": "Calendar webhook endpoint is ready to receive notifications"
    }


# ============== Microsoft Graph Webhooks ==============

@router.get("/microsoft", response_class=PlainTextResponse, responses={
    200: {"description": "Validation token echo", "content": {"text/plain": {"schema": {"type": "string"}}}},
})
async def microsoft_webhook_validation(
    validationToken: str = Query(..., description="Validation token from Microsoft Graph subscription creation"),
):
    """
    Microsoft Graph subscription validation endpoint.

    When creating a subscription, Microsoft sends a GET request with a
    validationToken query parameter. We MUST return the token as plain text
    within 10 seconds or subscription creation fails.
    """
    logger.info("📡 [Microsoft] Subscription validation request received")
    logger.debug(f"📡 [Microsoft] Validation token: {validationToken[:20]}...")
    return PlainTextResponse(
        content=validationToken,
        status_code=200,
        media_type="text/plain"
    )


@router.post("/microsoft", status_code=202, responses={
    202: {"description": "Notification acknowledged", "content": {"application/json": {"schema": {"type": "object", "properties": {"status": {"type": "string"}, "processed": {"type": "integer"}}}}}},
})
async def microsoft_webhook_notification(
    request: Request,
):
    """
    Microsoft Graph change notification endpoint.

    Microsoft sends POST requests when subscribed resources change.
    Contains an array of notifications in body.value, each with:
    subscriptionId, changeType, resource, clientState.

    We verify clientState matches what we stored and return 202 Accepted
    to acknowledge receipt (async processing).
    """
    try:
        # Microsoft may send validation as POST with ?validationToken= query param
        validation_token = request.query_params.get("validationToken")
        if validation_token:
            logger.info("📡 [Microsoft] Subscription validation via POST request")
            return PlainTextResponse(
                content=validation_token,
                status_code=200,
                media_type="text/plain"
            )

        body = await request.json()

        logger.info("📬 [Microsoft] Webhook notification received")

        notifications = body.get("value", [])
        if not notifications:
            logger.warning("⚠️ [Microsoft] Empty notification payload")
            return Response(status_code=202)

        logger.info(f"📬 [Microsoft] Processing {len(notifications)} notification(s)")

        # Try queue-based processing first
        from lib.queue import queue_client
        use_queue = queue_client.available

        results = []
        for notification in notifications:
            if use_queue:
                result = await _enqueue_microsoft_notification(notification)
            else:
                result = await process_microsoft_notification(notification)
            results.append(result)

        # Return 202 Accepted - we've acknowledged the notifications
        return Response(
            status_code=202,
            content=json.dumps({
                "status": "accepted",
                "processed": len(notifications)
            }),
            media_type="application/json"
        )

    except json.JSONDecodeError:
        logger.error("❌ [Microsoft] Invalid JSON in webhook body")
        return Response(status_code=400)
    except Exception as e:
        logger.error(f"❌ [Microsoft] Webhook error: {e}")
        import traceback
        logger.error(f"❌ [Microsoft] Traceback: {traceback.format_exc()}")
        # Still return 202 to acknowledge - cron will catch any missed changes
        return Response(status_code=202)


async def _enqueue_microsoft_notification(notification: dict) -> dict:
    """
    Enqueue a Microsoft notification as a worker job via QStash.

    Validates clientState and resolves connection_id before enqueuing.
    Falls back to inline processing on any error.
    """
    from lib.supabase_client import get_service_role_client
    from lib.queue import queue_client
    from api.services.microsoft.microsoft_webhook_provider import MicrosoftWebhookProvider

    subscription_id = notification.get('subscriptionId')
    resource = notification.get('resource', '')

    if not subscription_id:
        return {"success": False, "error": "No subscriptionId"}

    try:
        service_supabase = get_service_role_client()

        # Look up subscription to verify clientState and get connection
        sub_result = service_supabase.table('push_subscriptions')\
            .select('*, ext_connections(*)')\
            .eq('channel_id', subscription_id)\
            .eq('is_active', True)\
            .limit(1)\
            .execute()

        if not sub_result.data:
            return {"success": False, "error": "Unknown subscription"}

        subscription_data = sub_result.data[0]
        connection_data = decrypt_ext_connection_tokens(
            subscription_data.get('ext_connections')
        )
        if not connection_data:
            return {"success": False, "error": "No connection data"}

        # Verify clientState
        provider = MicrosoftWebhookProvider()
        if not provider.validate_notification(notification, subscription_data):
            return {"success": False, "error": "Invalid clientState"}

        connection_id = connection_data.get('id')
        if not connection_id:
            return {"success": False, "error": "No connection_id"}

        # Determine job type from resource path (case-insensitive — Graph
        # may return PascalCase paths like Users/.../Events/...)
        resource_lower = resource.lower()
        if '/events' in resource_lower or '/calendar' in resource_lower:
            job_type = "sync-outlook-calendar"
        else:
            # Default to mail sync (covers /messages, /mailFolders, etc.)
            job_type = "sync-outlook"

        if queue_client.enqueue_sync_for_connection(connection_id, job_type):
            return {"success": True, "enqueued": job_type}

        # enqueue returned False — fall through to inline
        logger.warning(f"⚠️ [Microsoft] Queue publish failed for {connection_id[:8]}..., falling back to inline")
        return await process_microsoft_notification(notification)

    except Exception as e:
        logger.warning(f"⚠️ [Microsoft] Queue enqueue failed, falling back: {e}")
        return await process_microsoft_notification(notification)


async def process_microsoft_notification(notification: dict) -> dict:
    """
    Process a single Microsoft Graph notification.

    Verifies clientState, looks up connection, and triggers sync.
    """
    from lib.supabase_client import get_service_role_client
    from api.services.microsoft.microsoft_webhook_provider import MicrosoftWebhookProvider

    subscription_id = notification.get('subscriptionId')
    client_state = notification.get('clientState')
    change_type = notification.get('changeType')
    resource = notification.get('resource', '')

    logger.info(f"📬 [Microsoft] Notification: {change_type} on {resource[:50]}... (clientState: {'✓' if client_state else '✗'})")

    if not subscription_id:
        logger.warning("⚠️ [Microsoft] No subscriptionId in notification")
        return {"success": False, "error": "No subscriptionId"}

    try:
        service_supabase = get_service_role_client()

        # Look up the subscription to get connection info and verify clientState
        sub_result = service_supabase.table('push_subscriptions')\
            .select('*, ext_connections(*)')\
            .eq('channel_id', subscription_id)\
            .eq('is_active', True)\
            .limit(1)\
            .execute()

        if not sub_result.data:
            logger.warning(f"⚠️ [Microsoft] Unknown subscription: {subscription_id[:20]}...")
            return {"success": False, "error": "Unknown subscription"}

        subscription_data = sub_result.data[0]
        connection_data = decrypt_ext_connection_tokens(
            subscription_data.get('ext_connections')
        )

        if not connection_data:
            logger.warning("⚠️ [Microsoft] No connection data for subscription")
            return {"success": False, "error": "No connection data"}

        # Verify clientState
        provider = MicrosoftWebhookProvider()
        if not provider.validate_notification(notification, subscription_data):
            logger.warning("⚠️ [Microsoft] clientState validation failed - ignoring")
            return {"success": False, "error": "Invalid clientState"}

        # Process the notification (triggers sync)
        result = provider.process_notification(notification, connection_data)

        return result

    except Exception as e:
        logger.error(f"❌ [Microsoft] Notification processing error: {e}")
        import traceback
        logger.error(f"❌ [Microsoft] Traceback: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}


@router.get("/microsoft/verify", response_model=HealthResponse)
async def verify_microsoft_webhook():
    """
    Health check endpoint for Microsoft webhook.
    Used to verify the endpoint is accessible.
    """
    return {
        "status": "healthy",
        "service": "microsoft-webhook",
        "message": "Microsoft webhook endpoint is ready to receive notifications"
    }
