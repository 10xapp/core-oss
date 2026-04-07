"""
Webhooks router - Receives push notifications from external services
Thin layer that handles HTTP concerns and delegates to services.
"""
from fastapi import APIRouter, Request, Header, HTTPException, Query, Response, status
from fastapi.responses import PlainTextResponse
import asyncio
import base64
from collections import OrderedDict
import json
import logging
import threading
import time
from typing import Dict, Optional
from starlette.requests import ClientDisconnect

from api.config import settings
from api.schemas import HealthResponse
from api.services.syncs.sync_dispatcher import mark_stream_dirty
from api.services.syncs.sync_state_store import SYNC_KIND_CALENDAR, SYNC_KIND_EMAIL
from lib.google_webhook_security import verify_google_calendar_channel_token
from lib.supabase_client import get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens

from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])
_PUBSUB_JWT_CACHE: Dict[str, tuple[dict, float]] = {}
_PUBSUB_JWT_CACHE_LOCK = threading.Lock()
_PUBSUB_JWT_CACHE_MAX_ENTRIES = 4_096
_GMAIL_INGRESS_DEDUP_TTL_SECONDS = 120.0
_GMAIL_INGRESS_DEDUP_MAX_ENTRIES = 20_000
_GMAIL_INGRESS_DEDUP_CACHE: "OrderedDict[str, float]" = OrderedDict()
_GMAIL_INGRESS_DEDUP_LOCK = threading.Lock()


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


def _extract_bearer_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


def _gmail_ingress_dedup_key(email_address: str, history_id: str) -> str:
    return f"{email_address}\n{history_id}"


def _prune_gmail_ingress_dedup_cache(now: float) -> None:
    while _GMAIL_INGRESS_DEDUP_CACHE:
        first_key = next(iter(_GMAIL_INGRESS_DEDUP_CACHE))
        expires_at = _GMAIL_INGRESS_DEDUP_CACHE[first_key]
        if expires_at > now and len(_GMAIL_INGRESS_DEDUP_CACHE) <= _GMAIL_INGRESS_DEDUP_MAX_ENTRIES:
            return
        _GMAIL_INGRESS_DEDUP_CACHE.popitem(last=False)


def _was_recent_successful_gmail_notification(email_address: str, history_id: str) -> bool:
    now = time.monotonic()
    key = _gmail_ingress_dedup_key(email_address, history_id)
    with _GMAIL_INGRESS_DEDUP_LOCK:
        _prune_gmail_ingress_dedup_cache(now)
        expires_at = _GMAIL_INGRESS_DEDUP_CACHE.get(key)
        if expires_at is None:
            return False
        if expires_at <= now:
            _GMAIL_INGRESS_DEDUP_CACHE.pop(key, None)
            return False
        _GMAIL_INGRESS_DEDUP_CACHE.move_to_end(key)
        return True


def _remember_successful_gmail_notification(email_address: str, history_id: str) -> None:
    now = time.monotonic()
    key = _gmail_ingress_dedup_key(email_address, history_id)
    with _GMAIL_INGRESS_DEDUP_LOCK:
        _GMAIL_INGRESS_DEDUP_CACHE[key] = now + _GMAIL_INGRESS_DEDUP_TTL_SECONDS
        _GMAIL_INGRESS_DEDUP_CACHE.move_to_end(key)
        _prune_gmail_ingress_dedup_cache(now)


def _verify_google_pubsub_auth(authorization: Optional[str]) -> None:
    """Verify Pub/Sub authenticated push JWT when the deployment requires it."""
    expected_email = settings.google_pubsub_push_service_account_email.strip()
    configured_audience = settings.google_pubsub_push_audience.strip()

    if not expected_email and not configured_audience:
        return

    expected_audience = settings.resolved_google_pubsub_push_audience.strip()
    encoded_token = _extract_bearer_token(authorization)
    if not encoded_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Pub/Sub authorization token",
        )

    now = time.time()
    with _PUBSUB_JWT_CACHE_LOCK:
        cached_claims = _PUBSUB_JWT_CACHE.get(encoded_token)
    if cached_claims and cached_claims[1] > now + 30:
        claims = cached_claims[0]
    else:
        from google.auth import exceptions as google_auth_exceptions
        from google.auth.transport.requests import Request as GoogleAuthRequest
        from google.oauth2 import id_token as google_id_token

        try:
            claims = google_id_token.verify_oauth2_token(
                encoded_token,
                GoogleAuthRequest(),
                audience=expected_audience,
                clock_skew_in_seconds=3600,
            )
        except (ValueError, google_auth_exceptions.GoogleAuthError) as exc:
            logger.warning("Pub/Sub JWT verification failed: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid Pub/Sub authorization token",
            ) from exc
        expiry = float(claims.get("exp") or 0)
        with _PUBSUB_JWT_CACHE_LOCK:
            _PUBSUB_JWT_CACHE[encoded_token] = (claims, expiry)
            expired_tokens = [
                token
                for token, (_, token_expiry) in _PUBSUB_JWT_CACHE.items()
                if token_expiry <= now
            ]
            for token in expired_tokens:
                _PUBSUB_JWT_CACHE.pop(token, None)
            # LRU eviction if cache exceeds max size
            while len(_PUBSUB_JWT_CACHE) > _PUBSUB_JWT_CACHE_MAX_ENTRIES:
                _PUBSUB_JWT_CACHE.pop(next(iter(_PUBSUB_JWT_CACHE)))

    if claims.get("email_verified") is False:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Pub/Sub token email is not verified",
        )

    if expected_email and claims.get("email") != expected_email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unexpected Pub/Sub service account",
        )

@router.post("/gmail", response_model=WebhookProcessResponse)
async def gmail_webhook(
    request: Request,
    authorization: Optional[str] = Header(None),
):
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
    
    Returns 200 on success or non-actionable input. Returns 503 on transient
    infrastructure failures (e.g. Supabase down) so Pub/Sub retries delivery.
    """
    try:
        # Buffer the small Pub/Sub payload before JWT verification so a slow
        # cert fetch or claim check does not turn a later body read into
        # ClientDisconnect noise.
        raw_body = await request.body()
    except ClientDisconnect:
        logger.warning("Gmail webhook client disconnected before request body was read")
        return {"status": "error", "message": "Client disconnected before body read"}

    # Pub/Sub envelopes are ~200 bytes. Reject anything unreasonably large
    # before spending time on auth verification.
    if len(raw_body) > 65_536:
        return {"status": "error", "message": "Payload too large"}

    # Run JWT verification in a thread pool so the blocking Google cert
    # fetch does not freeze the event loop and starve concurrent requests.
    await asyncio.to_thread(_verify_google_pubsub_auth, authorization)

    try:
        # Parse Pub/Sub message format
        body = json.loads(raw_body)
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
            logger.debug(
                "📩 Decoded Gmail payload for %s historyId=%s",
                payload.get("emailAddress"),
                payload.get("historyId"),
            )
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

        if _was_recent_successful_gmail_notification(email_address, history_id):
            logger.debug(
                "Skipping duplicate Gmail notification for %s historyId=%s before DB write",
                email_address,
                history_id,
            )
            return {"status": "ok", "message": "Accepted duplicate notification"}

        service_supabase = get_service_role_client()
        conn_result = service_supabase.table('ext_connections')\
            .select('id')\
            .eq('provider_email', email_address)\
            .eq('provider', 'google')\
            .eq('is_active', True)\
            .limit(1)\
            .execute()

        if not conn_result.data:
            logger.debug(f"No active Gmail connection found for {email_address}; acknowledging webhook")
            return {"status": "ok", "message": "Accepted without matching connection"}

        connection_id = conn_result.data[0]['id']
        marked = mark_stream_dirty(
            service_supabase,
            connection_id,
            "google",
            SYNC_KIND_EMAIL,
            latest_seen_cursor=history_id,
            priority=100,
            metadata={
                "source": "gmail-webhook",
                "email_address": email_address,
                "history_id": history_id,
            },
        )
        if marked:
            _remember_successful_gmail_notification(email_address, history_id)
        return {"status": "ok", "message": "Accepted for async sync"}

    except Exception as e:
        logger.exception(
            "❌ Error handling Gmail webhook after auth; acknowledging to avoid "
            "global Pub/Sub push backoff: %s",
            str(e),
        )
        # Gmail Pub/Sub push backoff is global to the subscription. Once the
        # request is authenticated and the payload parsed, prefer an ACK and
        # rely on reconciliation as the safety net instead of slowing delivery
        # for every mailbox on transient dirty-mark failures.
        return {
            "status": "ok",
            "message": "Accepted without dirty mark; reconciliation will recover",
        }


@router.post("/calendar", response_model=WebhookProcessResponse)
async def calendar_webhook(
    request: Request,
    x_goog_channel_id: Optional[str] = Header(None),
    x_goog_channel_token: Optional[str] = Header(None),
    x_goog_resource_id: Optional[str] = Header(None),
    x_goog_resource_state: Optional[str] = Header(None),
    x_goog_message_number: Optional[str] = Header(None)
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
    
    Returns 200 on success. Returns 503 on transient infrastructure failures
    so Google retries delivery instead of silently losing the notification.
    """
    try:
        logger.debug(f"Calendar webhook received: channel={x_goog_channel_id}, state={x_goog_resource_state}")

        if not x_goog_channel_id:
            return {"status": "ok", "message": "Accepted without channel id"}

        service_supabase = get_service_role_client()
        sub_result = service_supabase.table('push_subscriptions')\
            .select('ext_connection_id, resource_id')\
            .eq('channel_id', x_goog_channel_id)\
            .eq('is_active', True)\
            .limit(1)\
            .execute()

        if not sub_result.data:
            logger.debug(f"No active Calendar subscription found for channel {x_goog_channel_id}")
            return {"status": "ok", "message": "Accepted without matching subscription"}

        subscription = sub_result.data[0]
        connection_id = subscription['ext_connection_id']
        stored_resource_id = subscription.get('resource_id')
        if stored_resource_id and x_goog_resource_id and stored_resource_id != x_goog_resource_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unexpected Google Calendar resource id",
            )
        if not verify_google_calendar_channel_token(
            connection_id,
            x_goog_channel_id,
            x_goog_channel_token,
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid Google Calendar channel token",
            )

        mark_stream_dirty(
            service_supabase,
            connection_id,
            "google",
            SYNC_KIND_CALENDAR,
            priority=100,
            metadata={
                "source": "google-calendar-webhook",
                "channel_id": x_goog_channel_id,
                "resource_id": x_goog_resource_id,
                "resource_state": x_goog_resource_state,
                "message_number": x_goog_message_number,
            },
        )
        return {"status": "ok", "message": "Accepted for async sync"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Error handling Calendar webhook: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Transient error processing webhook",
        )


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
    logger.debug("[Microsoft] Subscription validation request received")
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
    to acknowledge receipt. Returns 503 on transient infrastructure failures
    so Microsoft retries delivery.
    """
    try:
        # Microsoft may send validation as POST with ?validationToken= query param
        validation_token = request.query_params.get("validationToken")
        if validation_token:
            logger.debug("[Microsoft] Subscription validation via POST request")
            return PlainTextResponse(
                content=validation_token,
                status_code=200,
                media_type="text/plain"
            )

        body = await request.json()

        logger.debug("[Microsoft] Webhook notification received")

        notifications = body.get("value", [])
        if not notifications:
            logger.warning("⚠️ [Microsoft] Empty notification payload")
            return Response(status_code=202)

        logger.debug(f"[Microsoft] Processing {len(notifications)} notification(s)")

        results = []
        for notification in notifications:
            results.append(await _mark_microsoft_notification_dirty(notification))

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
        logger.exception(f"❌ [Microsoft] Webhook error: {e}")
        return Response(status_code=503)


async def _mark_microsoft_notification_dirty(notification: dict) -> dict:
    """
    Validate a Microsoft notification and mark the corresponding stream dirty.
    """
    from lib.supabase_client import get_service_role_client
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
            sync_kind = SYNC_KIND_CALENDAR
        else:
            # Default to mail sync (covers /messages, /mailFolders, etc.)
            job_type = "sync-outlook"
            sync_kind = SYNC_KIND_EMAIL

        if mark_stream_dirty(
            service_supabase,
            connection_id,
            "microsoft",
            sync_kind,
            priority=100,
            metadata={
                "source": "microsoft-webhook",
                "resource": resource,
                "subscription_id": subscription_id,
            },
        ):
            return {"success": True, "marked": job_type}

        logger.warning(f"⚠️ [Microsoft] Failed to mark dirty for {connection_id[:8]}...")
        return {"success": False, "error": "mark dirty failed"}

    except Exception as e:
        logger.warning(f"⚠️ [Microsoft] Failed to mark notification dirty: {e}")
        return {"success": False, "error": str(e)}


async def process_microsoft_notification(notification: dict) -> dict:
    """
    Process a single Microsoft Graph notification.

    Verifies clientState, looks up connection, and triggers sync.
    """
    from lib.supabase_client import get_service_role_client
    from api.services.microsoft.microsoft_webhook_provider import MicrosoftWebhookProvider

    subscription_id = notification.get('subscriptionId')
    change_type = notification.get('changeType')
    resource = notification.get('resource', '')

    logger.debug(f"[Microsoft] Notification: {change_type} on {resource[:50]}...")

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
