"""
Verification of Google Cloud Pub/Sub push notifications.

Pub/Sub push subscriptions configured with a service-account auth send an
Authorization: Bearer <ID token> header on every delivery. We verify that
token against Google's public keys and the configured service account.
"""
import logging
from typing import Optional

from fastapi import Request
from google.auth.transport import requests as gauth_requests
from google.oauth2 import id_token

from api.config import settings

logger = logging.getLogger(__name__)

_request_adapter = gauth_requests.Request()


class PubSubAuthError(Exception):
    """Raised when a Pub/Sub push request cannot be authenticated."""


def _expected_audience(request: Request) -> str:
    if settings.pubsub_push_audience:
        return settings.pubsub_push_audience
    if settings.webhook_base_url:
        return settings.webhook_base_url.rstrip("/") + request.url.path
    # Last resort: use the absolute URL the proxy forwarded.
    return str(request.url).split("?", 1)[0]


def verify_pubsub_push(request: Request) -> None:
    """
    Validate a Pub/Sub push request. Raises PubSubAuthError on any failure.

    Caller is responsible for translating that into the appropriate response
    (Pub/Sub semantics: return HTTP 200 + log on bad tokens to avoid retry
    storms; non-200 makes Pub/Sub keep redelivering).
    """
    sa = settings.pubsub_push_service_account
    if not sa:
        # Verification not configured. Treat as a hard failure so we don't
        # silently fall back to "anyone can post" if the env is missing.
        raise PubSubAuthError("pubsub_push_service_account is not configured")

    auth_header: Optional[str] = request.headers.get("Authorization") or request.headers.get("authorization")
    if not auth_header or not auth_header.lower().startswith("bearer "):
        raise PubSubAuthError("missing bearer token")

    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise PubSubAuthError("empty bearer token")

    audience = _expected_audience(request)

    try:
        claims = id_token.verify_oauth2_token(token, _request_adapter, audience=audience)
    except Exception as exc:
        raise PubSubAuthError(f"token verification failed: {exc}") from exc

    if claims.get("email") != sa:
        raise PubSubAuthError(f"unexpected token issuer: {claims.get('email')!r}")

    if claims.get("email_verified") is not True:
        raise PubSubAuthError("token issuer email is not verified")
