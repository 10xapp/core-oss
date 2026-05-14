"""Helpers for Google webhook provenance checks."""

from __future__ import annotations

import hashlib
import hmac
from typing import Optional

from api.config import settings


def build_google_calendar_channel_token(
    connection_id: str,
    channel_id: str,
) -> Optional[str]:
    """Build a stable HMAC token for Google Calendar push channels."""
    secret = settings.google_calendar_webhook_secret.strip()
    if not secret:
        return None

    payload = f"{connection_id}:{channel_id}"
    signature = hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"v1:{payload}:{signature}"


def verify_google_calendar_channel_token(
    connection_id: str,
    channel_id: str,
    provided_token: Optional[str],
) -> bool:
    """Return True when the received Calendar channel token matches."""
    expected_token = build_google_calendar_channel_token(connection_id, channel_id)
    if expected_token is None:
        return True
    if not provided_token:
        return False
    return hmac.compare_digest(provided_token, expected_token)
