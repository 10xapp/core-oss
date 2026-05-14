"""
Retry helpers for transient Google API and OAuth errors.

google-auth's credentials.refresh() retries on HTTP status codes (500, 503, 429)
but does NOT retry on transport-level errors (Server disconnected, SSL EOF,
ConnectionError). This module fills that gap.

For Google API .execute() calls, use the GOOGLE_API_NUM_RETRIES constant
which enables the built-in retry in google-api-python-client.
"""
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# Pass to every Google API .execute(num_retries=GOOGLE_API_NUM_RETRIES)
# Handles 5xx, 429, SSL errors, ConnectionError with exponential backoff.
GOOGLE_API_NUM_RETRIES = 3

_TRANSIENT_ERROR_KEYWORDS = (
    "server disconnected",
    "remotedisconnected",
    "remoteprotocolerror",
    "connectionterminated",
    "connection aborted",
    "connection reset",
    "connectionerror",
    "ssl",
    "eof occurred",
    "broken pipe",
    "timed out",
)


def _is_transient_transport_error(exc: Exception) -> bool:
    """Check if an exception is a transient transport/connection error."""
    msg = str(exc).lower()
    return any(keyword in msg for keyword in _TRANSIENT_ERROR_KEYWORDS)


def refresh_credentials(
    credentials: Any,
    request: Any,
    *,
    max_retries: int = 2,
    context: str = "",
) -> None:
    """
    Refresh Google OAuth credentials with retry on transient transport errors.

    google-auth retries on HTTP 500/503/429 internally, but transport-level
    errors (Server disconnected, SSL EOF, etc.) propagate as raw exceptions.
    This wrapper retries those.

    Args:
        credentials: google.oauth2.credentials.Credentials instance
        request: google.auth.transport.requests.Request instance
        max_retries: Number of retries on transient errors (default 2 = 3 total attempts)
        context: Optional context string for log messages (e.g. connection ID)

    Raises:
        The original exception if all retries are exhausted or error is not transient.
    """
    for attempt in range(max_retries + 1):
        try:
            credentials.refresh(request)
            return
        except Exception as exc:
            if attempt < max_retries and _is_transient_transport_error(exc):
                wait = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s
                logger.warning(
                    f"⚠️ Transient error refreshing credentials{f' for {context}' if context else ''} "
                    f"(attempt {attempt + 1}/{max_retries + 1}): {exc} — retrying in {wait}s"
                )
                time.sleep(wait)
                continue
            raise
