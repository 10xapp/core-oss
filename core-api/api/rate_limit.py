"""Rate limiting configuration using slowapi with Upstash Redis backend."""
import base64
import json
import hashlib
import logging
from typing import Optional

from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_ipaddr

from api.config import settings

logger = logging.getLogger(__name__)


def _get_client_ip(request: Request) -> str:
    """Get client IP, proxy-aware (reads X-Forwarded-For for Vercel/CDN)."""
    return get_ipaddr(request)


def _extract_user_id_from_jwt(token: str) -> Optional[str]:
    """Extract the 'sub' (user ID) from a JWT without verifying signature.

    This is safe for rate limiting because:
    - We only use it as a rate-limit key, not for authorization
    - An attacker forging a JWT with a different 'sub' would fail auth anyway
    - The key stays stable across token refreshes for the same user
    """
    try:
        # Decode the payload (second segment) without verification
        payload_b64 = token.split(".")[1]
        # Add padding if needed
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return payload.get("sub")
    except Exception:
        return None


def _get_user_or_ip(request: Request) -> str:
    """Rate-limit key for authenticated endpoints.

    Uses the stable user ID (sub claim) from the JWT so that rate limits
    persist across token refreshes. Falls back to proxy-aware IP for
    requests without a bearer token.
    """
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer ") and len(auth_header) > 40:
        token = auth_header[7:]
        user_id = _extract_user_id_from_jwt(token)
        if user_id:
            return f"uid:{user_id}"
        # Fallback: hash the token if we can't extract sub
        token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
        return f"tok:{token_hash}"
    return _get_client_ip(request)


def _get_storage_uri() -> Optional[str]:
    """Build Redis storage URI from Upstash credentials.

    Upstash REST URL (https://host.upstash.io) is converted to a
    rediss:// URI for the limits library (TLS required by Upstash).
    """
    if settings.upstash_redis_url and settings.upstash_redis_token:
        url = settings.upstash_redis_url.replace("https://", "").rstrip("/")
        return f"rediss://default:{settings.upstash_redis_token}@{url}:6379/0"
    logger.warning(
        "No Upstash Redis credentials found — rate limiting will use in-memory "
        "(ineffective on serverless)"
    )
    return None


storage_uri = _get_storage_uri()

limiter = Limiter(
    key_func=_get_user_or_ip,
    storage_uri=storage_uri,
    headers_enabled=True,
    key_style="endpoint",
    in_memory_fallback_enabled=True,  # Fall back to in-memory if Redis is unreachable
    swallow_errors=True,  # Never let rate limiter failures break the app
)
