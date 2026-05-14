"""Utilities for classifying Google API/OAuth errors by permanence."""

from typing import Any

# These indicate account capability/configuration problems that retries won't fix.
PERMANENT_GOOGLE_API_ERROR_PATTERNS = (
    "insufficientpermissions",
    "insufficientauthenticationscopes",
    "insufficient authentication scopes",
    "mail service not enabled",
    "failedprecondition",
    "forbidden",
    "accessnotconfigured",
    "access not configured",
)

# These indicate OAuth credential state that requires reconnect/user action.
PERMANENT_GOOGLE_OAUTH_ERROR_PATTERNS = (
    "invalid_grant",
    "account has been deleted",
    "refresh token is invalid",
    "refresh token has expired or been revoked",
    "token expired or revoked",
    "token has been expired or revoked",
    "token has been revoked",
    "user has been suspended",
)


def _normalize_error(error: Any) -> str:
    return str(error).lower() if error is not None else ""


def is_permanent_google_api_error(error: Any) -> bool:
    """Return True when a Google API error is known-permanent/unrecoverable."""
    error_str = _normalize_error(error)
    return any(pattern in error_str for pattern in PERMANENT_GOOGLE_API_ERROR_PATTERNS)


def is_permanent_google_oauth_error(error: Any) -> bool:
    """Return True when an OAuth refresh/auth error is known-permanent."""
    error_str = _normalize_error(error)
    return any(pattern in error_str for pattern in PERMANENT_GOOGLE_OAUTH_ERROR_PATTERNS)
