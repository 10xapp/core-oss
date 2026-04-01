"""
FastAPI dependencies for authentication and authorization
"""
import sentry_sdk
from fastapi import Depends, Header, HTTPException, status
from typing import Optional, Tuple, Dict, Any
import jwt
from jwt import PyJWKClient
from api.config import settings


# Cache the JWKS client for ES256 verification
_jwks_client: Optional[PyJWKClient] = None


def _get_jwks_client() -> PyJWKClient:
    """Get or create cached JWKS client for Supabase."""
    global _jwks_client
    if _jwks_client is None:
        jwks_url = f"{settings.supabase_url}/auth/v1/.well-known/jwks.json"
        _jwks_client = PyJWKClient(jwks_url, cache_keys=True)
    return _jwks_client


def _decode_jwt(token: str) -> Dict[str, Any]:
    """
    Decode JWT supporting both HS256 (legacy) and ES256 (new ECC keys).
    Checks the token header to determine which algorithm to use.
    """
    # Peek at the algorithm in the token header
    try:
        unverified_header = jwt.get_unverified_header(token)
        alg = unverified_header.get("alg", "")
    except jwt.DecodeError:
        raise jwt.DecodeError("Invalid token format")

    # ES256 - use JWKS public key
    if alg == "ES256":
        jwks_client = _get_jwks_client()
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            signing_key.key,
            algorithms=["ES256"],
            audience="authenticated"
        )

    # HS256 - use shared secret (legacy)
    if alg == "HS256":
        if not settings.supabase_jwt_secret:
            raise jwt.DecodeError("JWT secret not configured for HS256")
        return jwt.decode(
            token,
            settings.supabase_jwt_secret,
            algorithms=["HS256"],
            audience="authenticated"
        )

    raise jwt.InvalidAlgorithmError(f"Unsupported algorithm: {alg}")


async def get_validated_token_and_payload(
    authorization: Optional[str] = Header(None)
) -> Tuple[str, Dict[str, Any]]:
    """
    Single source of truth for JWT validation.
    Extracts, validates, and decodes the Supabase JWT from the Authorization header.
    Supports both ES256 (new ECC keys) and HS256 (legacy shared secret).

    Returns:
        Tuple[str, Dict[str, Any]]: (token, payload) tuple

    Raises:
        HTTPException: If token is missing, invalid, or expired

    Note:
        FastAPI caches this dependency per-request, so multiple dependencies
        that use this (get_current_user_jwt, get_current_user_id) only trigger
        one JWT decode operation.
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header"
        )

    # Extract token from "Bearer <token>" format
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format. Expected: Bearer <token>"
        )

    token = parts[1]

    try:
        payload = _decode_jwt(token)
        return token, payload

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.InvalidAudienceError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token audience"
        )
    except jwt.InvalidAlgorithmError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )
    except jwt.DecodeError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid JWT token"
        )


async def get_current_user_jwt(
    token_and_payload: Tuple[str, Dict[str, Any]] = Depends(get_validated_token_and_payload)
) -> str:
    """
    Extract and validate the Supabase JWT from the Authorization header.

    Returns:
        str: The validated JWT token (for use with Supabase authenticated client)
    """
    return token_and_payload[0]


async def get_current_user_id(
    token_and_payload: Tuple[str, Dict[str, Any]] = Depends(get_validated_token_and_payload)
) -> str:
    """
    Extract the user ID from the validated Supabase JWT.

    Returns:
        str: The user ID from the JWT token

    Raises:
        HTTPException: If user ID is missing from token
    """
    payload = token_and_payload[1]
    user_id = payload.get("sub")

    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token: missing user ID"
        )

    sentry_sdk.set_user({"id": user_id})
    return user_id


def get_current_user_email(
    token_and_payload: Tuple[str, Dict[str, Any]] = Depends(get_validated_token_and_payload)
) -> str:
    """
    Extract the user's email from the validated Supabase JWT.

    Returns:
        str: The normalized email from the JWT claims

    Raises:
        HTTPException: If email is missing from token
    """
    payload = token_and_payload[1]
    user_email = payload.get("email")

    if not user_email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token: missing user email",
        )

    return str(user_email).strip().lower()


async def get_optional_user_jwt(
    authorization: Optional[str] = Header(None)
) -> Optional[str]:
    """
    Extract the Supabase JWT from the Authorization header.
    Returns None if not present (doesn't raise an error).

    Note:
        This does NOT validate the token - use only when auth is optional
        and you'll validate later if token is present.

    Returns:
        Optional[str]: The JWT token or None
    """
    if not authorization:
        return None

    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    return parts[1]
