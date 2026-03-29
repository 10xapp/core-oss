"""
Microsoft OAuth Provider Implementation

Implements OAuthProvider protocol for Microsoft/Outlook accounts.
Uses Microsoft Identity Platform (Azure AD) OAuth 2.0 endpoints.

Supports:
- Personal Microsoft accounts (outlook.com, hotmail.com, live.com)
- Work/School accounts (Office 365, Microsoft 365)

Key differences from Google:
- Microsoft returns a NEW refresh token on each refresh (must save it!)
- Uses Microsoft Graph API for user info
- Tenant is "common" for multi-account support
"""
from typing import Dict, Any, Optional
import logging
import requests
from datetime import datetime, timedelta, timezone

from api.config import settings
from lib.token_encryption import encrypt_token_fields

logger = logging.getLogger(__name__)


class MicrosoftReauthRequiredError(ValueError):
    """Raised when Microsoft refresh tokens are permanently invalid."""


# Microsoft token lifetime (typically 1 hour, but can vary)
DEFAULT_TOKEN_LIFETIME_SECONDS = 3600

# Microsoft OAuth endpoints
MICROSOFT_TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
MICROSOFT_GRAPH_URL = "https://graph.microsoft.com/v1.0"

# Required scopes for our app
MICROSOFT_SCOPES = [
    "openid",
    "email",
    "profile",
    "offline_access",
    "User.Read",
    "Mail.ReadWrite",  # ReadWrite needed for mark read/unread
    "Mail.Send",
    "Calendars.ReadWrite",
    "MailboxSettings.Read",  # For reading user's timezone
]


class MicrosoftOAuthProvider:
    """
    Microsoft implementation of OAuthProvider protocol.

    Handles OAuth token operations for Microsoft/Outlook accounts.
    """

    @property
    def provider_name(self) -> str:
        return "microsoft"

    def exchange_auth_code(
        self,
        auth_code: str,
        redirect_uri: Optional[str] = None,
        code_verifier: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Exchange a Microsoft authorization code for tokens.

        Supports PKCE (Proof Key for Code Exchange) when code_verifier is provided.
        iOS uses PKCE with ASWebAuthenticationSession for security.

        Args:
            auth_code: The authorization code from Microsoft OAuth flow
            redirect_uri: The redirect URI used in the OAuth flow
            code_verifier: PKCE code verifier (required if PKCE was used in auth request)

        Returns:
            Dict with access_token, refresh_token, expires_in, token_type
        """
        logger.info("🔑 [Microsoft] Exchanging auth code for tokens...")

        tenant = settings.microsoft_tenant_id or "common"
        token_url = MICROSOFT_TOKEN_URL.format(tenant=tenant)

        token_data = {
            "code": auth_code,
            "client_id": settings.microsoft_client_id,
            "grant_type": "authorization_code",
            "scope": " ".join(MICROSOFT_SCOPES),
        }

        if redirect_uri:
            token_data["redirect_uri"] = redirect_uri

        # PKCE flow (public client from iOS) - use code_verifier WITHOUT client_secret
        # The iOS platform in Azure AD is registered as a public client
        # Public clients CANNOT send client_secret (AADSTS90023 error)
        if code_verifier:
            token_data["code_verifier"] = code_verifier
            logger.info("🔐 [Microsoft] Using PKCE flow (public client, no client_secret)")
        else:
            # Confidential client flow (web backend) - use client_secret
            token_data["client_secret"] = settings.microsoft_client_secret
            logger.info("🔐 [Microsoft] Using confidential client flow (with client_secret)")

        response = requests.post(
            token_url,
            data=token_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30
        )

        if response.status_code != 200:
            error_data = response.json()
            error_msg = error_data.get('error_description', error_data.get('error', 'Unknown error'))
            logger.error(f"❌ [Microsoft] Token exchange failed: {error_msg}")
            raise ValueError(f"Failed to exchange auth code: {error_msg}")

        tokens = response.json()
        logger.info("✅ [Microsoft] Token exchange successful")

        return {
            "access_token": tokens.get("access_token"),
            "refresh_token": tokens.get("refresh_token"),
            "id_token": tokens.get("id_token"),  # For Supabase auth (returned when openid scope)
            "expires_in": tokens.get("expires_in", DEFAULT_TOKEN_LIFETIME_SECONDS),
            "token_type": tokens.get("token_type", "Bearer"),
            "is_public_client": code_verifier is not None,  # Track PKCE flow for refresh
        }

    def refresh_access_token(
        self,
        connection_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Refresh an expired Microsoft access token.

        IMPORTANT: Microsoft returns a NEW refresh token on each refresh.
        The caller MUST save the new refresh_token to the database!

        Args:
            connection_data: Dict with refresh_token and optional metadata

        Returns:
            Dict with new access_token, refresh_token (NEW!), expires_in
        """
        refresh_token = connection_data.get('refresh_token')
        if not refresh_token:
            raise ValueError("No refresh token available")

        # Get client credentials (support per-connection overrides)
        metadata = connection_data.get('metadata', {}) or {}
        client_id = metadata.get('client_id') or settings.microsoft_client_id
        tenant = metadata.get('tenant_id') or settings.microsoft_tenant_id or "common"

        # Check if this is a public client (PKCE/iOS) - don't send client_secret
        is_public_client = metadata.get('is_public_client', False)

        if not client_id:
            raise ValueError("Missing Microsoft OAuth client_id")

        token_url = MICROSOFT_TOKEN_URL.format(tenant=tenant)

        token_data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "scope": " ".join(MICROSOFT_SCOPES),
        }

        # Only send client_secret for confidential clients (NOT PKCE/public clients)
        if not is_public_client:
            client_secret = metadata.get('client_secret') or settings.microsoft_client_secret
            if not client_secret:
                raise ValueError("Missing client_secret for confidential client refresh")
            token_data["client_secret"] = client_secret
            logger.info("🔐 [Microsoft] Refreshing with client_secret (confidential client)")
        else:
            logger.info("🔐 [Microsoft] Refreshing WITHOUT client_secret (public client/PKCE)")

        response = requests.post(
            token_url,
            data=token_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30
        )

        if response.status_code != 200:
            error_data = response.json()
            error_code = error_data.get('error', '')
            error_msg = error_data.get('error_description', error_code)

            logger.error(f"❌ [Microsoft] Token refresh failed: {error_msg}")

            # Handle revoked/expired refresh token
            if error_code == 'invalid_grant':
                raise MicrosoftReauthRequiredError("Refresh token is invalid - user must re-authenticate")

            raise ValueError(f"Failed to refresh token: {error_msg}")

        tokens = response.json()

        # CRITICAL: Microsoft returns a NEW refresh token - must save it!
        new_refresh_token = tokens.get("refresh_token")
        if new_refresh_token and new_refresh_token != refresh_token:
            logger.info("🔄 [Microsoft] New refresh token issued (token rotation)")

        logger.info("✅ [Microsoft] Token refresh successful")

        return {
            "access_token": tokens.get("access_token"),
            "refresh_token": new_refresh_token or refresh_token,
            "expires_in": tokens.get("expires_in", DEFAULT_TOKEN_LIFETIME_SECONDS),
        }

    def get_user_info(self, access_token: str) -> Dict[str, Any]:
        """
        Get user profile info from Microsoft Graph API.

        Args:
            access_token: Valid Microsoft access token

        Returns:
            Dict with email, name, picture, id
        """
        logger.info("👤 [Microsoft] Fetching user info from Graph API...")

        response = requests.get(
            f"{MICROSOFT_GRAPH_URL}/me",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30
        )

        if response.status_code != 200:
            logger.error(f"❌ [Microsoft] Failed to get user info: {response.text}")
            raise ValueError("Failed to get user info from Microsoft")

        user_info = response.json()

        # Microsoft Graph returns different field names
        email = (
            user_info.get("mail") or
            user_info.get("userPrincipalName") or
            ""
        )

        # Try to get profile photo URL (may require additional permissions)
        picture_url = None
        try:
            photo_response = requests.get(
                f"{MICROSOFT_GRAPH_URL}/me/photo/$value",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10
            )
            if photo_response.status_code == 200:
                # Photo exists but we get binary data
                # For now, we'll skip embedding it - could base64 encode or upload to storage
                picture_url = None  # Microsoft doesn't provide a direct URL like Google
        except Exception:
            pass  # Photo not available

        logger.info(f"✅ [Microsoft] Got user info: {email}")

        return {
            "email": email,
            "name": user_info.get("displayName"),
            "picture": picture_url,
            "id": user_info.get("id"),
        }


# Utility functions for Microsoft-specific operations

def get_valid_microsoft_credentials(
    connection_data: Dict[str, Any],
    supabase_client=None
) -> str:
    """
    Get a valid Microsoft access token, refreshing if needed.

    Similar to google_auth.get_valid_credentials but for Microsoft.
    Returns just the access token string (Microsoft Graph uses bearer token directly).

    Args:
        connection_data: Dict with tokens and expiry info
        supabase_client: Optional Supabase client for saving refreshed token

    Returns:
        Valid access token string
    """

    access_token = connection_data.get('access_token')
    token_expires_at = connection_data.get('token_expires_at')

    if not access_token:
        raise ValueError("No access token available")

    # Check if token needs refresh (5 minute buffer)
    needs_refresh = False
    if token_expires_at:
        try:
            if token_expires_at.endswith('Z'):
                expires_at = datetime.fromisoformat(token_expires_at.replace('Z', '+00:00'))
            else:
                expires_at = datetime.fromisoformat(token_expires_at)

            buffer = timedelta(minutes=5)
            if expires_at <= (datetime.now(timezone.utc) + buffer):
                needs_refresh = True
        except (ValueError, TypeError):
            needs_refresh = True
    else:
        needs_refresh = True  # No expiry stored, refresh to be safe

    if not needs_refresh:
        return access_token

    # Refresh the token
    logger.info("🔄 [Microsoft] Token expired or expiring soon, refreshing...")
    provider = MicrosoftOAuthProvider()
    new_tokens = provider.refresh_access_token(connection_data)

    # Save new tokens to database
    if supabase_client and connection_data.get('id'):
        new_expires_at = datetime.now(timezone.utc) + timedelta(seconds=new_tokens['expires_in'])

        update_data = {
            'access_token': new_tokens['access_token'],
            'refresh_token': new_tokens['refresh_token'],  # IMPORTANT: Save new refresh token!
            'token_expires_at': new_expires_at.isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }

        supabase_client.table('ext_connections')\
            .update(encrypt_token_fields(update_data))\
            .eq('id', connection_data['id'])\
            .execute()

        logger.info("✅ [Microsoft] Saved refreshed tokens to database")

    return new_tokens['access_token']
