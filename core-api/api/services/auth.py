"""
Authentication service - Business logic for user and OAuth operations

Supports multiple OAuth providers:
- Google (Gmail, Google Calendar)
- Microsoft (Outlook, Microsoft 365)
"""
from typing import Dict, Any, List, Optional
from io import BytesIO
from datetime import datetime
import asyncio
import uuid
import logging

import httpx

from lib.supabase_client import get_authenticated_supabase_client, supabase
from lib.token_encryption import encrypt_token_fields
from api.services.provider_factory import ProviderFactory
from api.config import settings


def _redact_email(email: str) -> str:
    """Partially redact an email for safe logging: ja***@gmail.com"""
    if "@" not in email:
        return "***"
    local, domain = email.rsplit("@", 1)
    return f"{local[:2]}***@{domain}" if len(local) > 2 else f"{local[0]}***@{domain}"

logger = logging.getLogger(__name__)

# Supported email providers
SUPPORTED_PROVIDERS = ["google", "microsoft"]

_ALLOWED_AVATAR_TYPES = {"image/png", "image/jpeg"}


def _run_inline_google_initial_sync(
    *,
    connection_id: str,
    user_id: str,
    access_token: str,
    refresh_token: Optional[str],
    provider_email: str,
    run_gmail: bool,
    run_calendar: bool,
) -> None:
    """Inline fallback for Google initial sync when queue publish fails."""
    if not run_gmail and not run_calendar:
        return

    from api.config import settings as app_settings
    from api.services.syncs import sync_google_calendar_cron, sync_gmail_for_connection
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from lib.supabase_client import get_service_role_client

    credentials = Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=app_settings.google_client_id,
        client_secret=app_settings.google_client_secret
    )
    service_supabase = get_service_role_client()

    if run_gmail:
        try:
            sync_result = sync_gmail_for_connection(
                gmail_service=build('gmail', 'v1', credentials=credentials),
                user_id=user_id,
                connection_id=connection_id,
                supabase_client=service_supabase,
                max_results=50,
                days_back=20,
            )
            if not sync_result.get('success'):
                logger.warning(f"⚠️ [Google] Inline initial Gmail sync failed for {_redact_email(provider_email)}: {sync_result.get('error')}")
        except Exception as exc:
            logger.warning(f"⚠️ [Google] Inline initial Gmail sync error for {_redact_email(provider_email)}: {exc}")

    if run_calendar:
        try:
            cal_result = sync_google_calendar_cron(
                calendar_service=build('calendar', 'v3', credentials=credentials),
                connection_id=connection_id,
                user_id=user_id,
                service_supabase=service_supabase,
                days_past=7,
                days_future=60,
            )
            if cal_result.get('status') != 'success':
                logger.warning(f"⚠️ [Google] Inline initial Calendar sync failed for {_redact_email(provider_email)}: {cal_result.get('error')}")
        except Exception as exc:
            logger.warning(f"⚠️ [Google] Inline initial Calendar sync error for {_redact_email(provider_email)}: {exc}")


async def _enqueue_or_fallback_google_initial_sync(
    *,
    connection_id: str,
    user_id: str,
    access_token: str,
    refresh_token: Optional[str],
    provider_email: str,
) -> None:
    """
    Mark Google initial sync streams dirty for background worker execution.
    """
    from lib.supabase_client import get_service_role_client
    from api.services.syncs.sync_dispatcher import mark_stream_dirty
    from api.services.syncs.sync_state_store import SYNC_KIND_CALENDAR, SYNC_KIND_EMAIL

    service_supabase = get_service_role_client()

    gmail_marked = mark_stream_dirty(
        service_supabase,
        connection_id,
        "google",
        SYNC_KIND_EMAIL,
        priority=100,
        metadata={
            "source": "initial-sync",
            "initial_sync": True,
            "max_results": 50,
            "days_back": 20,
        },
    )
    calendar_marked = mark_stream_dirty(
        service_supabase,
        connection_id,
        "google",
        SYNC_KIND_CALENDAR,
        priority=100,
        metadata={
            "source": "initial-sync",
            "initial_sync": True,
            "days_past": 7,
            "days_future": 60,
        },
    )

    if gmail_marked and calendar_marked:
        logger.info(f"✅ [Google] Initial sync scheduled for {_redact_email(provider_email)}")
        return

    logger.warning(
        f"⚠️ [Google] Initial sync dirty-mark partial/failed for {_redact_email(provider_email)}. "
        f"gmail_marked={gmail_marked}, calendar_marked={calendar_marked}"
    )


def _run_inline_microsoft_initial_sync(
    *,
    connection_id: str,
    user_id: str,
    access_token: str,
    refresh_token: Optional[str],
    token_expires_at: Optional[str],
    metadata: Optional[Dict[str, Any]],
    provider_email: str,
    run_email: bool,
    run_calendar: bool,
) -> None:
    """Inline fallback for Microsoft initial sync when queue publish fails."""
    if not run_email and not run_calendar:
        return

    from api.services.syncs.sync_outlook import sync_outlook_for_connection
    from api.services.syncs.sync_outlook_calendar import sync_outlook_calendar

    if run_email:
        try:
            sync_result = sync_outlook_for_connection(
                access_token=access_token,
                user_id=user_id,
                connection_id=connection_id,
                max_results=50,
                days_back=20,
            )
            if not sync_result.get('success'):
                logger.warning(f"⚠️ [Microsoft] Inline initial email sync failed for {_redact_email(provider_email)}: {sync_result.get('error')}")
        except Exception as exc:
            logger.warning(f"⚠️ [Microsoft] Inline initial email sync error for {_redact_email(provider_email)}: {exc}")

    if run_calendar:
        try:
            cal_result = sync_outlook_calendar(
                user_id=user_id,
                connection_id=connection_id,
                connection_data={
                    'id': connection_id,
                    'access_token': access_token,
                    'refresh_token': refresh_token,
                    'token_expires_at': token_expires_at,
                    'metadata': metadata or {},
                },
                days_back=7,
                days_forward=60,
            )
            if not cal_result.get('success'):
                logger.warning(f"⚠️ [Microsoft] Inline initial calendar sync failed for {_redact_email(provider_email)}: {cal_result.get('error')}")
        except Exception as exc:
            logger.warning(f"⚠️ [Microsoft] Inline initial calendar sync error for {_redact_email(provider_email)}: {exc}")


async def _enqueue_or_fallback_microsoft_initial_sync(
    *,
    connection_id: str,
    user_id: str,
    access_token: str,
    refresh_token: Optional[str],
    token_expires_at: Optional[str],
    metadata: Optional[Dict[str, Any]],
    provider_email: str,
    include_calendar: bool,
) -> None:
    """
    Mark Microsoft initial sync streams dirty for background worker execution.
    """
    from lib.supabase_client import get_service_role_client
    from api.services.syncs.sync_dispatcher import mark_stream_dirty
    from api.services.syncs.sync_state_store import SYNC_KIND_CALENDAR, SYNC_KIND_EMAIL

    service_supabase = get_service_role_client()

    email_marked = mark_stream_dirty(
        service_supabase,
        connection_id,
        "microsoft",
        SYNC_KIND_EMAIL,
        priority=100,
        metadata={
            "source": "initial-sync",
            "initial_sync": True,
            "max_results": 50,
            "days_back": 20,
        },
    )

    calendar_marked = True
    if include_calendar:
        calendar_marked = mark_stream_dirty(
            service_supabase,
            connection_id,
            "microsoft",
            SYNC_KIND_CALENDAR,
            priority=100,
            metadata={
                "source": "initial-sync",
                "initial_sync": True,
                "days_past": 7,
                "days_future": 60,
            },
        )

    if email_marked and calendar_marked:
        # logger.info(f"✅ [Microsoft] Initial sync scheduled for {_redact_email(provider_email)}")
        return

    # logger.warning(
    #     f"⚠️ [Microsoft] Initial sync dirty-mark partial/failed for {_redact_email(provider_email)}. "
    #     f"email_marked={email_marked}, calendar_marked={calendar_marked}"
    # )


async def _download_avatar_to_r2(avatar_url: Optional[str], user_id: str) -> Optional[str]:
    """Download an external avatar image and upload it to the R2 public bucket.

    Returns the public R2 URL on success, or None on failure.
    """
    if not avatar_url or not settings.r2_public_access_url:
        return None

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(avatar_url, timeout=10, follow_redirects=True)
        resp.raise_for_status()

        content_type = resp.headers.get('content-type', 'image/png').split(';')[0]
        if content_type not in _ALLOWED_AVATAR_TYPES:
            logger.warning(f"⚠️ Unsupported avatar content type: {content_type}")
            return None

        ext = '.jpg' if 'jpeg' in content_type else '.png'
        timestamp = datetime.utcnow().strftime('%Y%m%d')
        r2_key = f"avatars/{user_id}/{timestamp}/{uuid.uuid4()}{ext}"

        from lib.r2_client import get_r2_client
        r2 = get_r2_client()

        # Run blocking S3 upload in a thread to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: r2.s3_client.upload_fileobj(
                BytesIO(resp.content),
                settings.r2_public_bucket,
                r2_key,
                ExtraArgs={'ContentType': content_type},
            ),
        )

        public_url = f"{settings.r2_public_access_url}/{r2_key}"
        logger.info(f"✅ Downloaded avatar to R2: {public_url}")
        return public_url
    except Exception as e:
        logger.warning(f"⚠️ Failed to download avatar to R2: {e}")
        return None


def exchange_auth_code_for_tokens(
    server_auth_code: str,
    redirect_uri: Optional[str] = None,
    provider: str = "google",
    code_verifier: Optional[str] = None
) -> Dict[str, Any]:
    """
    Exchange an OAuth server auth code for access and refresh tokens.

    Supports multiple providers via the provider factory.

    Args:
        server_auth_code: The one-time auth code from OAuth flow
        redirect_uri: The redirect URI used in the OAuth flow
        provider: OAuth provider ('google' or 'microsoft')
        code_verifier: PKCE code verifier (for Microsoft iOS flow)

    Returns:
        Dict with access_token, refresh_token, expires_in, token_type

    Raises:
        ValueError: If the exchange fails or provider is unsupported
    """
    # Validate provider to prevent injection attacks
    if provider not in SUPPORTED_PROVIDERS:
        raise ValueError(f"Unsupported provider: {provider}. Must be one of: {SUPPORTED_PROVIDERS}")

    logger.info(f"🔑 [{provider}] Exchanging server auth code for tokens...")

    try:
        oauth_provider = ProviderFactory.get_oauth_provider(provider)

        # Microsoft supports PKCE, Google doesn't need it (uses client_secret)
        if provider == "microsoft" and code_verifier:
            tokens = oauth_provider.exchange_auth_code(server_auth_code, redirect_uri, code_verifier)
        else:
            tokens = oauth_provider.exchange_auth_code(server_auth_code, redirect_uri)

        logger.info(f"✅ [{provider}] Token exchange successful - "
                   f"access_token: {'✓' if tokens.get('access_token') else '✗'}, "
                   f"refresh_token: {'✓' if tokens.get('refresh_token') else '✗'}")

        return tokens

    except Exception as e:
        logger.error(f"❌ [{provider}] Token exchange failed: {e}")
        raise ValueError(f"Failed to exchange auth code: {e}") from e


def get_user_info(access_token: str, provider: str = "google") -> Dict[str, Any]:
    """
    Get user profile info using an access token.

    Supports multiple providers via the provider factory.

    Args:
        access_token: A valid OAuth access token
        provider: OAuth provider ('google' or 'microsoft')

    Returns:
        Dict with email, name, picture, id (provider's user ID)

    Raises:
        ValueError: If the request fails
    """
    logger.info(f"👤 [{provider}] Fetching user info...")

    try:
        oauth_provider = ProviderFactory.get_oauth_provider(provider)
        user_info = oauth_provider.get_user_info(access_token)

        email = user_info.get('email', '')
        redacted_email = f"{email[:3]}...@..." if email and '@' in email else '***'
        logger.info(f"✅ [{provider}] Got user info: {redacted_email}")
        return user_info

    except Exception as e:
        logger.error(f"❌ [{provider}] Failed to get user info: {e}")
        raise ValueError(f"Failed to get user info: {e}") from e


# Backward compatibility alias
def get_google_user_info(access_token: str) -> Dict[str, Any]:
    """Backward compatibility wrapper - use get_user_info() instead."""
    return get_user_info(access_token, provider="google")


class AuthService:
    """Service class for authentication operations"""

    @staticmethod
    def create_user(user_data: Dict[str, Any], user_jwt: str) -> Dict[str, Any]:
        """
        Create a new user in the database.
        Returns existing user if already exists.
        """
        user_id = user_data.get('id')
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        # Check if user already exists
        existing = auth_supabase.table('users').select('*').eq('id', user_id).maybe_single().execute()

        if existing.data:
            logger.info(f"User {user_id} already exists")
            return {
                "message": "User already exists",
                "user": existing.data
            }

        # Create new user
        result = auth_supabase.table('users').insert({
            'id': user_data.get('id'),
            'email': user_data.get('email'),
            'name': user_data.get('name'),
            'avatar_url': user_data.get('avatar_url'),
        }).execute()

        logger.info(f"Created new user: {user_id}")
        return {
            "message": "User created successfully",
            "user": result.data[0]
        }

    @staticmethod
    def create_oauth_connection(connection_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Store OAuth connection tokens for a user.
        Uses upsert to avoid race conditions when multiple requests try to
        create/update the same connection simultaneously (#6 fix).
        """
        user_id = connection_data.get('user_id')
        provider = connection_data.get('provider')
        provider_user_id = connection_data.get('provider_user_id')

        data = {
            'user_id': user_id,
            'provider': provider,
            'provider_user_id': provider_user_id,
            'provider_email': connection_data.get('provider_email'),
            'access_token': connection_data.get('access_token'),
            'refresh_token': connection_data.get('refresh_token'),
            'token_expires_at': connection_data.get('token_expires_at'),
            'scopes': connection_data.get('scopes', []),
            'is_active': True,
            'metadata': connection_data.get('metadata') or {}
        }

        # Use upsert to atomically create or update - avoids race condition
        result = supabase.table('ext_connections')\
            .upsert(encrypt_token_fields(data), on_conflict='user_id,provider,provider_user_id')\
            .execute()

        logger.info(f"Upserted OAuth connection for user {user_id}, provider {provider}")

        return {
            "message": "OAuth connection saved successfully",
            "connection": result.data[0] if result.data else None
        }

    @staticmethod
    def get_user_connections(user_id: str) -> Dict[str, Any]:
        """
        Get all OAuth connections for a user.
        """
        result = supabase.table('ext_connections')\
            .select('id, user_id, provider, provider_email, scopes, is_active, created_at, updated_at')\
            .eq('user_id', user_id)\
            .execute()
        
        return {
            "connections": result.data
        }

    @staticmethod
    def revoke_connection(connection_id: str, user_id: str) -> bool:
        """
        Revoke/deactivate an OAuth connection.
        Returns True if revoked, False if not found or not owned by user.

        Args:
            connection_id: The connection ID to revoke
            user_id: The authenticated user's ID (must own the connection)
        """
        # Only revoke if the connection belongs to the user
        result = supabase.table('ext_connections')\
            .update({'is_active': False})\
            .eq('id', connection_id)\
            .eq('user_id', user_id)\
            .execute()

        if not result.data:
            return False

        logger.info(f"Revoked connection {connection_id} for user {user_id}")
        return True

    @staticmethod
    async def complete_oauth_flow(oauth_data: Dict[str, Any], user_jwt: str) -> Dict[str, Any]:
        """
        Complete OAuth flow - creates user and stores connection in one operation.

        Supports multiple providers (Google, Microsoft) and two auth modes:
        1. Direct tokens: access_token (and optionally refresh_token) provided directly
           - Used by web app where Supabase handles token exchange
        2. Server auth code: server_auth_code provided, must be exchanged for tokens
           - Used by iOS app where we need to exchange the code ourselves

        NOTE: This is async because Microsoft webhook subscription creation
        must be async to avoid blocking the event loop during validation.

        Args:
            oauth_data: OAuth connection data including:
                - provider: 'google' or 'microsoft' (default: 'google')
                - user_id, email, name, avatar_url
                - server_auth_code OR access_token
            user_jwt: User's Supabase JWT for authenticated requests
        """
        user_id = oauth_data.get('user_id')
        email = oauth_data.get('email')
        provider = oauth_data.get('provider', 'google')

        # Check if we need to exchange a server auth code for tokens (iOS flow)
        server_auth_code = oauth_data.get('server_auth_code')
        code_verifier = oauth_data.get('code_verifier')  # PKCE support for Microsoft
        access_token = oauth_data.get('access_token')
        refresh_token = oauth_data.get('refresh_token')
        expires_in = None

        if server_auth_code:
            logger.info(f"📱 [{provider}] iOS auth flow detected - exchanging server auth code")
            if code_verifier:
                logger.info(f"🔐 [{provider}] PKCE code_verifier provided")
            try:
                tokens = exchange_auth_code_for_tokens(
                    server_auth_code,
                    provider=provider,
                    code_verifier=code_verifier
                )
                access_token = tokens.get('access_token')
                refresh_token = tokens.get('refresh_token')
                expires_in = tokens.get('expires_in')
                logger.info(f"✅ [{provider}] Got tokens from exchange - has refresh: {bool(refresh_token)}")
            except ValueError as e:
                logger.error(f"❌ [{provider}] Token exchange failed: {e}")
                raise
        elif not access_token:
            logger.warning(f"⚠️ [{provider}] No access_token or server_auth_code provided")

        # Use authenticated Supabase client (respects RLS policies)
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        # Create or update user
        user_result = auth_supabase.table('users').select('id, avatar_url').eq('id', user_id).execute()

        google_avatar_url = oauth_data.get('avatar_url')

        if not user_result.data:
            # New user: download Google avatar to R2 for reliable hosting
            r2_avatar_url = await _download_avatar_to_r2(google_avatar_url, user_id)

            auth_supabase.table('users').insert({
                'id': user_id,
                'email': email,
                'name': oauth_data.get('name'),
                'avatar_url': r2_avatar_url or google_avatar_url,
            }).execute()
            logger.info(f"✅ Created new user: {user_id}")
        else:
            existing_avatar = user_result.data[0].get('avatar_url') if user_result.data else None

            # Only update avatar if user doesn't already have one
            update_data: Dict[str, Any] = {'name': oauth_data.get('name')}
            if not existing_avatar and google_avatar_url:
                r2_avatar_url = await _download_avatar_to_r2(google_avatar_url, user_id)
                update_data['avatar_url'] = r2_avatar_url or google_avatar_url

            auth_supabase.table('users').update(update_data).eq('id', user_id).execute()
            logger.info(f"✅ Updated user: {user_id}")

        # Store OAuth connection using upsert to avoid race conditions (#6 fix)
        from datetime import datetime, timedelta, timezone

        # Calculate token expiry
        token_expires_at = oauth_data.get('token_expires_at')
        if not token_expires_at and access_token:
            if expires_in:
                # Use expires_in from token exchange if available
                token_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
            else:
                # Default to 1 hour from now
                token_expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

        # Check if user already has ANY primary connection (across all providers)
        # This prevents multiple is_primary=true when user signs up with Google
        # then signs in with Microsoft using the same email (issue #59)
        provider = oauth_data.get('provider', 'google')
        existing_primary = auth_supabase.table('ext_connections')\
            .select('id')\
            .eq('user_id', user_id)\
            .eq('is_primary', True)\
            .eq('is_active', True)\
            .limit(1)\
            .execute()

        has_primary = existing_primary.data and len(existing_primary.data) > 0

        # Also check if this is a new connection for THIS provider (for initial sync)
        existing_provider_connection = auth_supabase.table('ext_connections')\
            .select('id')\
            .eq('user_id', user_id)\
            .eq('provider', provider)\
            .eq('is_active', True)\
            .limit(1)\
            .execute()

        is_new_provider_connection = not existing_provider_connection.data or len(existing_provider_connection.data) == 0

        # Build metadata, including is_public_client for PKCE/iOS flows
        metadata = oauth_data.get('metadata') or {}
        if code_verifier:
            metadata['is_public_client'] = True

        connection_data = {
            'user_id': user_id,
            'provider': oauth_data.get('provider', 'google'),
            'provider_user_id': oauth_data.get('provider_user_id'),
            'provider_email': email,
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_expires_at': token_expires_at,
            'scopes': oauth_data.get('scopes', []),
            'is_active': True,
            'metadata': metadata
        }

        # Set is_primary only if user has no existing primary connection
        # This handles: signup (first ever), but NOT cross-provider sign-in with same email
        if not has_primary:
            connection_data['is_primary'] = True
            connection_data['account_order'] = 0
            logger.info(f"📌 Setting as primary connection for user {user_id}")

        # Use upsert to atomically create or update - avoids race condition
        upsert_result = auth_supabase.table('ext_connections')\
            .upsert(encrypt_token_fields(connection_data), on_conflict='user_id,provider,provider_user_id')\
            .execute()
        logger.info(f"✅ Upserted OAuth connection for {user_id}")

        # Get the connection ID from the upsert result
        connection_id = upsert_result.data[0]['id'] if upsert_result.data else None

        # Set up webhook subscriptions for Microsoft (create/renew on EVERY login)
        # (Google is handled by iOS calling ensure-watches)
        # NOTE: These calls are async to avoid blocking the event loop during validation
        if provider == 'microsoft' and access_token and connection_id:
            try:
                from api.services.microsoft.microsoft_webhook_provider import create_microsoft_subscription
                from lib.supabase_client import get_service_role_client

                service_supabase = get_service_role_client()

                redacted_email = f"{email[:3]}...@..." if email and '@' in email else '***'
                logger.info(f"📡 [Microsoft] Setting up webhook subscriptions for {redacted_email}...")

                # Mail subscription (async to allow webhook validation)
                mail_sub_result = await create_microsoft_subscription(
                    access_token=access_token,
                    resource_type='mail',
                    connection_id=connection_id,
                    user_id=user_id,
                    supabase_client=service_supabase
                )
                if mail_sub_result.get('success'):
                    logger.info("✅ [Microsoft] Mail subscription created")
                else:
                    logger.warning(f"⚠️ [Microsoft] Mail subscription failed: {mail_sub_result.get('error')}")

                # Calendar subscription (async to allow webhook validation)
                cal_sub_result = await create_microsoft_subscription(
                    access_token=access_token,
                    resource_type='calendar',
                    connection_id=connection_id,
                    user_id=user_id,
                    supabase_client=service_supabase
                )
                if cal_sub_result.get('success'):
                    logger.info("✅ [Microsoft] Calendar subscription created")
                else:
                    logger.warning(f"⚠️ [Microsoft] Calendar subscription failed: {cal_sub_result.get('error')}")

            except Exception as e:
                logger.warning(f"⚠️ [Microsoft] Failed to create subscriptions: {str(e)}")
                # Don't fail - cron will still sync periodically

        # Initial sync: ONLY for first Microsoft connection (not on re-login)
        if provider == 'microsoft' and is_new_provider_connection and access_token and connection_id:
            redacted_email_for_log = f"{email[:3]}...@..." if email and '@' in email else '***'
            try:
                await _enqueue_or_fallback_microsoft_initial_sync(
                    connection_id=connection_id,
                    user_id=user_id,
                    access_token=access_token,
                    refresh_token=refresh_token,
                    token_expires_at=token_expires_at,
                    metadata=metadata,
                    provider_email=redacted_email_for_log,
                    include_calendar=False,  # Preserve existing complete_oauth_flow behavior
                )
            except Exception as e:
                logger.warning(f"⚠️ [Microsoft] Failed to run initial sync for {redacted_email_for_log}: {str(e)}")
                # Don't fail the whole operation if sync setup fails

        return {
            "message": "OAuth flow completed successfully",
            "user_id": user_id,
            "has_refresh_token": bool(refresh_token)
        }

    # ============== Multi-Account Methods ==============

    @staticmethod
    def get_email_accounts(user_id: str, user_jwt: str) -> List[Dict[str, Any]]:
        """
        Get all connected email accounts for a user (Google + Microsoft).
        Returns list sorted by account_order.
        """
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        # Fetch both Google and Microsoft accounts
        result = auth_supabase.table('ext_connections')\
            .select('id, provider, provider_email, is_primary, account_order, is_active, metadata')\
            .eq('user_id', user_id)\
            .in_('provider', SUPPORTED_PROVIDERS)\
            .eq('is_active', True)\
            .order('account_order')\
            .execute()

        accounts = []
        for conn in result.data or []:
            metadata = conn.get('metadata') or {}
            provider = conn.get('provider', 'google')
            accounts.append({
                'id': conn['id'],
                'provider': provider,
                'provider_email': conn['provider_email'],
                'provider_name': metadata.get('full_name') or metadata.get('name'),
                'provider_avatar': metadata.get('picture') or metadata.get('avatar_url'),
                'is_primary': conn.get('is_primary', False),
                'account_order': conn.get('account_order', 0),
                'is_active': conn['is_active']
            })

        return accounts

    @staticmethod
    async def add_email_account(
        user_id: str,
        user_jwt: str,
        account_data: Dict[str, Any],
        account_order: int
    ) -> Dict[str, Any]:
        """
        Add a secondary email account (Google or Microsoft).
        Handles token exchange for iOS/web direct OAuth flow.
        Fetches user info from provider when using auth code flow.
        Sets up push notification watch for the new account (Google only for now).
        """
        auth_supabase = get_authenticated_supabase_client(user_jwt)
        provider = account_data.get('provider', 'google')

        # Handle token exchange for auth code flow (iOS or web direct OAuth)
        server_auth_code = account_data.get('server_auth_code')
        code_verifier = account_data.get('code_verifier')  # PKCE support for Microsoft
        redirect_uri = account_data.get('redirect_uri')
        access_token = account_data.get('access_token')
        refresh_token = account_data.get('refresh_token')
        expires_in = None

        # User info - may be provided or fetched from provider
        provider_email = account_data.get('provider_email')
        provider_user_id = account_data.get('provider_user_id')
        user_name = None
        user_picture = None

        if server_auth_code:
            logger.info(f"🔑 [{provider}] Auth code flow - exchanging server auth code for secondary account")
            if code_verifier:
                logger.info(f"🔐 [{provider}] PKCE code_verifier provided")
            tokens = exchange_auth_code_for_tokens(
                server_auth_code,
                redirect_uri,
                provider=provider,
                code_verifier=code_verifier
            )
            access_token = tokens.get('access_token')
            refresh_token = tokens.get('refresh_token')
            expires_in = tokens.get('expires_in')

            # Fetch user info from provider using the access token
            logger.info(f"👤 [{provider}] Fetching user info...")
            user_info = get_user_info(access_token, provider=provider)
            provider_email = user_info.get('email')
            provider_user_id = user_info.get('id')
            user_name = user_info.get('name')
            user_picture = user_info.get('picture')
            redacted_provider_email = f"{provider_email[:3]}...@..." if provider_email and '@' in provider_email else '***'
            logger.info(f"✅ [{provider}] Got user info: {redacted_provider_email}")

        if not access_token:
            raise ValueError("No access token available")

        if not provider_email:
            raise ValueError("Could not determine email for account")

        # Check for duplicate ACTIVE account (allow re-adding if previously hard-deleted)
        existing = auth_supabase.table('ext_connections')\
            .select('id')\
            .eq('user_id', user_id)\
            .eq('provider_email', provider_email)\
            .eq('is_active', True)\
            .execute()
        if existing.data:
            raise ValueError("This email account is already connected")

        # Note: We use upsert below to handle the case where the account
        # was previously added but hard-deleted (row still exists)

        # Calculate token expiry
        from datetime import datetime, timedelta, timezone
        if expires_in:
            token_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
        else:
            token_expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

        # Build metadata with user info and public client flag for PKCE/iOS flows
        metadata = account_data.get('metadata') or {}
        if user_name:
            metadata['full_name'] = user_name
        if user_picture:
            metadata['picture'] = user_picture
        if code_verifier:
            metadata['is_public_client'] = True

        # Insert the new connection (NOT primary, with order)
        connection_data = {
            'user_id': user_id,
            'provider': account_data.get('provider', 'google'),
            'provider_user_id': provider_user_id or provider_email,
            'provider_email': provider_email,
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_expires_at': token_expires_at,
            'scopes': account_data.get('scopes', []),
            'is_active': True,
            'is_primary': False,  # Secondary accounts are never primary
            'account_order': account_order,
            'metadata': metadata
        }

        # Use upsert to handle re-adding previously deleted accounts
        result = auth_supabase.table('ext_connections')\
            .upsert(encrypt_token_fields(connection_data), on_conflict='user_id,provider,provider_user_id')\
            .execute()

        if not result.data:
            raise ValueError("Failed to create connection")

        new_connection = result.data[0]
        connection_id = new_connection['id']

        logger.info(f"✅ [{provider}] Added secondary email account {_redact_email(provider_email)} for user {user_id[:8]}...")

        # Set up subscriptions/watch and trigger initial sync for the new account.
        if provider == 'google':
            try:
                from api.services.syncs import start_gmail_watch_service_role
                from lib.supabase_client import get_service_role_client
                from google.oauth2.credentials import Credentials
                from googleapiclient.discovery import build
                from api.config import settings

                credentials = Credentials(
                    token=access_token,
                    refresh_token=refresh_token,
                    token_uri='https://oauth2.googleapis.com/token',
                    client_id=settings.google_client_id,
                    client_secret=settings.google_client_secret
                )
                gmail_service = build('gmail', 'v1', credentials=credentials)
                service_supabase = get_service_role_client()

                # Set up Gmail watch for push notifications (this is fast, do it sync)
                watch_result = start_gmail_watch_service_role(
                    user_id, gmail_service, connection_id, service_supabase
                )
                if watch_result.get('success'):
                    logger.info("✅ Gmail watch set up for new account")
                else:
                    logger.warning(f"⚠️ Gmail watch setup failed: {watch_result.get('error')}")

                await _enqueue_or_fallback_google_initial_sync(
                    connection_id=connection_id,
                    user_id=user_id,
                    access_token=access_token,
                    refresh_token=refresh_token,
                    provider_email=provider_email,
                )

            except Exception as e:
                logger.warning(f"⚠️ Failed to set up Gmail watch/sync: {str(e)}")
                # Don't fail the whole operation if watch/sync setup fails

        elif provider == 'microsoft':
            # Set up webhook subscriptions and start initial sync for secondary Microsoft account
            try:
                from api.services.microsoft.microsoft_webhook_provider import create_microsoft_subscription
                from lib.supabase_client import get_service_role_client

                service_supabase = get_service_role_client()

                # Create webhook subscriptions for mail and calendar (async to allow validation)
                try:
                    logger.info(f"📡 [Microsoft] Setting up webhook subscriptions for {_redact_email(provider_email)}...")

                    # Mail subscription (async to allow webhook validation)
                    mail_sub_result = await create_microsoft_subscription(
                        access_token=access_token,
                        resource_type='mail',
                        connection_id=connection_id,
                        user_id=user_id,
                        supabase_client=service_supabase
                    )
                    if mail_sub_result.get('success'):
                        logger.info("✅ [Microsoft] Mail subscription created for secondary account")
                    else:
                        logger.warning(f"⚠️ [Microsoft] Mail subscription failed: {mail_sub_result.get('error')}")

                    # Calendar subscription (async to allow webhook validation)
                    cal_sub_result = await create_microsoft_subscription(
                        access_token=access_token,
                        resource_type='calendar',
                        connection_id=connection_id,
                        user_id=user_id,
                        supabase_client=service_supabase
                    )
                    if cal_sub_result.get('success'):
                        logger.info("✅ [Microsoft] Calendar subscription created for secondary account")
                    else:
                        logger.warning(f"⚠️ [Microsoft] Calendar subscription failed: {cal_sub_result.get('error')}")

                except Exception as e:
                    logger.warning(f"⚠️ [Microsoft] Failed to create subscriptions: {str(e)}")
                    # Don't fail - cron will still sync periodically

                await _enqueue_or_fallback_microsoft_initial_sync(
                    connection_id=connection_id,
                    user_id=user_id,
                    access_token=access_token,
                    refresh_token=refresh_token,
                    token_expires_at=token_expires_at,
                    metadata=metadata,
                    provider_email=provider_email,
                    include_calendar=True,
                )

            except Exception as e:
                logger.warning(f"⚠️ [{provider}] Failed to start initial sync: {str(e)}")
                # Don't fail the whole operation if sync setup fails

        return {
            "message": "Email account added successfully",
            "account": {
                "id": connection_id,
                "provider": provider,
                "provider_email": provider_email,
                "provider_name": metadata.get('full_name') or metadata.get('name'),
                "provider_avatar": metadata.get('picture') or metadata.get('avatar_url'),
                "is_primary": False,
                "account_order": account_order,
                "is_active": True
            }
        }

    @staticmethod
    def remove_email_account(account_id: str, user_id: str) -> bool:
        """
        Remove a secondary email account.

        Hard deletes the connection and all associated emails.
        Uses service_role_client to bypass RLS for this admin operation.

        Returns True if removed, False if not found.
        Raises ValueError if trying to remove primary account.
        """
        from lib.supabase_client import get_service_role_client
        service_supabase = get_service_role_client()

        # Get the account to verify it exists and belongs to user
        account_result = service_supabase.table('ext_connections')\
            .select('id, is_primary, provider_email')\
            .eq('id', account_id)\
            .eq('user_id', user_id)\
            .execute()

        if not account_result.data:
            return False

        account = account_result.data[0]

        # Prevent removal of primary account
        if account.get('is_primary'):
            raise ValueError("Cannot remove primary account")

        provider_email = account.get('provider_email')

        # Deactivate push subscriptions first
        try:
            service_supabase.table('push_subscriptions')\
                .update({'is_active': False})\
                .eq('ext_connection_id', account_id)\
                .execute()
            logger.info(f"✅ Deactivated push subscriptions for account {account_id}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to deactivate push subscriptions: {str(e)}")

        # Hard delete all emails for this account first
        try:
            delete_emails_result = service_supabase.table('emails')\
                .delete()\
                .eq('ext_connection_id', account_id)\
                .execute()
            deleted_count = len(delete_emails_result.data) if delete_emails_result.data else 0
            logger.info(f"🗑️ Deleted {deleted_count} emails for account {account_id}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to delete emails: {str(e)}")

        # Now delete the connection
        delete_result = service_supabase.table('ext_connections')\
            .delete()\
            .eq('id', account_id)\
            .eq('user_id', user_id)\
            .execute()

        if not delete_result.data:
            return False

        logger.info(f"✅ Removed email account {_redact_email(provider_email)} for user {user_id[:8]}...")
        return True

    @staticmethod
    def update_email_account(
        account_id: str,
        user_id: str,
        user_jwt: str,
        update_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update email account properties (currently just account_order).
        """
        auth_supabase = get_authenticated_supabase_client(user_jwt)

        # Only allow updating account_order for now
        allowed_fields = {'account_order'}
        filtered_data = {k: v for k, v in update_data.items() if k in allowed_fields}

        if not filtered_data:
            return {"success": False, "error": "No valid fields to update"}

        result = auth_supabase.table('ext_connections')\
            .update(filtered_data)\
            .eq('id', account_id)\
            .eq('user_id', user_id)\
            .execute()

        if not result.data:
            return {"success": False, "error": "Account not found or not owned by you"}

        logger.info(f"✅ Updated email account {account_id}")

        return {"success": True}
