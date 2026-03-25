"""
Supabase client for core-api
Provides Supabase client instances with proper authentication.

Includes both sync and async clients:
- Sync clients: For cron jobs and background tasks
- Async clients: For FastAPI endpoints (non-blocking I/O)
"""
import asyncio
import os
import threading
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Dict, Optional

from supabase import create_client, Client, acreate_client, AsyncClient

_supabase_client: Optional[Client] = None
_service_role_client: Optional[Client] = None
_async_supabase_client: Optional[AsyncClient] = None
_async_service_role_client: Optional[AsyncClient] = None
_supabase_client_lock = threading.Lock()
_service_role_client_lock = threading.Lock()
_async_client_lock = asyncio.Lock()
_async_service_role_client_lock = asyncio.Lock()


@dataclass
class _SupabaseRequestScope:
    authenticated_sync_clients: Dict[str, Client] = field(default_factory=dict)
    authenticated_async_clients: Dict[str, AsyncClient] = field(default_factory=dict)
    authenticated_async_locks: Dict[str, asyncio.Lock] = field(default_factory=dict)


_request_scope_var: ContextVar[Optional[_SupabaseRequestScope]] = ContextVar(
    "supabase_request_scope",
    default=None,
)


def start_supabase_request_scope() -> Token:
    """Start a fresh request-local Supabase scope."""
    return _request_scope_var.set(_SupabaseRequestScope())


def reset_supabase_request_scope(token: Token) -> None:
    """Reset the current request-local Supabase scope."""
    _request_scope_var.reset(token)


def _get_request_scope() -> Optional[_SupabaseRequestScope]:
    return _request_scope_var.get()


def _get_anon_client_config() -> tuple[str, str]:
    from api.config import settings

    supabase_url = settings.supabase_url or os.getenv("SUPABASE_URL")
    supabase_key = settings.supabase_anon_key or os.getenv("SUPABASE_ANON_KEY")

    if not supabase_url or not supabase_key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables or .env file"
        )

    return supabase_url, supabase_key


def _get_service_role_config() -> tuple[str, str]:
    from api.config import settings

    supabase_url = settings.supabase_url or os.getenv("SUPABASE_URL")
    supabase_service_key = (
        settings.supabase_service_role_key or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    )

    if not supabase_url:
        raise ValueError("SUPABASE_URL must be set in environment variables or .env file")

    if not supabase_service_key:
        raise ValueError(
            "SUPABASE_SERVICE_ROLE_KEY must be set in environment variables or .env file. "
            "This is required for server-to-server operations like cron jobs. "
            "Find your service role key in Supabase Dashboard → Settings → API"
        )

    return supabase_url, supabase_service_key


def _create_authenticated_sync_client(user_jwt: str) -> Client:
    supabase_url, supabase_key = _get_anon_client_config()

    client = create_client(supabase_url, supabase_key)
    client.postgrest.auth(user_jwt)
    return client


async def _create_authenticated_async_client(user_jwt: str) -> AsyncClient:
    supabase_url, supabase_key = _get_anon_client_config()

    client = await acreate_client(supabase_url, supabase_key)
    client.postgrest.auth(user_jwt)
    return client


def get_supabase_client() -> Client:
    """
    Get or create the Supabase client singleton with anon key.
    Use this for non-authenticated operations only.
    
    Returns:
        Client: The Supabase client instance with anon key
    """
    global _supabase_client

    if _supabase_client is not None:
        return _supabase_client

    with _supabase_client_lock:
        if _supabase_client is None:
            supabase_url, supabase_key = _get_anon_client_config()
            _supabase_client = create_client(supabase_url, supabase_key)

    return _supabase_client


def get_authenticated_supabase_client(user_jwt: str) -> Client:
    """
    Create a Supabase client authenticated as a specific user.
    This client will respect RLS policies for the authenticated user.
    
    Args:
        user_jwt: The user's Supabase JWT access token
        
    Returns:
        Client: Supabase client authenticated as the user
    """
    scope = _get_request_scope()
    if scope is None:
        return _create_authenticated_sync_client(user_jwt)

    client = scope.authenticated_sync_clients.get(user_jwt)
    if client is not None:
        return client

    client = _create_authenticated_sync_client(user_jwt)
    scope.authenticated_sync_clients[user_jwt] = client
    return client


def get_service_role_client() -> Client:
    """
    Create a Supabase client with service role key.
    This client bypasses RLS policies and should ONLY be used for:
    - Cron jobs that need to access data across all users
    - Background tasks that run server-to-server
    - Administrative operations
    
    ⚠️ WARNING: Use with extreme caution! This client has full database access.
    
    Returns:
        Client: Supabase client with service role privileges
    """
    global _service_role_client

    if _service_role_client is not None:
        return _service_role_client

    with _service_role_client_lock:
        if _service_role_client is None:
            supabase_url, supabase_service_key = _get_service_role_config()
            _service_role_client = create_client(supabase_url, supabase_service_key)

    return _service_role_client


# Convenience alias for anon client
supabase = get_supabase_client()


# =============================================================================
# ASYNC CLIENTS - For FastAPI endpoints (non-blocking I/O)
# =============================================================================

async def get_async_supabase_client() -> AsyncClient:
    """
    Get or create the async Supabase client singleton with anon key.
    Use this for non-authenticated async operations only.

    Returns:
        AsyncClient: The async Supabase client instance with anon key
    """
    global _async_supabase_client

    # Fast path: return existing client without lock
    if _async_supabase_client is not None:
        return _async_supabase_client

    # Slow path: acquire lock and double-check
    async with _async_client_lock:
        # Double-check after acquiring lock
        if _async_supabase_client is None:
            from api.config import settings

            supabase_url = settings.supabase_url or os.getenv('SUPABASE_URL')
            supabase_key = settings.supabase_anon_key or os.getenv('SUPABASE_ANON_KEY')

            if not supabase_url or not supabase_key:
                raise ValueError(
                    "SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables or .env file"
                )

            _async_supabase_client = await acreate_client(supabase_url, supabase_key)

    return _async_supabase_client


async def get_authenticated_async_client(user_jwt: str) -> AsyncClient:
    """
    Create an async Supabase client authenticated as a specific user.
    This client will respect RLS policies for the authenticated user.

    Args:
        user_jwt: The user's Supabase JWT access token

    Returns:
        AsyncClient: Async Supabase client authenticated as the user
    """
    scope = _get_request_scope()
    if scope is None:
        return await _create_authenticated_async_client(user_jwt)

    client = scope.authenticated_async_clients.get(user_jwt)
    if client is not None:
        return client

    lock = scope.authenticated_async_locks.get(user_jwt)
    if lock is None:
        lock = asyncio.Lock()
        scope.authenticated_async_locks[user_jwt] = lock

    async with lock:
        client = scope.authenticated_async_clients.get(user_jwt)
        if client is None:
            client = await _create_authenticated_async_client(user_jwt)
            scope.authenticated_async_clients[user_jwt] = client

    return client


async def get_async_service_role_client() -> AsyncClient:
    """
    Create an async Supabase client with service role key.
    This client bypasses RLS policies and should ONLY be used for:
    - Async background tasks that need cross-user access
    - Webhook handlers that need to process data for any user

    ⚠️ WARNING: Use with extreme caution! This client has full database access.

    Returns:
        AsyncClient: Async Supabase client with service role privileges
    """
    global _async_service_role_client

    if _async_service_role_client is not None:
        return _async_service_role_client

    async with _async_service_role_client_lock:
        if _async_service_role_client is None:
            supabase_url, supabase_service_key = _get_service_role_config()
            _async_service_role_client = await acreate_client(
                supabase_url,
                supabase_service_key,
            )

    return _async_service_role_client

