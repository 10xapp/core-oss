"""
Tests for supabase_client module.

Focus: singleton reuse, request-scoped authenticated reuse, and middleware
integration for the phase 1 Supabase client optimization.
"""
import asyncio
import os
import threading
from contextvars import ContextVar
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

TEST_SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiYW5vbiIsImlzcyI6InRlc3QifQ.signature"
)
TEST_SUPABASE_SERVICE_ROLE_KEY = (
    "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIiwiaXNzIjoidGVzdCJ9.signature"
)

os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_ANON_KEY", TEST_SUPABASE_ANON_KEY)
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", TEST_SUPABASE_SERVICE_ROLE_KEY)


def _mock_settings() -> MagicMock:
    settings = MagicMock()
    settings.supabase_url = "https://test.supabase.co"
    settings.supabase_anon_key = TEST_SUPABASE_ANON_KEY
    settings.supabase_service_role_key = TEST_SUPABASE_SERVICE_ROLE_KEY
    return settings


def _mock_client() -> MagicMock:
    client = MagicMock()
    client.postgrest = MagicMock()
    return client


@pytest.fixture
def module():
    import lib.supabase_client as module

    with patch.object(module, "_supabase_client", None):
        with patch.object(module, "_service_role_client", None):
            with patch.object(module, "_async_supabase_client", None):
                with patch.object(module, "_async_service_role_client", None):
                    with patch.object(module, "_supabase_client_lock", threading.Lock()):
                        with patch.object(module, "_service_role_client_lock", threading.Lock()):
                            with patch.object(module, "_async_client_lock", asyncio.Lock()):
                                with patch.object(
                                    module,
                                    "_async_service_role_client_lock",
                                    asyncio.Lock(),
                                ):
                                    with patch.object(
                                        module,
                                        "_request_scope_var",
                                        ContextVar(
                                            "supabase_request_scope_test",
                                            default=None,
                                        ),
                                    ):
                                        yield module


class TestGetAsyncSupabaseClient:
    async def test_concurrent_calls_only_create_one_client(self, module):
        creation_count = 0
        created_client = _mock_client()

        async def slow_create_client(*args, **kwargs):
            nonlocal creation_count
            creation_count += 1
            await asyncio.sleep(0.01)
            return created_client

        with patch("lib.supabase_client.acreate_client", side_effect=slow_create_client):
            with patch("api.config.settings", _mock_settings()):
                results = await asyncio.gather(
                    module.get_async_supabase_client(),
                    module.get_async_supabase_client(),
                    module.get_async_supabase_client(),
                    module.get_async_supabase_client(),
                    module.get_async_supabase_client(),
                )

        assert all(result is created_client for result in results)
        assert creation_count == 1


class TestServiceRoleClients:
    def test_sync_service_role_returns_same_instance_across_calls(self, module):
        created_client = _mock_client()

        with patch("lib.supabase_client.create_client", return_value=created_client) as mock_create:
            with patch("api.config.settings", _mock_settings()):
                client_one = module.get_service_role_client()
                client_two = module.get_service_role_client()

        assert client_one is client_two is created_client
        assert mock_create.call_count == 1

    async def test_async_service_role_singleton_under_concurrency(self, module):
        creation_count = 0
        created_client = _mock_client()

        async def slow_create_client(*args, **kwargs):
            nonlocal creation_count
            creation_count += 1
            await asyncio.sleep(0.01)
            return created_client

        with patch("lib.supabase_client.acreate_client", side_effect=slow_create_client):
            with patch("api.config.settings", _mock_settings()):
                results = await asyncio.gather(
                    module.get_async_service_role_client(),
                    module.get_async_service_role_client(),
                    module.get_async_service_role_client(),
                    module.get_async_service_role_client(),
                )

        assert all(result is created_client for result in results)
        assert creation_count == 1


class TestGetAuthenticatedAsyncClient:
    async def test_same_jwt_same_request_scope_returns_same_client(self, module):
        created_client = _mock_client()

        with patch("lib.supabase_client.acreate_client", new_callable=AsyncMock) as mock_create:
            with patch("api.config.settings", _mock_settings()):
                mock_create.return_value = created_client
                scope_token = module.start_supabase_request_scope()
                try:
                    client_one = await module.get_authenticated_async_client("same-jwt")
                    client_two = await module.get_authenticated_async_client("same-jwt")
                finally:
                    module.reset_supabase_request_scope(scope_token)

        assert client_one is client_two is created_client
        created_client.postgrest.auth.assert_called_once_with("same-jwt")
        assert mock_create.await_count == 1

    async def test_different_jwts_same_request_scope_return_different_clients(self, module):
        client_one = _mock_client()
        client_two = _mock_client()

        with patch("lib.supabase_client.acreate_client", new_callable=AsyncMock) as mock_create:
            with patch("api.config.settings", _mock_settings()):
                mock_create.side_effect = [client_one, client_two]
                scope_token = module.start_supabase_request_scope()
                try:
                    result_one = await module.get_authenticated_async_client("jwt-1")
                    result_two = await module.get_authenticated_async_client("jwt-2")
                finally:
                    module.reset_supabase_request_scope(scope_token)

        assert result_one is client_one
        assert result_two is client_two
        assert result_one is not result_two
        assert mock_create.await_count == 2

    async def test_no_request_scope_creates_fresh_client_per_call(self, module):
        client_one = _mock_client()
        client_two = _mock_client()

        with patch("lib.supabase_client.acreate_client", new_callable=AsyncMock) as mock_create:
            with patch("api.config.settings", _mock_settings()):
                mock_create.side_effect = [client_one, client_two]
                result_one = await module.get_authenticated_async_client("same-jwt")
                result_two = await module.get_authenticated_async_client("same-jwt")

        assert result_one is client_one
        assert result_two is client_two
        assert result_one is not result_two
        assert mock_create.await_count == 2

    async def test_same_jwt_concurrent_requests_in_one_scope_create_one_client(self, module):
        creation_count = 0
        created_client = _mock_client()

        async def slow_create_client(*args, **kwargs):
            nonlocal creation_count
            creation_count += 1
            await asyncio.sleep(0.01)
            return created_client

        with patch("lib.supabase_client.acreate_client", side_effect=slow_create_client):
            with patch("api.config.settings", _mock_settings()):
                scope_token = module.start_supabase_request_scope()
                try:
                    results = await asyncio.gather(
                        module.get_authenticated_async_client("same-jwt"),
                        module.get_authenticated_async_client("same-jwt"),
                        module.get_authenticated_async_client("same-jwt"),
                        module.get_authenticated_async_client("same-jwt"),
                    )
                finally:
                    module.reset_supabase_request_scope(scope_token)

        assert all(result is created_client for result in results)
        assert creation_count == 1


class TestGetAuthenticatedSupabaseClient:
    def test_same_jwt_same_request_scope_returns_same_client(self, module):
        created_client = _mock_client()

        with patch("lib.supabase_client.create_client", return_value=created_client) as mock_create:
            with patch("api.config.settings", _mock_settings()):
                scope_token = module.start_supabase_request_scope()
                try:
                    client_one = module.get_authenticated_supabase_client("same-jwt")
                    client_two = module.get_authenticated_supabase_client("same-jwt")
                finally:
                    module.reset_supabase_request_scope(scope_token)

        assert client_one is client_two is created_client
        created_client.postgrest.auth.assert_called_once_with("same-jwt")
        assert mock_create.call_count == 1

    def test_different_jwts_same_request_scope_return_different_clients(self, module):
        client_one = _mock_client()
        client_two = _mock_client()

        with patch("lib.supabase_client.create_client") as mock_create:
            with patch("api.config.settings", _mock_settings()):
                mock_create.side_effect = [client_one, client_two]
                scope_token = module.start_supabase_request_scope()
                try:
                    result_one = module.get_authenticated_supabase_client("jwt-1")
                    result_two = module.get_authenticated_supabase_client("jwt-2")
                finally:
                    module.reset_supabase_request_scope(scope_token)

        assert result_one is client_one
        assert result_two is client_two
        assert result_one is not result_two
        assert mock_create.call_count == 2

    def test_no_request_scope_creates_fresh_client_per_call(self, module):
        client_one = _mock_client()
        client_two = _mock_client()

        with patch("lib.supabase_client.create_client") as mock_create:
            with patch("api.config.settings", _mock_settings()):
                mock_create.side_effect = [client_one, client_two]
                result_one = module.get_authenticated_supabase_client("same-jwt")
                result_two = module.get_authenticated_supabase_client("same-jwt")

        assert result_one is client_one
        assert result_two is client_two
        assert result_one is not result_two
        assert mock_create.call_count == 2


class TestRequestScopeBehavior:
    async def test_request_scopes_are_isolated(self, module):
        client_one = _mock_client()
        client_two = _mock_client()

        with patch("lib.supabase_client.acreate_client", new_callable=AsyncMock) as mock_create:
            with patch("api.config.settings", _mock_settings()):
                mock_create.side_effect = [client_one, client_two]

                first_scope = module.start_supabase_request_scope()
                try:
                    first_result = await module.get_authenticated_async_client("same-jwt")
                finally:
                    module.reset_supabase_request_scope(first_scope)

                second_scope = module.start_supabase_request_scope()
                try:
                    second_result = await module.get_authenticated_async_client("same-jwt")
                finally:
                    module.reset_supabase_request_scope(second_scope)

        assert first_result is client_one
        assert second_result is client_two
        assert first_result is not second_result
        assert mock_create.await_count == 2

    async def test_detached_task_created_in_scope_can_finish_after_scope_reset(self, module):
        created_client = _mock_client()
        task_ready = asyncio.Event()
        continue_task = asyncio.Event()

        async def detached_worker():
            task_ready.set()
            await continue_task.wait()
            return await module.get_authenticated_async_client("same-jwt")

        with patch("lib.supabase_client.acreate_client", new_callable=AsyncMock) as mock_create:
            with patch("api.config.settings", _mock_settings()):
                mock_create.return_value = created_client
                scope_token = module.start_supabase_request_scope()
                try:
                    request_client = await module.get_authenticated_async_client("same-jwt")
                    task = asyncio.create_task(detached_worker())
                    await task_ready.wait()
                finally:
                    module.reset_supabase_request_scope(scope_token)

                continue_task.set()
                detached_client = await task

        assert request_client is created_client
        assert detached_client is created_client
        assert mock_create.await_count == 1


class TestMiddlewareIntegration:
    def test_request_middleware_reuses_client_once_per_request(self, module, app):
        request_path = f"/__test__/supabase-scope-{uuid4().hex}"
        first_request_client = _mock_client()
        second_request_client = _mock_client()

        @app.get(request_path)
        async def request_scope_probe():
            client_one = await module.get_authenticated_async_client("request-jwt")
            client_two = await module.get_authenticated_async_client("request-jwt")
            return {
                "same_within_request": client_one is client_two,
                "client_id": id(client_one),
            }

        with patch("lib.supabase_client.acreate_client", new_callable=AsyncMock) as mock_create:
            with patch("api.config.settings", _mock_settings()):
                mock_create.side_effect = [first_request_client, second_request_client]
                with TestClient(app) as client:
                    first_response = client.get(request_path)
                    second_response = client.get(request_path)

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json()["same_within_request"] is True
        assert second_response.json()["same_within_request"] is True
        assert first_response.json()["client_id"] != second_response.json()["client_id"]
        assert mock_create.await_count == 2
