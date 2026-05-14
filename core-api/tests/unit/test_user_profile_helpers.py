from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_attach_public_profiles_populates_rows_and_fallbacks(monkeypatch):
    from api.services.users import search as users_search

    monkeypatch.setattr(
        users_search,
        "get_public_profiles_by_ids",
        AsyncMock(
            return_value={
                "user-1": {"id": "user-1", "name": "Alice", "avatar_url": "a.png"},
                "user-2": {"id": "user-2", "name": "Bob", "avatar_url": None},
            }
        ),
    )

    rows = [
        {"user_id": "user-1"},
        {"user_id": "user-2"},
        {"user_id": "user-3"},
        {},
    ]

    await users_search.attach_public_profiles(rows)

    assert rows[0]["user"]["name"] == "Alice"
    assert rows[1]["user"]["name"] == "Bob"
    assert rows[2]["user"] == {"id": "user-3"}
    assert rows[3]["user"] is None


@pytest.mark.asyncio
async def test_get_auth_user_by_email_reads_auth_store(monkeypatch):
    from api.services.users import search as users_search

    class FakeAdmin:
        async def list_users(self, page=None, per_page=None):
            assert page == 1
            assert per_page == 200
            return [
                SimpleNamespace(id="user-1", email="owner@example.com"),
                SimpleNamespace(id="user-2", email="other@example.com"),
            ]

    fake_client = SimpleNamespace(auth=SimpleNamespace(admin=FakeAdmin()))

    monkeypatch.setattr(
        users_search,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )
    monkeypatch.setattr(
        users_search,
        "get_users_by_ids",
        AsyncMock(
            return_value={
                "user-1": {
                    "id": "user-1",
                    "email": "stale@example.com",
                    "name": "Owner",
                    "avatar_url": "avatar.png",
                }
            }
        ),
    )

    result = await users_search.get_auth_user_by_email("owner@example.com")

    assert result == {
        "id": "user-1",
        "email": "owner@example.com",
        "name": "Owner",
        "avatar_url": "avatar.png",
    }
