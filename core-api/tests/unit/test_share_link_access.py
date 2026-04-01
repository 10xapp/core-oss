from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytest
from fastapi import HTTPException
from unittest.mock import AsyncMock


class FakeResponse:
    def __init__(self, data: Any):
        self.data = data


class FakeSupabaseQuery:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]], table_name: str):
        self._state = state
        self._table_name = table_name
        self._filters: List[tuple[str, str, Any]] = []
        self._limit: Optional[int] = None
        self._maybe_single = False

    def _rows(self) -> List[Dict[str, Any]]:
        if self._table_name not in self._state:
            self._state[self._table_name] = []
        return self._state[self._table_name]

    def select(self, _fields: str = "*") -> "FakeSupabaseQuery":
        return self

    def eq(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append(("eq", field, value))
        return self

    def ilike(self, field: str, value: Any) -> "FakeSupabaseQuery":
        raise AssertionError(f"ilike() should not be used — use eq() to avoid wildcard injection (field={field})")

    def limit(self, value: int) -> "FakeSupabaseQuery":
        self._limit = value
        return self

    def maybe_single(self) -> "FakeSupabaseQuery":
        self._maybe_single = True
        return self

    def _matches(self, row: Dict[str, Any]) -> bool:
        for op, field, value in self._filters:
            row_value = row.get(field)
            if op == "eq" and row_value != value:
                return False
            if op == "ilike":
                if row_value is None or str(row_value).lower() != str(value).lower():
                    return False
        return True

    async def execute(self) -> FakeResponse:
        await asyncio.sleep(0)
        rows = [dict(row) for row in self._rows() if self._matches(row)]
        if self._limit is not None:
            rows = rows[: self._limit]
        if self._maybe_single:
            return FakeResponse(rows[0] if rows else None)
        return FakeResponse(rows)


class FakePublicSupabaseClient:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]]):
        self._state = state

    def table(self, table_name: str) -> FakeSupabaseQuery:
        return FakeSupabaseQuery(self._state, table_name)


class FakeRpcResult:
    def __init__(self, payload: Any):
        self._payload = payload

    async def execute(self) -> FakeResponse:
        await asyncio.sleep(0)
        return FakeResponse(self._payload)


class FakeAuthenticatedClient:
    def __init__(self, payload: Any):
        self._payload = payload

    def rpc(self, name: str, params: Dict[str, Any]) -> FakeRpcResult:
        assert name == "resolve_share_link_grant"
        assert "p_link_token" in params
        return FakeRpcResult(self._payload)


@pytest.mark.asyncio
async def test_get_public_shared_resource_uses_permissions_table_lookup(monkeypatch):
    from api.services.permissions import public as public_module

    expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    state = {
        "permissions": [
            {
                "grantee_type": "link",
                "link_token": "token-123",
                "link_slug": "docs-link",
                "resource_type": "document",
                "resource_id": "doc-1",
                "permission": "read",
                "granted_by": "user-1",
                "expires_at": expires_at,
            }
        ],
        "users": [
            {
                "id": "user-1",
                "name": "Jay",
                "avatar_url": "avatar.png",
            }
        ],
        "documents": [
            {
                "id": "doc-1",
                "title": "Quarterly Plan",
                "content": "Secret",
                "is_folder": False,
                "created_at": "2026-03-31T00:00:00+00:00",
                "updated_at": "2026-03-31T00:00:00+00:00",
                "file": {"r2_key": "docs/doc-1.png", "file_type": "image/png"},
            }
        ],
    }

    monkeypatch.setattr(
        public_module,
        "get_async_service_role_client",
        AsyncMock(return_value=FakePublicSupabaseClient(state)),
    )
    monkeypatch.setattr(public_module, "_enrich_documents_with_image_urls", lambda docs: None)

    result = await public_module.get_public_shared_resource("token-123")

    assert result["resource_type"] == "document"
    assert result["resource_id"] == "doc-1"
    assert result["permission"] == "read"
    assert result["shared_by"]["name"] == "Jay"
    assert result["shared_by"]["avatar_url"] == "avatar.png"
    assert result["document"]["id"] == "doc-1"
    assert "file" not in result["document"]
    assert "is_folder" not in result["document"]


@pytest.mark.asyncio
async def test_get_public_shared_resource_rejects_expired_links(monkeypatch):
    from api.services.permissions import public as public_module

    expired_at = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    state = {
        "permissions": [
            {
                "grantee_type": "link",
                "link_token": "expired-link",
                "resource_type": "document",
                "resource_id": "doc-1",
                "permission": "read",
                "granted_by": "user-1",
                "expires_at": expired_at,
            }
        ],
        "documents": [
            {
                "id": "doc-1",
                "title": "Quarterly Plan",
                "content": "Secret",
                "is_folder": False,
                "created_at": "2026-03-31T00:00:00+00:00",
                "updated_at": "2026-03-31T00:00:00+00:00",
                "file": {"r2_key": "docs/doc-1.png", "file_type": "image/png"},
            }
        ],
    }

    monkeypatch.setattr(
        public_module,
        "get_async_service_role_client",
        AsyncMock(return_value=FakePublicSupabaseClient(state)),
    )

    with pytest.raises(HTTPException) as exc_info:
        await public_module.get_public_shared_resource("expired-link")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_public_shared_resource_falls_back_to_slug_lookup(monkeypatch):
    from api.services.permissions import public as public_module

    expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    state = {
        "permissions": [
            {
                "grantee_type": "link",
                "link_token": "raw-uuid-token",
                "link_slug": "docs-link",
                "resource_type": "document",
                "resource_id": "doc-1",
                "permission": "read",
                "granted_by": "user-1",
                "expires_at": expires_at,
            }
        ],
        "users": [{"id": "user-1", "name": "Jay", "avatar_url": "avatar.png"}],
        "documents": [
            {
                "id": "doc-1",
                "title": "Quarterly Plan",
                "content": "Secret",
                "is_folder": False,
                "created_at": "2026-03-31T00:00:00+00:00",
                "updated_at": "2026-03-31T00:00:00+00:00",
                "file": {"r2_key": "docs/doc-1.png", "file_type": "image/png"},
            }
        ],
    }

    monkeypatch.setattr(
        public_module,
        "get_async_service_role_client",
        AsyncMock(return_value=FakePublicSupabaseClient(state)),
    )
    monkeypatch.setattr(public_module, "_enrich_documents_with_image_urls", lambda docs: None)

    # Lookup by slug (not the raw token) — exercises the fallback branch
    result = await public_module.get_public_shared_resource("docs-link")

    assert result["resource_type"] == "document"
    assert result["resource_id"] == "doc-1"
    assert result["permission"] == "read"
    assert result["shared_by"]["name"] == "Jay"


@pytest.mark.asyncio
async def test_resolve_share_link_uses_context_workspace_when_rpc_payload_trimmed(monkeypatch):
    from api.services.permissions import links as links_module

    monkeypatch.setattr(
        links_module,
        "get_authenticated_async_client",
        AsyncMock(
            return_value=FakeAuthenticatedClient(
                {
                    "resource_type": "document",
                    "resource_id": "doc-1",
                    "permission": "write",
                }
            )
        ),
    )
    monkeypatch.setattr(
        links_module,
        "resolve_resource_context",
        AsyncMock(
            return_value={
                "resource_type": "document",
                "workspace_id": "ws-1",
                "workspace_app_id": "app-1",
                "title": "Quarterly Plan",
            }
        ),
    )

    result = await links_module.resolve_share_link(
        user_id="user-1",
        user_jwt="jwt",
        token="token-123",
    )

    assert result == {
        "resource_type": "document",
        "resource_id": "doc-1",
        "workspace_id": "ws-1",
        "workspace_app_id": "app-1",
        "app_type": "files",
        "title": "Quarterly Plan",
        "permission": "write",
    }
