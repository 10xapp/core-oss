from __future__ import annotations

from typing import Any, Dict, List

import pytest
from unittest.mock import AsyncMock


class FakeResponse:
    def __init__(self, data: Any):
        self.data = data


class FakeSupabaseQuery:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]], table_name: str):
        self._state = state
        self._table_name = table_name
        self._payload: Dict[str, Any] | None = None

    def upsert(self, payload: Dict[str, Any], on_conflict: str | None = None) -> "FakeSupabaseQuery":
        self._payload = dict(payload)
        return self

    async def execute(self) -> FakeResponse:
        table_rows = self._state.setdefault(self._table_name, [])
        row = dict(self._payload or {})
        row.setdefault("id", f"{self._table_name}-1")
        table_rows.append(row)
        return FakeResponse([row])


class FakeSupabaseClient:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]]):
        self._state = state

    def table(self, table_name: str) -> FakeSupabaseQuery:
        return FakeSupabaseQuery(self._state, table_name)


@pytest.fixture
def share_module():
    from api.services.permissions import share

    return share


@pytest.mark.asyncio
async def test_share_resource_emits_file_shared_for_documents(monkeypatch, share_module):
    state = {"permissions": []}
    create_notification_mock = AsyncMock()

    monkeypatch.setattr(
        share_module,
        "get_authenticated_async_client",
        AsyncMock(return_value=FakeSupabaseClient(state)),
    )
    monkeypatch.setattr(share_module, "assert_can_manage_shares", AsyncMock())
    monkeypatch.setattr(
        share_module,
        "get_user_by_email",
        AsyncMock(return_value={"id": "user-2", "email": "target@example.com"}),
    )
    monkeypatch.setattr(
        share_module,
        "resolve_resource_context",
        AsyncMock(
            return_value={
                "resource_type": "document",
                "workspace_id": "ws-1",
                "title": "Quarterly Plan",
                "document_id": "doc-1",
            }
        ),
    )
    monkeypatch.setattr(
        share_module,
        "get_actor_info",
        AsyncMock(return_value={"actor_name": "Jay", "actor_avatar": "avatar.png"}),
    )
    monkeypatch.setattr(share_module, "create_notification", create_notification_mock)

    result = await share_module.share_resource(
        user_id="user-1",
        user_jwt="jwt",
        resource_type="document",
        resource_id="doc-1",
        grantee_email="target@example.com",
        permission="write",
    )

    assert result["resource_type"] == "document"
    assert result["resource_id"] == "doc-1"
    create_notification_mock.assert_awaited_once()
    kwargs = create_notification_mock.await_args.kwargs
    assert kwargs["type"] == share_module.NotificationType.FILE_SHARED
    assert kwargs["title"] == 'Jay shared "Quarterly Plan" with you'
    assert kwargs["body"] == "Can edit"
    assert kwargs["resource_type"] == "document"
    assert kwargs["resource_id"] == "doc-1"
    assert kwargs["workspace_id"] == "ws-1"
    assert kwargs["data"]["permission"] == "write"
    assert kwargs["data"]["resource_title"] == "Quarterly Plan"
    assert kwargs["data"]["workspace_id"] == "ws-1"
    assert kwargs["data"]["document_id"] == "doc-1"


@pytest.mark.asyncio
async def test_share_resource_keeps_permission_granted_for_non_file_resources(monkeypatch, share_module):
    create_notification_mock = AsyncMock()

    monkeypatch.setattr(
        share_module,
        "get_authenticated_async_client",
        AsyncMock(return_value=FakeSupabaseClient({"permissions": []})),
    )
    monkeypatch.setattr(share_module, "assert_can_manage_shares", AsyncMock())
    monkeypatch.setattr(
        share_module,
        "get_user_by_email",
        AsyncMock(return_value={"id": "user-2", "email": "target@example.com"}),
    )
    monkeypatch.setattr(
        share_module,
        "resolve_resource_context",
        AsyncMock(
            return_value={
                "resource_type": "project_board",
                "workspace_id": "ws-1",
                "title": "Roadmap",
                "document_id": None,
            }
        ),
    )
    monkeypatch.setattr(
        share_module,
        "get_actor_info",
        AsyncMock(return_value={"actor_name": "Jay", "actor_avatar": None}),
    )
    monkeypatch.setattr(share_module, "create_notification", create_notification_mock)

    await share_module.share_resource(
        user_id="user-1",
        user_jwt="jwt",
        resource_type="project_board",
        resource_id="board-1",
        grantee_email="target@example.com",
        permission="read",
    )

    kwargs = create_notification_mock.await_args.kwargs
    assert kwargs["type"] == share_module.NotificationType.PERMISSION_GRANTED
    assert kwargs["body"] == "Can view"
    assert kwargs["data"]["document_id"] is None


@pytest.mark.asyncio
async def test_batch_share_resource_emits_file_shared_for_documents(monkeypatch, share_module):
    create_notification_mock = AsyncMock()

    monkeypatch.setattr(
        share_module,
        "get_authenticated_async_client",
        AsyncMock(return_value=FakeSupabaseClient({"permissions": []})),
    )
    monkeypatch.setattr(share_module, "assert_can_manage_shares", AsyncMock())
    monkeypatch.setattr(
        share_module,
        "get_user_by_email",
        AsyncMock(return_value={"id": "user-2", "email": "target@example.com"}),
    )
    monkeypatch.setattr(
        share_module,
        "resolve_resource_context",
        AsyncMock(
            return_value={
                "resource_type": "document",
                "workspace_id": "ws-1",
                "title": "Quarterly Plan",
                "document_id": "doc-1",
            }
        ),
    )
    monkeypatch.setattr(
        share_module,
        "get_actor_info",
        AsyncMock(return_value={"actor_name": "Jay", "actor_avatar": None}),
    )
    monkeypatch.setattr(share_module, "create_notification", create_notification_mock)

    result = await share_module.batch_share_resource(
        user_id="user-1",
        user_jwt="jwt",
        resource_type="document",
        resource_id="doc-1",
        grants=[{"email": "target@example.com", "permission": "admin"}],
    )

    assert len(result) == 1
    kwargs = create_notification_mock.await_args.kwargs
    assert kwargs["type"] == share_module.NotificationType.FILE_SHARED
    assert kwargs["body"] == "Full access"
    assert kwargs["recipients"] == ["user-2"]
