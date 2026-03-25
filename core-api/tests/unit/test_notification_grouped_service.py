"""Unit tests for grouped notification primitives."""

from __future__ import annotations

from typing import Any, Dict, List
from uuid import uuid4

import pytest
from unittest.mock import AsyncMock


class FakeResponse:
    def __init__(self, data: Any):
        self.data = data


class FakeSupabaseQuery:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]], table_name: str):
        self._state = state
        self._table_name = table_name
        self._op = "select"
        self._payload: Any = None
        self._filters: List[tuple[str, str, Any]] = []

    def _rows(self) -> List[Dict[str, Any]]:
        return self._state.setdefault(self._table_name, [])

    def _matches(self, row: Dict[str, Any]) -> bool:
        for op, field, value in self._filters:
            if op == "eq" and row.get(field) != value:
                return False
        return True

    def update(self, payload: Dict[str, Any]) -> "FakeSupabaseQuery":
        self._op = "update"
        self._payload = payload
        return self

    def insert(self, payload: Dict[str, Any]) -> "FakeSupabaseQuery":
        self._op = "insert"
        self._payload = payload
        return self

    def eq(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append(("eq", field, value))
        return self

    async def execute(self) -> FakeResponse:
        table_rows = self._rows()

        if self._op == "insert":
            payload = dict(self._payload)
            for row in table_rows:
                if (
                    payload.get("group_key") is not None
                    and row.get("user_id") == payload.get("user_id")
                    and row.get("type") == payload.get("type")
                    and row.get("group_key") == payload.get("group_key")
                    and row.get("archived") is False
                ):
                    raise Exception("duplicate key value violates unique constraint")

            payload.setdefault("id", str(uuid4()))
            table_rows.append(payload)
            return FakeResponse([dict(payload)])

        updated: List[Dict[str, Any]] = []
        for row in table_rows:
            if self._matches(row):
                row.update(self._payload or {})
                updated.append(dict(row))
        return FakeResponse(updated)


class FakeSupabaseClient:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]]):
        self._state = state

    def table(self, table_name: str) -> FakeSupabaseQuery:
        return FakeSupabaseQuery(self._state, table_name)


class DuplicateInsertThenUpdateClient:
    """Simulates the grouped-upsert race: insert loses after initial miss."""

    def __init__(self, state: Dict[str, List[Dict[str, Any]]]):
        self._state = state
        self._update_calls = 0

    def table(self, table_name: str):
        if table_name != "notifications":
            return FakeSupabaseQuery(self._state, table_name)
        return DuplicateInsertThenUpdateQuery(self)


class DuplicateInsertThenUpdateQuery:
    def __init__(self, client: DuplicateInsertThenUpdateClient):
        self._client = client
        self._op = "select"
        self._payload: Any = None
        self._filters: List[tuple[str, str, Any]] = []

    def update(self, payload: Dict[str, Any]) -> "DuplicateInsertThenUpdateQuery":
        self._op = "update"
        self._payload = payload
        return self

    def insert(self, payload: Dict[str, Any]) -> "DuplicateInsertThenUpdateQuery":
        self._op = "insert"
        self._payload = payload
        return self

    def eq(self, field: str, value: Any) -> "DuplicateInsertThenUpdateQuery":
        self._filters.append(("eq", field, value))
        return self

    async def execute(self) -> FakeResponse:
        notifications = self._client._state.setdefault("notifications", [])

        if self._op == "update":
            self._client._update_calls += 1
            if self._client._update_calls == 1:
                return FakeResponse([])

            updated: List[Dict[str, Any]] = []
            for row in notifications:
                if (
                    row.get("user_id") == self._filters[0][2]
                    and row.get("type") == self._filters[1][2]
                    and row.get("group_key") == self._filters[2][2]
                    and row.get("archived") == self._filters[3][2]
                ):
                    row.update(self._payload or {})
                    updated.append(dict(row))
            return FakeResponse(updated)

        payload = dict(self._payload)
        notifications.append(
            {
                "id": "notif-race",
                "user_id": payload["user_id"],
                "type": payload["type"],
                "group_key": payload["group_key"],
                "title": "Concurrent title",
                "body": "Concurrent body",
                "read": False,
                "seen": False,
                "archived": False,
                "data": {"unread_count": 2},
                "created_at": "2026-03-19T09:00:00+00:00",
            }
        )
        raise Exception("duplicate key value violates unique constraint")


@pytest.fixture
def grouped_module():
    from api.services.notifications import grouped

    return grouped


@pytest.mark.asyncio
async def test_upsert_grouped_notification_updates_existing_active_row(monkeypatch, grouped_module):
    state = {
        "notifications": [
            {
                "id": "notif-1",
                "user_id": "user-1",
                "workspace_id": "ws-1",
                "type": "message_received",
                "title": "Old title",
                "body": "Old body",
                "resource_type": "channel",
                "resource_id": "chan-1",
                "group_key": "channel:chan-1",
                "actor_id": "actor-1",
                "data": {"unread_count": 1},
                "read": True,
                "seen": True,
                "archived": False,
                "created_at": "2026-03-18T10:00:00+00:00",
            }
        ]
    }

    monkeypatch.setattr(
        grouped_module,
        "get_async_service_role_client",
        AsyncMock(return_value=FakeSupabaseClient(state)),
    )
    monkeypatch.setattr(grouped_module, "should_notify", AsyncMock(return_value=True))

    result = await grouped_module.upsert_grouped_notification(
        user_id="user-1",
        type="message_received",
        group_key="channel:chan-1",
        title="3 unread messages in #design",
        body="Latest message preview",
        actor_id="actor-2",
        data={"unread_count": 3},
    )

    assert result["id"] == "notif-1"
    assert result["title"] == "3 unread messages in #design"
    assert result["body"] == "Latest message preview"
    assert result["actor_id"] == "actor-2"
    assert result["data"] == {"unread_count": 3}
    assert result["read"] is False
    assert result["seen"] is False
    assert result["archived"] is False
    assert result["created_at"] != "2026-03-18T10:00:00+00:00"


@pytest.mark.asyncio
async def test_upsert_grouped_notification_inserts_new_row(monkeypatch, grouped_module):
    state = {"notifications": []}

    monkeypatch.setattr(
        grouped_module,
        "get_async_service_role_client",
        AsyncMock(return_value=FakeSupabaseClient(state)),
    )
    monkeypatch.setattr(grouped_module, "should_notify", AsyncMock(return_value=True))

    result = await grouped_module.upsert_grouped_notification(
        user_id="user-1",
        type="email_received",
        group_key="email:acct-1",
        title="5 new unread emails",
        workspace_id="ws-1",
        resource_type="email_account",
        resource_id="acct-1",
        data={"unread_count": 5},
    )

    assert result["user_id"] == "user-1"
    assert result["type"] == "email_received"
    assert result["group_key"] == "email:acct-1"
    assert result["title"] == "5 new unread emails"
    assert result["workspace_id"] == "ws-1"
    assert result["resource_type"] == "email_account"
    assert result["resource_id"] == "acct-1"
    assert result["data"] == {"unread_count": 5}
    assert result["read"] is False
    assert result["seen"] is False
    assert result["archived"] is False
    assert len(state["notifications"]) == 1


@pytest.mark.asyncio
async def test_upsert_grouped_notification_requires_title_for_insert(monkeypatch, grouped_module):
    monkeypatch.setattr(
        grouped_module,
        "get_async_service_role_client",
        AsyncMock(return_value=FakeSupabaseClient({"notifications": []})),
    )
    monkeypatch.setattr(grouped_module, "should_notify", AsyncMock(return_value=True))

    with pytest.raises(ValueError, match="title is required"):
        await grouped_module.upsert_grouped_notification(
            user_id="user-1",
            type="message_received",
            group_key="channel:chan-1",
        )


@pytest.mark.asyncio
async def test_resolve_grouped_notification_archives_message_rows(monkeypatch, grouped_module):
    state = {
        "notifications": [
            {
                "id": "notif-1",
                "user_id": "user-1",
                "type": "message_received",
                "group_key": "channel:chan-1",
                "read": False,
                "seen": False,
                "archived": False,
            }
        ]
    }

    monkeypatch.setattr(
        grouped_module,
        "get_async_service_role_client",
        AsyncMock(return_value=FakeSupabaseClient(state)),
    )

    result = await grouped_module.resolve_grouped_notification(
        user_id="user-1",
        type="message_received",
        group_key="channel:chan-1",
    )

    assert len(result) == 1
    assert result[0]["read"] is True
    assert result[0]["seen"] is True
    assert result[0]["archived"] is True


@pytest.mark.asyncio
async def test_resolve_grouped_notification_marks_file_edits_read_without_archiving(
    monkeypatch,
    grouped_module,
):
    state = {
        "notifications": [
            {
                "id": "notif-1",
                "user_id": "user-1",
                "type": "file_edited",
                "group_key": "document:doc-1",
                "read": False,
                "seen": False,
                "archived": False,
            }
        ]
    }

    monkeypatch.setattr(
        grouped_module,
        "get_async_service_role_client",
        AsyncMock(return_value=FakeSupabaseClient(state)),
    )

    result = await grouped_module.resolve_grouped_notification(
        user_id="user-1",
        type="file_edited",
        group_key="document:doc-1",
    )

    assert len(result) == 1
    assert result[0]["read"] is True
    assert result[0]["seen"] is True
    assert result[0]["archived"] is False


@pytest.mark.asyncio
async def test_upsert_grouped_notification_skips_muted_users(monkeypatch, grouped_module):
    should_notify_mock = AsyncMock(return_value=False)
    monkeypatch.setattr(grouped_module, "should_notify", should_notify_mock)

    result = await grouped_module.upsert_grouped_notification(
        user_id="user-1",
        type="message_received",
        group_key="channel:chan-1",
        title="3 unread messages in #design",
    )

    assert result is None
    should_notify_mock.assert_awaited_once_with("user-1", "messages", None, channel="in_app")


@pytest.mark.asyncio
async def test_upsert_grouped_notification_skips_actor_self_notifications(monkeypatch, grouped_module):
    should_notify_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(grouped_module, "should_notify", should_notify_mock)

    result = await grouped_module.upsert_grouped_notification(
        user_id="user-1",
        type="message_received",
        group_key="channel:chan-1",
        title="3 unread messages in #design",
        actor_id="user-1",
    )

    assert result is None
    should_notify_mock.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_grouped_notification_retries_after_duplicate_insert_race(
    monkeypatch,
    grouped_module,
):
    state = {"notifications": []}
    monkeypatch.setattr(
        grouped_module,
        "get_async_service_role_client",
        AsyncMock(return_value=DuplicateInsertThenUpdateClient(state)),
    )
    monkeypatch.setattr(grouped_module, "should_notify", AsyncMock(return_value=True))

    result = await grouped_module.upsert_grouped_notification(
        user_id="user-1",
        type="message_received",
        group_key="channel:chan-1",
        title="4 unread messages in #design",
        body="Latest preview",
        data={"unread_count": 4},
    )

    assert result is not None
    assert result["id"] == "notif-race"
    assert result["title"] == "4 unread messages in #design"
    assert result["body"] == "Latest preview"
    assert result["data"] == {"unread_count": 4}
    assert result["read"] is False
    assert result["seen"] is False
    assert result["archived"] is False


def test_get_grouped_resolution_requires_explicit_mapping(grouped_module):
    with pytest.raises(ValueError, match="missing an explicit resolution mode"):
        grouped_module.get_grouped_resolution("task_assigned")
