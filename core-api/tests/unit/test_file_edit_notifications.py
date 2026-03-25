"""Unit tests for grouped file-edit notification emission."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List

import pytest
from unittest.mock import AsyncMock


class FakeResponse:
    def __init__(self, data: Any):
        self.data = data


class FakeAsyncQuery:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]], table_name: str):
        self._state = state
        self._table_name = table_name
        self._op = "select"
        self._payload: Any = None
        self._filters: List[tuple[str, str, Any]] = []
        self._maybe_single = False

    def select(self, _columns: str) -> "FakeAsyncQuery":
        self._op = "select"
        return self

    def update(self, payload: Dict[str, Any]) -> "FakeAsyncQuery":
        self._op = "update"
        self._payload = deepcopy(payload)
        return self

    def insert(self, payload: Dict[str, Any]) -> "FakeAsyncQuery":
        self._op = "insert"
        self._payload = deepcopy(payload)
        return self

    def upsert(self, payload: Dict[str, Any], on_conflict: str | None = None) -> "FakeAsyncQuery":
        self._op = "upsert"
        self._payload = deepcopy(payload)
        return self

    def eq(self, field: str, value: Any) -> "FakeAsyncQuery":
        self._filters.append(("eq", field, value))
        return self

    def maybe_single(self) -> "FakeAsyncQuery":
        self._maybe_single = True
        return self

    def _rows(self) -> List[Dict[str, Any]]:
        return self._state.setdefault(self._table_name, [])

    def _matches(self, row: Dict[str, Any]) -> bool:
        for op, field, value in self._filters:
            if op == "eq" and row.get(field) != value:
                return False
        return True

    async def execute(self) -> FakeResponse:
        rows = self._rows()

        if self._op == "insert":
            payload = deepcopy(self._payload)
            if self._table_name == "notifications":
                for row in rows:
                    if (
                        payload.get("group_key") is not None
                        and row.get("user_id") == payload.get("user_id")
                        and row.get("type") == payload.get("type")
                        and row.get("group_key") == payload.get("group_key")
                        and row.get("archived") is False
                    ):
                        raise Exception("duplicate key value violates unique constraint")
            payload.setdefault("id", f"{self._table_name}-{len(rows) + 1}")
            rows.append(payload)
            return FakeResponse([deepcopy(payload)])

        if self._op == "upsert":
            payload = deepcopy(self._payload)
            unique_keys = ("user_id", "resource_type", "resource_id")
            for row in rows:
                if all(row.get(key) == payload.get(key) for key in unique_keys):
                    row.update(payload)
                    row.setdefault("id", f"{self._table_name}-{rows.index(row) + 1}")
                    return FakeResponse([deepcopy(row)])

            payload.setdefault("id", f"{self._table_name}-{len(rows) + 1}")
            rows.append(payload)
            return FakeResponse([deepcopy(payload)])

        if self._op == "update":
            updated: List[Dict[str, Any]] = []
            for row in rows:
                if self._matches(row):
                    row.update(deepcopy(self._payload))
                    updated.append(deepcopy(row))
            return FakeResponse(updated)

        matched = [deepcopy(row) for row in rows if self._matches(row)]
        if self._maybe_single:
            return FakeResponse(matched[0] if matched else None)
        return FakeResponse(matched)


class FakeAsyncClient:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]]):
        self._state = state

    def table(self, table_name: str) -> FakeAsyncQuery:
        return FakeAsyncQuery(self._state, table_name)


@pytest.fixture
def notification_modules():
    from api.services.notifications import file_edits, grouped, subscriptions

    return file_edits, grouped, subscriptions


def _patch_modules(monkeypatch, state, notification_modules):
    file_edits_module, grouped_module, subscriptions_module = notification_modules
    client = FakeAsyncClient(state)

    monkeypatch.setattr(
        file_edits_module,
        "get_async_service_role_client",
        AsyncMock(return_value=client),
    )
    monkeypatch.setattr(
        file_edits_module,
        "get_actor_info",
        AsyncMock(return_value={"actor_name": "Jay", "actor_avatar": "jay.png"}),
    )
    monkeypatch.setattr(
        grouped_module,
        "get_async_service_role_client",
        AsyncMock(return_value=client),
    )
    monkeypatch.setattr(grouped_module, "should_notify", AsyncMock(return_value=True))
    monkeypatch.setattr(
        subscriptions_module,
        "get_async_service_role_client",
        AsyncMock(return_value=client),
    )

    return file_edits_module


@pytest.mark.asyncio
async def test_emit_document_edited_notification_targets_owner_share_recipients_and_manual_watchers(
    monkeypatch,
    notification_modules,
):
    state = {
        "permissions": [
            {
                "resource_type": "document",
                "resource_id": "doc-1",
                "grantee_type": "user",
                "grantee_id": "user-2",
            }
        ],
        "notification_subscriptions": [
            {
                "id": "sub-manual",
                "user_id": "user-3",
                "resource_type": "document",
                "resource_id": "doc-1",
                "reason": "manual",
            }
        ],
        "notifications": [],
    }
    file_edits_module = _patch_modules(monkeypatch, state, notification_modules)

    result = await file_edits_module.emit_document_edited_notification(
        document_id="doc-1",
        document_title="Quarterly Plan",
        editor_user_id="user-4",
        workspace_id="ws-1",
        owner_id="user-1",
    )

    assert {row["user_id"] for row in result} == {"user-1", "user-2", "user-3"}
    assert {row["user_id"] for row in state["notifications"]} == {"user-1", "user-2", "user-3"}
    assert all(row["type"] == "file_edited" for row in state["notifications"])
    assert all(row["group_key"] == "document:doc-1" for row in state["notifications"])
    assert all(row["title"] == 'Jay made edits to "Quarterly Plan"' for row in state["notifications"])
    assert {row["user_id"] for row in state["notification_subscriptions"]} == {
        "user-1",
        "user-2",
        "user-3",
    }
    reasons_by_user = {
        row["user_id"]: row["reason"]
        for row in state["notification_subscriptions"]
    }
    assert reasons_by_user["user-1"] == "creator"
    assert reasons_by_user["user-2"] == "share_recipient"
    assert reasons_by_user["user-3"] == "manual"


@pytest.mark.asyncio
async def test_emit_document_edited_notification_ignores_stale_share_recipient_subscriptions(
    monkeypatch,
    notification_modules,
):
    state = {
        "permissions": [],
        "notification_subscriptions": [
            {
                "id": "sub-stale",
                "user_id": "user-2",
                "resource_type": "document",
                "resource_id": "doc-1",
                "reason": "share_recipient",
            },
            {
                "id": "sub-manual",
                "user_id": "user-3",
                "resource_type": "document",
                "resource_id": "doc-1",
                "reason": "manual",
            },
        ],
        "notifications": [],
    }
    file_edits_module = _patch_modules(monkeypatch, state, notification_modules)

    result = await file_edits_module.emit_document_edited_notification(
        document_id="doc-1",
        document_title="Quarterly Plan",
        editor_user_id="user-4",
        workspace_id="ws-1",
        owner_id="user-1",
    )

    assert {row["user_id"] for row in result} == {"user-1", "user-3"}
    assert {row["user_id"] for row in state["notifications"]} == {"user-1", "user-3"}
    assert all(row["user_id"] != "user-2" for row in state["notifications"])
