"""Service-level tests for workspace invitation business logic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

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
        self._op = "select"
        self._payload: Any = None
        self._filters: List[tuple[str, str, Any]] = []
        self._order_field: Optional[str] = None
        self._order_desc = False
        self._limit: Optional[int] = None
        self._maybe_single = False
        self._single = False

    def _rows(self) -> List[Dict[str, Any]]:
        if self._table_name not in self._state:
            self._state[self._table_name] = []
        return self._state[self._table_name]

    def _coerce_compare_value(self, value: Any) -> Any:
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return value
        return value

    def _compare(self, left: Any, right: Any, op: str) -> bool:
        if left is None:
            return False

        left_value = self._coerce_compare_value(left)
        right_value = self._coerce_compare_value(right)

        try:
            if op == "gt":
                return left_value > right_value
            if op == "gte":
                return left_value >= right_value
            if op == "lt":
                return left_value < right_value
            if op == "lte":
                return left_value <= right_value
        except TypeError:
            left_str = str(left_value)
            right_str = str(right_value)
            if op == "gt":
                return left_str > right_str
            if op == "gte":
                return left_str >= right_str
            if op == "lt":
                return left_str < right_str
            if op == "lte":
                return left_str <= right_str

        return False

    def _matches(self, row: Dict[str, Any]) -> bool:
        for op, field, value in self._filters:
            row_value = row.get(field)
            if op == "eq" and row_value != value:
                return False
            if op == "ilike":
                if row_value is None:
                    return False
                row_text = str(row_value).lower()
                pattern = str(value).lower()
                if "%" in pattern:
                    if pattern.replace("%", "") not in row_text:
                        return False
                elif row_text != pattern:
                    return False
            if op == "gt" and not self._compare(row_value, value, "gt"):
                return False
            if op == "gte" and not self._compare(row_value, value, "gte"):
                return False
            if op == "lt" and not self._compare(row_value, value, "lt"):
                return False
            if op == "lte" and not self._compare(row_value, value, "lte"):
                return False
            if op == "in_" and row_value not in value:
                return False
        return True

    def select(self, _fields: str = "*") -> "FakeSupabaseQuery":
        self._op = "select"
        return self

    def insert(self, payload: Any) -> "FakeSupabaseQuery":
        self._op = "insert"
        self._payload = payload
        return self

    def update(self, payload: Dict[str, Any]) -> "FakeSupabaseQuery":
        self._op = "update"
        self._payload = payload
        return self

    def delete(self) -> "FakeSupabaseQuery":
        self._op = "delete"
        return self

    def eq(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append(("eq", field, value))
        return self

    def ilike(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append(("ilike", field, value))
        return self

    def gt(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append(("gt", field, value))
        return self

    def gte(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append(("gte", field, value))
        return self

    def lt(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append(("lt", field, value))
        return self

    def lte(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append(("lte", field, value))
        return self

    def in_(self, field: str, values: list) -> "FakeSupabaseQuery":
        self._filters.append(("in_", field, values))
        return self

    def order(self, field: str, desc: bool = False) -> "FakeSupabaseQuery":
        self._order_field = field
        self._order_desc = desc
        return self

    def limit(self, value: int) -> "FakeSupabaseQuery":
        self._limit = value
        return self

    def maybe_single(self) -> "FakeSupabaseQuery":
        self._maybe_single = True
        return self

    def single(self) -> "FakeSupabaseQuery":
        self._single = True
        return self

    async def execute(self) -> FakeResponse:
        table_rows = self._rows()

        if self._op == "insert":
            to_insert = self._payload if isinstance(self._payload, list) else [self._payload]
            inserted: List[Dict[str, Any]] = []
            for row in to_insert:
                new_row = dict(row)
                new_row.setdefault("id", str(uuid4()))
                table_rows.append(new_row)
                inserted.append(dict(new_row))
            return FakeResponse(inserted)

        matched_rows = [row for row in table_rows if self._matches(row)]

        if self._op == "update":
            updated: List[Dict[str, Any]] = []
            for row in matched_rows:
                row.update(self._payload or {})
                updated.append(dict(row))
            return FakeResponse(updated)

        if self._op == "delete":
            deleted = [dict(row) for row in matched_rows]
            remaining = [row for row in table_rows if not self._matches(row)]
            self._state[self._table_name] = remaining
            return FakeResponse(deleted)

        selected = [dict(row) for row in matched_rows]

        if self._order_field:
            selected.sort(key=lambda row: row.get(self._order_field) or "", reverse=self._order_desc)

        if self._limit is not None:
            selected = selected[: self._limit]

        if self._maybe_single or self._single:
            return FakeResponse(selected[0] if selected else None)

        return FakeResponse(selected)


class FakeSupabaseClient:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]]):
        self._state = state

    def table(self, table_name: str) -> FakeSupabaseQuery:
        return FakeSupabaseQuery(self._state, table_name)


@pytest.fixture
def invitations_module():
    from api.services.workspaces import invitations

    return invitations


def _future_iso(days: int = 2) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()


def _past_iso(days: int = 2) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _base_state() -> Dict[str, List[Dict[str, Any]]]:
    return {
        "workspaces": [{"id": "ws-1", "name": "Core Team"}],
        "users": [{"id": "admin-1", "email": "admin@example.com", "name": "Admin"}],
        "workspace_members": [],
        "workspace_invitations": [],
        "notifications": [],
    }


@pytest.mark.asyncio
async def test_create_invite_requires_admin_or_owner(monkeypatch, invitations_module):
    monkeypatch.setattr(
        invitations_module,
        "get_user_workspace_role",
        AsyncMock(return_value="member"),
    )

    with pytest.raises(HTTPException) as exc_info:
        await invitations_module.create_or_refresh_workspace_invitation(
            workspace_id="ws-1",
            invited_email="new@example.com",
            role="member",
            inviter_user_id="admin-1",
            inviter_user_jwt="jwt",
        )

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_accept_invite_rejects_email_mismatch(monkeypatch, invitations_module):
    state = _base_state()
    state["users"].append({"id": "user-2", "email": "other@example.com", "name": "Other"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _future_iso(),
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    with pytest.raises(HTTPException) as exc_info:
        await invitations_module.accept_workspace_invitation("inv-1", "user-2")

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_accept_invite_expired_returns_410_and_marks_expired(monkeypatch, invitations_module):
    state = _base_state()
    state["users"].append({"id": "user-1", "email": "invitee@example.com", "name": "Invitee"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _past_iso(),
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    with pytest.raises(HTTPException) as exc_info:
        await invitations_module.accept_workspace_invitation("inv-1", "user-1")

    assert exc_info.value.status_code == 410
    assert state["workspace_invitations"][0]["status"] == "expired"


@pytest.mark.asyncio
async def test_accept_invite_prefers_claimed_email_over_profile_mirror(monkeypatch, invitations_module):
    state = _base_state()
    state["users"].append({"id": "user-1", "email": "stale@example.com", "name": "Invitee"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _future_iso(),
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    result = await invitations_module.accept_workspace_invitation(
        "inv-1",
        "user-1",
        user_email="invitee@example.com",
    )

    assert result["membership_created"] is True
    assert state["workspace_invitations"][0]["status"] == "accepted"


@pytest.mark.asyncio
@pytest.mark.parametrize("has_existing_pending", [False, True], ids=["new", "refresh"])
async def test_create_or_refresh_invite_email_failure_rolls_back(
    has_existing_pending: bool,
    monkeypatch,
    invitations_module,
):
    state = _base_state()
    old_expiry = _future_iso(days=1)

    if has_existing_pending:
        state["workspace_invitations"].append(
            {
                "id": "inv-old",
                "workspace_id": "ws-1",
                "email": "target@example.com",
                "role": "member",
                "status": "pending",
                "token": "old-token",
                "expires_at": old_expiry,
                "invited_by_user_id": "admin-1",
                "last_email_sent_at": "2026-02-01T00:00:00+00:00",
                "last_email_error": None,
            }
        )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )
    monkeypatch.setattr(
        invitations_module,
        "get_user_workspace_role",
        AsyncMock(return_value="admin"),
    )
    monkeypatch.setattr(
        invitations_module,
        "send_workspace_invitation_email",
        AsyncMock(side_effect=ValueError("resend down")),
    )
    monkeypatch.setattr(
        invitations_module,
        "_get_user_by_email",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        invitations_module.secrets,
        "token_urlsafe",
        lambda _n: "new-token",
    )

    with pytest.raises(HTTPException) as exc_info:
        await invitations_module.create_or_refresh_workspace_invitation(
            workspace_id="ws-1",
            invited_email="target@example.com",
            role="admin",
            inviter_user_id="admin-1",
            inviter_user_jwt="jwt",
        )

    assert exc_info.value.status_code == 502

    if not has_existing_pending:
        assert state["workspace_invitations"] == []
    else:
        assert len(state["workspace_invitations"]) == 1
        restored = state["workspace_invitations"][0]
        assert restored["token"] == "old-token"
        assert restored["expires_at"] == old_expiry
        assert restored["role"] == "member"
        assert "resend down" in (restored.get("last_email_error") or "")


@pytest.mark.asyncio
async def test_duplicate_pending_invite_refreshes_single_row(monkeypatch, invitations_module):
    state = _base_state()
    old_expiry = _future_iso(days=1)
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "target@example.com",
            "role": "member",
            "status": "pending",
            "token": "old-token",
            "expires_at": old_expiry,
            "invited_by_user_id": "admin-1",
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )
    monkeypatch.setattr(
        invitations_module,
        "get_user_workspace_role",
        AsyncMock(return_value="admin"),
    )
    monkeypatch.setattr(
        invitations_module,
        "send_workspace_invitation_email",
        AsyncMock(return_value="resend-id"),
    )
    monkeypatch.setattr(
        invitations_module,
        "_get_user_by_email",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        invitations_module.secrets,
        "token_urlsafe",
        lambda _n: "rotated-token",
    )

    result = await invitations_module.create_or_refresh_workspace_invitation(
        workspace_id="ws-1",
        invited_email="target@example.com",
        role="member",
        inviter_user_id="admin-1",
        inviter_user_jwt="jwt",
    )

    pending = [
        row for row in state["workspace_invitations"]
        if row["workspace_id"] == "ws-1"
        and row["email"] == "target@example.com"
        and row["status"] == "pending"
    ]

    assert len(pending) == 1
    assert pending[0]["token"] == "rotated-token"
    assert datetime.fromisoformat(pending[0]["expires_at"]) > datetime.fromisoformat(old_expiry)
    assert pending[0].get("last_email_sent_at") is not None
    assert result["id"] == "inv-1"


@pytest.mark.asyncio
async def test_accept_invite_happy_path_and_idempotent(monkeypatch, invitations_module):
    state = _base_state()
    state["users"].append({"id": "user-1", "email": "invitee@example.com", "name": "Invitee"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _future_iso(days=2),
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    first = await invitations_module.accept_workspace_invitation("inv-1", "user-1")
    second = await invitations_module.accept_workspace_invitation("inv-1", "user-1")

    assert first["membership_created"] is True
    assert first["already_processed"] is False
    assert state["workspace_invitations"][0]["status"] == "accepted"

    memberships = [
        row for row in state["workspace_members"]
        if row["workspace_id"] == "ws-1" and row["user_id"] == "user-1"
    ]
    assert len(memberships) == 1

    assert second["already_processed"] is True
    assert second["membership_created"] is False


@pytest.mark.asyncio
async def test_create_invite_rejects_personal_workspace(monkeypatch, invitations_module):
    state = _base_state()
    state["workspaces"][0]["is_default"] = True

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )
    monkeypatch.setattr(
        invitations_module,
        "get_user_workspace_role",
        AsyncMock(return_value="owner"),
    )
    monkeypatch.setattr(
        invitations_module,
        "_get_user_by_email",
        AsyncMock(return_value=None),
    )

    with pytest.raises(HTTPException) as exc_info:
        await invitations_module.create_or_refresh_workspace_invitation(
            workspace_id="ws-1",
            invited_email="target@example.com",
            role="member",
            inviter_user_id="admin-1",
            inviter_user_jwt="jwt",
        )

    assert exc_info.value.status_code == 400
    assert "personal workspace" in str(exc_info.value.detail).lower()


@pytest.mark.asyncio
async def test_accept_personal_workspace_invite_returns_410_and_revokes(monkeypatch, invitations_module):
    state = _base_state()
    state["workspaces"][0]["is_default"] = True
    state["users"].append({"id": "user-1", "email": "invitee@example.com", "name": "Invitee"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _future_iso(days=2),
        }
    )
    state["notifications"].append(
        {
            "id": "notif-1",
            "user_id": "user-1",
            "resource_type": "workspace_invitation",
            "resource_id": "inv-1",
            "read": False,
            "seen": False,
            "archived": False,
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    with pytest.raises(HTTPException) as exc_info:
        await invitations_module.accept_workspace_invitation("inv-1", "user-1")

    assert exc_info.value.status_code == 410
    assert state["workspace_invitations"][0]["status"] == "revoked"
    assert state["notifications"][0]["archived"] is True


@pytest.mark.asyncio
async def test_post_signup_revokes_personal_workspace_invites(monkeypatch, invitations_module):
    state = _base_state()
    state["workspaces"][0]["is_default"] = True
    state["users"].append({"id": "user-1", "email": "invitee@example.com", "name": "Invitee"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _future_iso(days=2),
        }
    )
    state["notifications"].append(
        {
            "id": "notif-1",
            "user_id": "user-1",
            "resource_type": "workspace_invitation",
            "resource_id": "inv-1",
            "read": False,
            "seen": False,
            "archived": False,
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    result = await invitations_module.resolve_post_signup_pending_invitations("user-1")

    assert result["count"] == 0
    assert result["pending_invitations"] == []
    assert state["workspace_invitations"][0]["status"] == "revoked"
    assert state["notifications"][0]["archived"] is True


@pytest.mark.asyncio
async def test_resolve_post_signup_expires_and_archives_notifications(monkeypatch, invitations_module):
    state = _base_state()
    state["users"].append({"id": "user-1", "email": "invitee@example.com", "name": "Invitee"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _past_iso(days=1),
        }
    )
    state["notifications"].append(
        {
            "id": "notif-1",
            "user_id": "user-1",
            "resource_type": "workspace_invitation",
            "resource_id": "inv-1",
            "read": False,
            "seen": False,
            "archived": False,
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    result = await invitations_module.resolve_post_signup_pending_invitations("user-1")

    assert result["count"] == 0
    assert state["workspace_invitations"][0]["status"] == "expired"
    assert state["notifications"][0]["archived"] is True
    assert state["notifications"][0]["read"] is True
    assert state["notifications"][0]["seen"] is True


@pytest.mark.asyncio
async def test_resolve_post_signup_preserves_read_state_for_matching_active_notification(
    monkeypatch,
    invitations_module,
):
    """Bootstrap should not resurrect an unchanged invite notification as unread."""
    state = _base_state()
    state["users"].append({"id": "user-1", "email": "invitee@example.com", "name": "Invitee"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _future_iso(days=1),
            "invited_by_user_id": "admin-1",
        }
    )
    state["notifications"].append(
        {
            "id": "notif-existing",
            "user_id": "user-1",
            "workspace_id": "ws-1",
            "type": "workspace_invite",
            "title": "Admin invited you to join Core Team",
            "body": "Role: member",
            "resource_type": "workspace_invitation",
            "resource_id": "inv-1",
            "actor_id": "admin-1",
            "data": {
                "invitation_id": "inv-1",
                "workspace_id": "ws-1",
                "workspace_name": "Core Team",
                "inviter_name": "Admin",
                "role": "member",
                "status": "pending",
            },
            "read": True,
            "seen": True,
            "archived": False,
            "created_at": "2026-02-18T10:00:00+00:00",
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    result = await invitations_module.resolve_post_signup_pending_invitations("user-1")

    assert result["count"] == 1

    # Should still have exactly 1 notification (updated in place, no new insert)
    invite_notifications = [
        row for row in state["notifications"]
        if row.get("resource_type") == "workspace_invitation"
        and row.get("resource_id") == "inv-1"
        and row.get("user_id") == "user-1"
    ]
    assert len(invite_notifications) == 1

    notif = invite_notifications[0]
    assert notif["id"] == "notif-existing"
    assert notif["read"] is True
    assert notif["seen"] is True
    assert notif["archived"] is False
    assert notif["title"] == "Admin invited you to join Core Team"
    assert notif["body"] == "Role: member"


@pytest.mark.asyncio
async def test_resolve_post_signup_refreshes_changed_active_notification(monkeypatch, invitations_module):
    """Changed invite details should still reopen the existing active row."""
    state = _base_state()
    state["users"].append({"id": "user-1", "email": "invitee@example.com", "name": "Invitee"})
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _future_iso(days=1),
            "invited_by_user_id": "admin-1",
        }
    )
    state["notifications"].append(
        {
            "id": "notif-existing",
            "user_id": "user-1",
            "workspace_id": "ws-1",
            "type": "workspace_invite",
            "title": "Stale invite title",
            "body": "Stale body",
            "resource_type": "workspace_invitation",
            "resource_id": "inv-1",
            "actor_id": "admin-1",
            "data": {},
            "read": True,
            "seen": True,
            "archived": False,
            "created_at": "2026-02-18T10:00:00+00:00",
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )

    result = await invitations_module.resolve_post_signup_pending_invitations("user-1")

    assert result["count"] == 1

    invite_notifications = [
        row for row in state["notifications"]
        if row.get("resource_type") == "workspace_invitation"
        and row.get("resource_id") == "inv-1"
        and row.get("user_id") == "user-1"
    ]
    assert len(invite_notifications) == 1

    notif = invite_notifications[0]
    assert notif["id"] == "notif-existing"
    assert notif["read"] is False
    assert notif["seen"] is False
    assert notif["archived"] is False
    assert notif["title"] == "Admin invited you to join Core Team"
    assert notif["body"] == "Role: member"


@pytest.mark.asyncio
async def test_get_invitation_share_link_requires_admin_and_returns_url(monkeypatch, invitations_module):
    state = _base_state()
    state["workspace_invitations"].append(
        {
            "id": "inv-1",
            "workspace_id": "ws-1",
            "email": "invitee@example.com",
            "role": "member",
            "status": "pending",
            "token": "token-1",
            "expires_at": _future_iso(days=2),
        }
    )

    fake_client = FakeSupabaseClient(state)
    monkeypatch.setattr(
        invitations_module,
        "get_async_service_role_client",
        AsyncMock(return_value=fake_client),
    )
    monkeypatch.setattr(
        invitations_module,
        "get_user_workspace_role",
        AsyncMock(return_value="admin"),
    )
    monkeypatch.setattr(invitations_module.settings, "frontend_url", "https://app.example.com")

    result = await invitations_module.get_workspace_invitation_share_link(
        invitation_id="inv-1",
        requester_user_id="admin-1",
        requester_user_jwt="jwt",
    )

    assert result["invitation_id"] == "inv-1"
    assert result["invite_url"] == "https://app.example.com/invite/token-1"
