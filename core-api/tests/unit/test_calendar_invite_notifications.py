"""Unit tests for calendar invite notification reconciliation."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List

from api.services.notifications.calendar_invites import reconcile_calendar_invite_notifications


class FakeResponse:
    def __init__(self, data: Any):
        self.data = data


class FakeSyncQuery:
    def __init__(
        self,
        state: Dict[str, List[Dict[str, Any]]],
        table_name: str,
        operations: List[Dict[str, Any]],
    ):
        self._state = state
        self._table_name = table_name
        self._operations = operations
        self._op = "select"
        self._payload: Any = None
        self._filters: List[tuple[str, str, Any]] = []
        self._maybe_single = False

    def select(self, _columns: str) -> "FakeSyncQuery":
        self._op = "select"
        return self

    def update(self, payload: Dict[str, Any]) -> "FakeSyncQuery":
        self._op = "update"
        self._payload = payload
        return self

    def insert(self, payload: Dict[str, Any]) -> "FakeSyncQuery":
        self._op = "insert"
        self._payload = payload
        return self

    def eq(self, field: str, value: Any) -> "FakeSyncQuery":
        self._filters.append(("eq", field, value))
        return self

    def in_(self, field: str, values: List[Any]) -> "FakeSyncQuery":
        self._filters.append(("in", field, values))
        return self

    def is_(self, field: str, value: Any) -> "FakeSyncQuery":
        self._filters.append(("is", field, value))
        return self

    def maybe_single(self) -> "FakeSyncQuery":
        self._maybe_single = True
        return self

    def _rows(self) -> List[Dict[str, Any]]:
        return self._state.setdefault(self._table_name, [])

    def _matches(self, row: Dict[str, Any]) -> bool:
        for op, field, value in self._filters:
            row_value = row.get(field)
            if op == "eq" and row_value != value:
                return False
            if op == "in" and row_value not in value:
                return False
            if op == "is":
                if value == "null" and row_value is not None:
                    return False
                if value != "null" and row_value is not value:
                    return False
        return True

    def execute(self) -> FakeResponse:
        rows = self._rows()
        self._operations.append(
            {
                "table": self._table_name,
                "op": self._op,
                "filters": deepcopy(self._filters),
                "payload": deepcopy(self._payload),
            }
        )

        if self._op == "insert":
            payload = deepcopy(self._payload)
            for row in rows:
                if (
                    payload.get("group_key") is not None
                    and row.get("user_id") == payload.get("user_id")
                    and row.get("type") == payload.get("type")
                    and row.get("group_key") == payload.get("group_key")
                    and row.get("archived") is False
                ):
                    raise Exception("duplicate key value violates unique constraint")
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


class FakeSyncClient:
    def __init__(self, state: Dict[str, List[Dict[str, Any]]]):
        self._state = state
        self.operations: List[Dict[str, Any]] = []

    def table(self, table_name: str) -> FakeSyncQuery:
        return FakeSyncQuery(self._state, table_name, self.operations)


def _calendar_event_row(
    *,
    event_id: str = "event-1",
    external_id: str = "google-1",
    response_status: str = "needsAction",
    title: str = "Roadmap Review",
) -> Dict[str, Any]:
    return {
        "id": event_id,
        "external_id": external_id,
        "title": title,
        "start_time": "2026-03-20T10:00:00+00:00",
        "status": "confirmed",
        "is_organizer": False,
        "organizer_email": "alex@example.com",
        "raw_item": {
            "organizer": {
                "email": "alex@example.com",
                "displayName": "Alex Founder",
            },
            "attendees": [
                {
                    "email": "user@example.com",
                    "self": True,
                    "responseStatus": response_status,
                }
            ],
        },
    }


def test_reconcile_inserts_notification_for_new_actionable_invite():
    state = {"notifications": [], "notification_preferences": []}
    client = FakeSyncClient(state)
    current = {"google-1": _calendar_event_row()}

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id={},
        current_rows_by_external_id=current,
    )

    assert result == {"upserted": 1, "resolved": 0}
    assert len(state["notifications"]) == 1
    notification = state["notifications"][0]
    assert notification["type"] == "calendar_invite"
    assert notification["group_key"] == "calendar:event-1"
    assert notification["resource_id"] == "event-1"
    assert notification["data"]["event_title"] == "Roadmap Review"
    assert notification["data"]["account_email"] == "user@example.com"


def test_reconcile_backfills_missing_notification_for_unchanged_existing_invite():
    state = {"notifications": [], "notification_preferences": []}
    client = FakeSyncClient(state)
    existing_row = _calendar_event_row()

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id={"google-1": existing_row},
        current_rows_by_external_id={"google-1": deepcopy(existing_row)},
    )

    assert result == {"upserted": 1, "resolved": 0}
    assert len(state["notifications"]) == 1


def test_reconcile_skips_unchanged_invite_when_active_notification_already_exists():
    state = {
        "notifications": [
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": "calendar:event-1",
                "title": 'Alex Founder invited you to "Roadmap Review"',
                "body": "Awaiting your response",
                "resource_id": "event-1",
                "archived": False,
                "read": False,
                "seen": False,
                "created_at": "2026-03-19T08:00:00+00:00",
                "data": {
                    "event_id": "event-1",
                    "event_title": "Roadmap Review",
                    "starts_at": "2026-03-20T10:00:00+00:00",
                    "organizer_name": "Alex Founder",
                    "account_email": "user@example.com",
                    "response_status": "needs_action",
                },
            }
        ],
        "notification_preferences": [],
    }
    client = FakeSyncClient(state)
    previous_row = _calendar_event_row()

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id={"google-1": previous_row},
        current_rows_by_external_id={"google-1": deepcopy(previous_row)},
    )

    assert result == {"upserted": 0, "resolved": 0}
    assert state["notifications"][0]["created_at"] == "2026-03-19T08:00:00+00:00"


def test_reconcile_preserves_read_state_for_unchanged_active_notification():
    state = {
        "notifications": [
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": "calendar:event-1",
                "title": 'Alex Founder invited you to "Roadmap Review"',
                "body": "Awaiting your response",
                "resource_id": "event-1",
                "archived": False,
                "read": True,
                "seen": True,
                "created_at": "2026-03-19T08:00:00+00:00",
                "data": {
                    "event_id": "event-1",
                    "event_title": "Roadmap Review",
                    "starts_at": "2026-03-20T10:00:00+00:00",
                    "organizer_name": "Alex Founder",
                    "account_email": "user@example.com",
                    "response_status": "needs_action",
                },
            }
        ],
        "notification_preferences": [],
    }
    client = FakeSyncClient(state)
    previous_row = _calendar_event_row()

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id={"google-1": previous_row},
        current_rows_by_external_id={"google-1": deepcopy(previous_row)},
    )

    assert result == {"upserted": 0, "resolved": 0}
    assert state["notifications"][0]["read"] is True
    assert state["notifications"][0]["seen"] is True
    assert state["notifications"][0]["created_at"] == "2026-03-19T08:00:00+00:00"


def test_reconcile_resolves_notification_when_invite_is_no_longer_actionable():
    state = {
        "notifications": [
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": "calendar:event-1",
                "archived": False,
                "read": False,
                "seen": False,
            }
        ],
        "notification_preferences": [],
    }
    client = FakeSyncClient(state)
    previous_row = _calendar_event_row(response_status="needsAction")
    current_row = _calendar_event_row(response_status="accepted")

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id={"google-1": previous_row},
        current_rows_by_external_id={"google-1": current_row},
    )

    assert result == {"upserted": 0, "resolved": 1}
    assert state["notifications"][0]["archived"] is True
    assert state["notifications"][0]["read"] is True
    assert state["notifications"][0]["seen"] is True


def test_reconcile_batches_resolution_for_multiple_non_actionable_invites():
    state = {
        "notifications": [
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": "calendar:event-1",
                "archived": False,
                "read": False,
                "seen": False,
            },
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": "calendar:event-2",
                "archived": False,
                "read": False,
                "seen": False,
            },
        ],
        "notification_preferences": [],
    }
    client = FakeSyncClient(state)
    previous_rows = {
        "google-1": _calendar_event_row(event_id="event-1", external_id="google-1", response_status="needsAction"),
        "google-2": _calendar_event_row(event_id="event-2", external_id="google-2", response_status="needsAction"),
    }
    current_rows = {
        "google-1": _calendar_event_row(event_id="event-1", external_id="google-1", response_status="accepted"),
        "google-2": _calendar_event_row(event_id="event-2", external_id="google-2", response_status="declined"),
    }

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id=previous_rows,
        current_rows_by_external_id=current_rows,
    )

    assert result == {"upserted": 0, "resolved": 2}
    update_ops = [
        op for op in client.operations
        if op["table"] == "notifications" and op["op"] == "update"
    ]
    assert len(update_ops) == 1
    assert ("in", "group_key", ["calendar:event-1", "calendar:event-2"]) in update_ops[0]["filters"]
    assert all(notification["archived"] is True for notification in state["notifications"])


def test_reconcile_counts_resolved_series_not_rows_when_duplicates_exist():
    state = {
        "notifications": [
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": "calendar:event-1",
                "archived": False,
                "read": False,
                "seen": False,
            },
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": "calendar:event-1",
                "archived": False,
                "read": False,
                "seen": False,
            },
        ],
        "notification_preferences": [],
    }
    client = FakeSyncClient(state)
    previous_row = _calendar_event_row(response_status="needsAction")
    current_row = _calendar_event_row(response_status="accepted")

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id={"google-1": previous_row},
        current_rows_by_external_id={"google-1": current_row},
    )

    assert result == {"upserted": 0, "resolved": 1}
    assert all(notification["archived"] is True for notification in state["notifications"])


def test_reconcile_skips_recurring_invite_when_representative_is_unchanged():
    recurring_series_id = "series-1"
    representative_start = "2999-04-03T10:00:00+00:00"
    current = {
        "google-1": {
            **_calendar_event_row(event_id="event-1", external_id="google-1"),
            "start_time": "2000-03-01T10:00:00+00:00",
            "recurring_event_id": recurring_series_id,
        },
        "google-2": {
            **_calendar_event_row(event_id="event-2", external_id="google-2"),
            "start_time": representative_start,
            "recurring_event_id": recurring_series_id,
        },
    }
    state = {
        "notifications": [
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": f"calendar:{recurring_series_id}",
                "title": 'Alex Founder invited you to "Roadmap Review"',
                "body": "Awaiting your response",
                "resource_id": "event-2",
                "archived": False,
                "read": True,
                "seen": True,
                "created_at": "2026-03-19T08:00:00+00:00",
                "data": {
                    "event_id": "event-2",
                    "event_title": "Roadmap Review",
                    "starts_at": representative_start,
                    "organizer_name": "Alex Founder",
                    "account_email": "user@example.com",
                    "response_status": "needs_action",
                },
            }
        ],
        "notification_preferences": [],
    }
    client = FakeSyncClient(state)

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id=deepcopy(current),
        current_rows_by_external_id=current,
    )

    assert result == {"upserted": 0, "resolved": 0}
    notification = state["notifications"][0]
    assert notification["resource_id"] == "event-2"
    assert notification["data"]["event_id"] == "event-2"
    assert notification["data"]["starts_at"] == representative_start
    assert notification["read"] is True
    assert notification["seen"] is True
    assert notification["created_at"] == "2026-03-19T08:00:00+00:00"


def test_reconcile_updates_recurring_invite_when_representative_changes():
    recurring_series_id = "series-1"
    stale_start = "2000-03-01T10:00:00+00:00"
    representative_start = "2999-04-03T10:00:00+00:00"
    current = {
        "google-1": {
            **_calendar_event_row(event_id="event-1", external_id="google-1"),
            "start_time": stale_start,
            "recurring_event_id": recurring_series_id,
        },
        "google-2": {
            **_calendar_event_row(event_id="event-2", external_id="google-2"),
            "start_time": representative_start,
            "recurring_event_id": recurring_series_id,
        },
    }
    state = {
        "notifications": [
            {
                "user_id": "user-1",
                "type": "calendar_invite",
                "group_key": f"calendar:{recurring_series_id}",
                "title": 'Alex Founder invited you to "Roadmap Review"',
                "body": "Awaiting your response",
                "resource_id": "event-1",
                "archived": False,
                "read": True,
                "seen": True,
                "created_at": "2026-03-19T08:00:00+00:00",
                "data": {
                    "event_id": "event-1",
                    "event_title": "Roadmap Review",
                    "starts_at": stale_start,
                    "organizer_name": "Alex Founder",
                    "account_email": "user@example.com",
                    "response_status": "needs_action",
                },
            }
        ],
        "notification_preferences": [],
    }
    client = FakeSyncClient(state)

    result = reconcile_calendar_invite_notifications(
        client=client,
        user_id="user-1",
        account_email="user@example.com",
        previous_rows_by_external_id=deepcopy(current),
        current_rows_by_external_id=current,
    )

    assert result == {"upserted": 1, "resolved": 0}
    notification = state["notifications"][0]
    assert notification["resource_id"] == "event-2"
    assert notification["data"]["event_id"] == "event-2"
    assert notification["data"]["starts_at"] == representative_start
    assert notification["read"] is True
    assert notification["seen"] is True
    assert notification["created_at"] == "2026-03-19T08:00:00+00:00"
