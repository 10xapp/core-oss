import os
from types import SimpleNamespace

import pytest

# Prevent import-time settings failures from unrelated package imports.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


class _FakeRpc:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return SimpleNamespace(data=self._data)


class _FakeSupabase:
    def __init__(self, rpc_data):
        self.rpc_data = rpc_data
        self.calls = []

    def rpc(self, name, params):
        self.calls.append((name, params))
        return _FakeRpc(self.rpc_data)

    def table(self, name):
        self.calls.append(("table", name))
        return self

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def maybe_single(self):
        return self

    def execute(self):
        return SimpleNamespace(data=self.rpc_data)


def test_mark_connection_sync_dirty_passes_expected_params():
    from api.services.syncs.sync_state_store import mark_connection_sync_dirty

    supabase = _FakeSupabase(True)

    result = mark_connection_sync_dirty(
        supabase,
        "conn-1",
        "email",
        latest_seen_cursor="123",
        priority=5,
        metadata={"source": "webhook"},
    )

    assert result is True
    assert supabase.calls == [
        (
            "mark_connection_sync_dirty",
            {
                "p_connection_id": "conn-1",
                "p_sync_kind": "email",
                "p_latest_seen_cursor": "123",
                "p_priority": 5,
                "p_provider_event_at": None,
                "p_metadata": {"source": "webhook"},
            },
        )
    ]


def test_get_reconcile_interval_seconds_defaults_to_6_hours(monkeypatch):
    from api.services.syncs.sync_state_store import get_reconcile_interval_seconds

    monkeypatch.delenv("RECONCILE_INTERVAL_SECONDS", raising=False)

    assert get_reconcile_interval_seconds() == 21600


def test_get_max_failure_retry_count_defaults_to_5(monkeypatch):
    from api.services.syncs.sync_state_store import get_max_failure_retry_count

    monkeypatch.delenv("MAX_FAILURE_RETRY_COUNT", raising=False)

    assert get_max_failure_retry_count() == 5


@pytest.mark.parametrize("value", ["300", "1800", "21600", "86400"])
def test_get_reconcile_interval_seconds_accepts_valid_values(monkeypatch, value):
    from api.services.syncs.sync_state_store import get_reconcile_interval_seconds

    monkeypatch.setenv("RECONCILE_INTERVAL_SECONDS", value)

    assert get_reconcile_interval_seconds() == int(value)


@pytest.mark.parametrize("value", ["", "0", "-1", "abc", str(8 * 24 * 3600)])
def test_get_reconcile_interval_seconds_clamps_invalid_values(monkeypatch, value):
    from api.services.syncs.sync_state_store import get_reconcile_interval_seconds

    monkeypatch.setenv("RECONCILE_INTERVAL_SECONDS", value)

    expected = 300 if value == "0" or value == "-1" else 21600
    if value == str(8 * 24 * 3600):
        expected = 7 * 24 * 3600

    assert get_reconcile_interval_seconds() == expected


@pytest.mark.parametrize("value, expected", [("", 5), ("0", 1), ("-1", 1), ("17", 17), ("1001", 1000), ("abc", 5)])
def test_get_max_failure_retry_count_clamps_invalid_values(monkeypatch, value, expected):
    from api.services.syncs.sync_state_store import get_max_failure_retry_count

    monkeypatch.setenv("MAX_FAILURE_RETRY_COUNT", value)

    assert get_max_failure_retry_count() == expected


@pytest.mark.parametrize(
    ("retry_count", "expected_seconds"),
    [
        (None, 60),
        (0, 60),
        (1, 300),
        (2, 900),
        (3, 1800),
        (4, 3600),
        (5, 21600),
        (6, 86400),
        (99, 86400),
    ],
)
def test_get_failure_retry_seconds_steps_up_aggressively(retry_count, expected_seconds):
    from api.services.syncs.sync_state_store import get_failure_retry_seconds

    assert get_failure_retry_seconds(retry_count) == expected_seconds


def test_claim_connection_sync_lease_returns_first_row_from_rpc_list():
    from api.services.syncs.sync_state_store import claim_connection_sync_lease

    supabase = _FakeSupabase(
        [
            {
                "connection_id": "conn-1",
                "provider": "google",
                "sync_kind": "email",
                "dirty_generation": 4,
            }
        ]
    )

    result = claim_connection_sync_lease(
        supabase,
        "worker-1",
        lease_seconds=180,
        provider="google",
        sync_kind="email",
    )

    assert result == {
        "connection_id": "conn-1",
        "provider": "google",
        "sync_kind": "email",
        "dirty_generation": 4,
    }
    assert supabase.calls == [
        (
            "claim_connection_sync_lease",
            {
                "p_worker_id": "worker-1",
                "p_lease_seconds": 180,
                "p_provider": "google",
                "p_sync_kind": "email",
                "p_connection_id": None,
                "p_claim_mode": "any",
            },
        )
    ]


def test_claim_connection_sync_lease_passes_explicit_claim_mode():
    from api.services.syncs.sync_state_store import (
        CLAIM_MODE_RECONCILE_ONLY,
        claim_connection_sync_lease,
    )

    supabase = _FakeSupabase([])

    claim_connection_sync_lease(
        supabase,
        "worker-1",
        claim_mode=CLAIM_MODE_RECONCILE_ONLY,
    )

    assert supabase.calls == [
        (
            "claim_connection_sync_lease",
            {
                "p_worker_id": "worker-1",
                "p_lease_seconds": 120,
                "p_provider": None,
                "p_sync_kind": None,
                "p_connection_id": None,
                "p_claim_mode": "reconcile_only",
            },
        )
    ]


def test_complete_connection_sync_lease_returns_none_when_rpc_returns_empty_list():
    from api.services.syncs.sync_state_store import complete_connection_sync_lease

    supabase = _FakeSupabase([])

    result = complete_connection_sync_lease(
        supabase,
        "conn-1",
        "calendar",
        "worker-1",
        last_synced_cursor="sync-token-2",
    )

    assert result is None
    assert supabase.calls == [
        (
            "complete_connection_sync_lease",
            {
                "p_connection_id": "conn-1",
                "p_sync_kind": "calendar",
                "p_worker_id": "worker-1",
                "p_last_synced_cursor": "sync-token-2",
                "p_latest_seen_cursor": None,
                "p_keep_dirty": False,
                "p_reconcile_interval_seconds": 21600,
            },
        )
    ]


def test_get_connection_sync_state_returns_row_dict():
    from api.services.syncs.sync_state_store import get_connection_sync_state

    supabase = _FakeSupabase(
        {
            "connection_id": "conn-1",
            "sync_kind": "calendar",
            "metadata": {"source": "webhook"},
        }
    )

    result = get_connection_sync_state(supabase, "conn-1", "calendar")

    assert result == {
        "connection_id": "conn-1",
        "sync_kind": "calendar",
        "metadata": {"source": "webhook"},
    }


def test_invalid_sync_kind_raises_value_error():
    from api.services.syncs.sync_state_store import mark_connection_sync_dirty

    supabase = _FakeSupabase(True)

    with pytest.raises(ValueError, match="Unsupported sync_kind"):
        mark_connection_sync_dirty(supabase, "conn-1", "contacts")


def test_invalid_claim_mode_raises_value_error():
    from api.services.syncs.sync_state_store import claim_connection_sync_lease

    supabase = _FakeSupabase(True)

    with pytest.raises(ValueError, match="Unsupported claim_mode"):
        claim_connection_sync_lease(supabase, "worker-1", claim_mode="burst_only")


def test_fail_connection_sync_lease_passes_retry_seconds():
    from api.services.syncs.sync_state_store import fail_connection_sync_lease

    supabase = _FakeSupabase({"retry_count": 2, "next_retry_at": "2026-04-01T20:30:00Z"})

    result = fail_connection_sync_lease(
        supabase,
        "conn-1",
        "email",
        "worker-2",
        "boom",
        retry_seconds=90,
    )

    assert result == {"retry_count": 2, "next_retry_at": "2026-04-01T20:30:00Z"}
    assert supabase.calls == [
        (
            "fail_connection_sync_lease",
            {
                "p_connection_id": "conn-1",
                "p_sync_kind": "email",
                "p_worker_id": "worker-2",
                "p_error": "boom",
                "p_retry_seconds": 90,
            },
        )
    ]
