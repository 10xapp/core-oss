import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Prevent module import failures from lib.supabase_client singleton init.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def _make_query_builder(data):
    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.in_.return_value = query
    query.gt.return_value = query
    query.execute.return_value = SimpleNamespace(data=data)
    return query


def test_classify_watch_health_candidate_detects_stale_notifications():
    from api.routers import cron

    now = datetime.now(timezone.utc)
    watch = {
        "provider": "gmail",
        "created_at": (now - timedelta(days=3)).isoformat(),
        "expiration": (now + timedelta(days=4)).isoformat(),
        "notification_count": 4,
        "last_notification_at": (now - timedelta(hours=30)).isoformat(),
    }
    sync_state = {
        "dirty": False,
        "retry_count": 0,
        "lease_expires_at": None,
        "last_sync_finished_at": (now - timedelta(hours=30)).isoformat(),
    }

    reason = cron._classify_watch_health_candidate(
        watch,
        sync_state,
        now=now,
        stale_after=timedelta(hours=24),
        initial_grace_after=timedelta(hours=12),
    )

    assert reason == "stale_notifications"


def test_classify_watch_health_candidate_skips_recently_synced_quiet_watch():
    from api.routers import cron

    now = datetime.now(timezone.utc)
    watch = {
        "provider": "gmail",
        "created_at": (now - timedelta(days=2)).isoformat(),
        "expiration": (now + timedelta(days=4)).isoformat(),
        "notification_count": 0,
        "last_notification_at": None,
    }
    sync_state = {
        "dirty": False,
        "retry_count": 0,
        "lease_expires_at": None,
        "last_sync_finished_at": (now - timedelta(hours=2)).isoformat(),
    }

    reason = cron._classify_watch_health_candidate(
        watch,
        sync_state,
        now=now,
        stale_after=timedelta(hours=24),
        initial_grace_after=timedelta(hours=12),
    )

    assert reason is None


@pytest.mark.asyncio
async def test_cron_watch_health_marks_targeted_recovery(monkeypatch):
    from api.routers import cron

    now = datetime.now(timezone.utc)
    watches_query = _make_query_builder(
        [
            {
                "id": "watch-1",
                "ext_connection_id": "conn-1",
                "provider": "gmail",
                "created_at": (now - timedelta(days=3)).isoformat(),
                "expiration": (now + timedelta(days=5)).isoformat(),
                "notification_count": 2,
                "last_notification_at": (now - timedelta(hours=36)).isoformat(),
            }
        ]
    )
    state_query = _make_query_builder(
        [
            {
                "connection_id": "conn-1",
                "provider": "google",
                "sync_kind": "email",
                "dirty": False,
                "retry_count": 0,
                "next_retry_at": None,
                "lease_expires_at": None,
                "last_sync_finished_at": (now - timedelta(hours=36)).isoformat(),
            }
        ]
    )

    supabase = MagicMock()
    supabase.table.side_effect = lambda name: {
        "push_subscriptions": watches_query,
        "connection_sync_state": state_query,
    }[name]

    marks = []
    monkeypatch.setattr(cron, "verify_cron_auth", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(cron, "get_service_role_client", lambda: supabase)
    monkeypatch.setattr(cron, "capture_checkin", lambda **_kwargs: "check-in")
    monkeypatch.setattr(
        cron,
        "mark_stream_dirty",
        lambda *args, **kwargs: marks.append((args, kwargs)) or True,
    )

    result = await cron.cron_watch_health(authorization="Bearer test")

    assert result["status"] == "completed"
    assert result["checked"] == 1
    assert result["queued"] == 1
    assert result["errors"] == 0
    assert len(marks) == 1
    assert marks[0][0][1] == "conn-1"
    assert marks[0][0][2] == "google"
    assert marks[0][0][3] == "email"
    assert marks[0][1]["priority"] == cron.get_watch_health_priority()
    assert marks[0][1]["metadata"]["source"] == "watch-health-recovery"
    assert marks[0][1]["metadata"]["reason"] == "stale_notifications"
