import os
from types import SimpleNamespace

# Prevent import-time settings failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def test_mark_and_enqueue_stream_marks_dirty_before_enqueuing(monkeypatch):
    from api.services.syncs import sync_dispatcher

    marked = {"called": False}
    enqueued = {"called": False}

    def fake_mark(*_args, **_kwargs):
        marked["called"] = True
        return True

    queue_client = SimpleNamespace(
        enqueue_sync_for_connection=lambda connection_id, job_type, extra=None, dedup_id=None: (
            enqueued.update(
                {
                    "called": True,
                    "connection_id": connection_id,
                    "job_type": job_type,
                    "extra": extra,
                    "dedup_id": dedup_id,
                }
            )
            or True
        )
    )

    monkeypatch.setattr(sync_dispatcher, "mark_connection_sync_dirty", fake_mark)

    result = sync_dispatcher.mark_and_enqueue_stream(
        object(),
        queue_client,
        "conn-1",
        "google",
        sync_dispatcher.SYNC_KIND_EMAIL,
        extra={"history_id": "123"},
    )

    assert result is True
    assert marked["called"] is True
    assert enqueued["called"] is True
    assert enqueued["job_type"] == "sync-gmail"
    assert enqueued["dedup_id"] == "stream-google-email-conn-1"


def test_mark_and_enqueue_stream_stops_when_mark_dirty_returns_false(monkeypatch):
    from api.services.syncs import sync_dispatcher

    queue_client = SimpleNamespace(
        enqueue_sync_for_connection=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("enqueue should not run when mark dirty fails")
        )
    )

    monkeypatch.setattr(sync_dispatcher, "mark_connection_sync_dirty", lambda *_args, **_kwargs: False)

    result = sync_dispatcher.mark_and_enqueue_stream(
        object(),
        queue_client,
        "conn-1",
        "microsoft",
        sync_dispatcher.SYNC_KIND_CALENDAR,
    )

    assert result is False
