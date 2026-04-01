import os

# Prevent import-time settings failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def test_enqueue_sync_for_connection_uses_default_dedup(monkeypatch):
    from lib.queue import QueueClient

    client = QueueClient()
    captured = {}

    def fake_enqueue(job_type, payload, dedup_id=None):
        captured["job_type"] = job_type
        captured["payload"] = payload
        captured["dedup_id"] = dedup_id
        return True

    monkeypatch.setattr(client, "enqueue", fake_enqueue)

    assert client.enqueue_sync_for_connection("conn-1", "sync-gmail") is True
    assert captured["job_type"] == "sync-gmail"
    assert captured["payload"] == {"connection_id": "conn-1"}
    assert captured["dedup_id"] == "sync-gmail-conn-1"


def test_enqueue_sync_for_connection_accepts_custom_dedup(monkeypatch):
    from lib.queue import QueueClient

    client = QueueClient()
    captured = {}

    def fake_enqueue(job_type, payload, dedup_id=None):
        captured["dedup_id"] = dedup_id
        captured["payload"] = payload
        return True

    monkeypatch.setattr(client, "enqueue", fake_enqueue)

    assert client.enqueue_sync_for_connection(
        "conn-1",
        "sync-gmail",
        extra={"history_id": "99"},
        dedup_id="wh-sync-gmail-conn-1-99",
    ) is True
    assert captured["dedup_id"] == "wh-sync-gmail-conn-1-99"
    assert captured["payload"] == {"connection_id": "conn-1", "history_id": "99"}


def test_enqueue_batch_noop_for_empty_ids(monkeypatch):
    from lib.queue import QueueClient

    client = QueueClient()

    def fail_if_called(*args, **kwargs):
        raise AssertionError("enqueue should not be called for empty batch")

    monkeypatch.setattr(client, "enqueue", fail_if_called)
    assert client.enqueue_batch("sync-gmail", []) is True


def test_enqueue_batch_passes_connection_ids(monkeypatch):
    from lib.queue import QueueClient

    client = QueueClient()
    captured = {}

    def fake_enqueue(job_type, payload, dedup_id=None):
        captured["job_type"] = job_type
        captured["payload"] = payload
        captured["dedup_id"] = dedup_id
        return True

    monkeypatch.setattr(client, "enqueue", fake_enqueue)

    assert client.enqueue_batch("sync-calendar", ["c1", "c2"]) is True
    assert captured["job_type"] == "sync-calendar"
    assert captured["payload"] == {"connection_ids": ["c1", "c2"]}
    assert captured["dedup_id"] is None


def test_enqueue_batch_preserves_extra_payload(monkeypatch):
    from lib.queue import QueueClient

    client = QueueClient()
    captured = {}

    def fake_enqueue(job_type, payload, dedup_id=None):
        captured["job_type"] = job_type
        captured["payload"] = payload
        captured["dedup_id"] = dedup_id
        return True

    monkeypatch.setattr(client, "enqueue", fake_enqueue)

    assert client.enqueue_batch(
        "sync-gmail",
        ["c1", "c2"],
        extra={"batch_token": "202604011200-abc123"},
        dedup_id="batch-sync-gmail-202604011200-abc123",
    ) is True
    assert captured["job_type"] == "sync-gmail"
    assert captured["payload"] == {
        "connection_ids": ["c1", "c2"],
        "batch_token": "202604011200-abc123",
    }
    assert captured["dedup_id"] == "batch-sync-gmail-202604011200-abc123"
