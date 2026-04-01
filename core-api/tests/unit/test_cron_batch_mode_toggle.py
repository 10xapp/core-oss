import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Prevent import-time settings failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def test_cron_batch_mode_defaults_to_true(monkeypatch):
    from api.routers import cron

    monkeypatch.delenv("CRON_BATCH_MODE", raising=False)
    assert cron.is_cron_batch_mode_enabled() is True


def test_cron_batch_mode_false_values(monkeypatch):
    from api.routers import cron

    for value in ["0", "false", "False", "no", "off"]:
        monkeypatch.setenv("CRON_BATCH_MODE", value)
        assert cron.is_cron_batch_mode_enabled() is False


def test_cron_batch_mode_true_values(monkeypatch):
    from api.routers import cron

    for value in ["1", "true", "yes", "on"]:
        monkeypatch.setenv("CRON_BATCH_MODE", value)
        assert cron.is_cron_batch_mode_enabled() is True


def test_cron_batch_size_defaults_to_100(monkeypatch):
    from api.routers import cron

    monkeypatch.delenv("CRON_BATCH_SIZE", raising=False)
    assert cron.get_cron_batch_size() == 100


@pytest.mark.parametrize("value", ["1", "25", "100", "250"])
def test_cron_batch_size_accepts_valid_values(monkeypatch, value):
    from api.routers import cron

    monkeypatch.setenv("CRON_BATCH_SIZE", value)
    assert cron.get_cron_batch_size() == int(value)


@pytest.mark.parametrize("value", ["", "0", "-1", "251", "nope"])
def test_cron_batch_size_rejects_invalid_values(monkeypatch, value):
    from api.routers import cron

    monkeypatch.setenv("CRON_BATCH_SIZE", value)
    assert cron.get_cron_batch_size() == 100


@pytest.mark.asyncio
async def test_incremental_sync_batch_mode_chunks_and_attaches_batch_tokens(monkeypatch):
    from api.routers import cron
    import lib.queue as queue_module

    query = MagicMock()
    query.select.return_value = query
    query.in_.return_value = query
    query.eq.return_value = query
    query.execute.return_value = SimpleNamespace(data=[
        {"id": "google-3", "user_id": "u1", "provider": "google", "last_synced": None},
        {"id": "google-1", "user_id": "u1", "provider": "google", "last_synced": None},
        {"id": "google-2", "user_id": "u1", "provider": "google", "last_synced": None},
        {"id": "ms-3", "user_id": "u2", "provider": "microsoft", "last_synced": None},
        {"id": "ms-1", "user_id": "u2", "provider": "microsoft", "last_synced": None},
        {"id": "ms-2", "user_id": "u2", "provider": "microsoft", "last_synced": None},
    ])
    supabase = MagicMock()
    supabase.table.return_value = query

    enqueue_batch_mock = MagicMock(return_value=True)
    queue_client_mock = SimpleNamespace(
        available=True,
        enqueue_batch=enqueue_batch_mock,
    )

    monkeypatch.setattr(cron, "verify_cron_auth", lambda *_: True)
    monkeypatch.setattr(cron, "get_service_role_client", lambda: supabase)
    monkeypatch.setattr(cron, "capture_checkin", lambda *args, **kwargs: "checkin-id")
    monkeypatch.setattr(cron, "is_cron_batch_mode_enabled", lambda: True)
    monkeypatch.setattr(cron, "_get_batch_bucket", lambda: "202604011200")
    monkeypatch.setenv("CRON_BATCH_SIZE", "2")
    monkeypatch.setattr(queue_module, "queue_client", queue_client_mock)

    result = await cron.cron_incremental_sync(authorization="Bearer test")

    expected_chunks = [
        ("sync-gmail", ["google-1", "google-2"]),
        ("sync-gmail", ["google-3"]),
        ("sync-calendar", ["google-1", "google-2"]),
        ("sync-calendar", ["google-3"]),
        ("sync-outlook", ["ms-1", "ms-2"]),
        ("sync-outlook", ["ms-3"]),
        ("sync-outlook-calendar", ["ms-1", "ms-2"]),
        ("sync-outlook-calendar", ["ms-3"]),
    ]

    assert result["jobs_enqueued"] == len(expected_chunks)
    assert result["jobs_failed"] == 0
    assert result["batch_mode"] is True
    assert enqueue_batch_mock.call_count == len(expected_chunks)

    for call, (job_type, chunk_ids) in zip(enqueue_batch_mock.call_args_list, expected_chunks):
        batch_token = cron._build_batch_token(chunk_ids, "202604011200")
        assert call.args[0] == job_type
        assert call.args[1] == chunk_ids
        assert call.kwargs["extra"] == {"batch_token": batch_token}
        assert call.kwargs["dedup_id"] == f"batch-{job_type}-{batch_token}"


@pytest.mark.asyncio
async def test_incremental_sync_batch_dedup_ids_stable_within_same_bucket(monkeypatch):
    from api.routers import cron
    import lib.queue as queue_module

    def build_supabase():
        query = MagicMock()
        query.select.return_value = query
        query.in_.return_value = query
        query.eq.return_value = query
        query.execute.return_value = SimpleNamespace(data=[
            {"id": "google-2", "user_id": "u1", "provider": "google", "last_synced": None},
            {"id": "google-1", "user_id": "u1", "provider": "google", "last_synced": None},
            {"id": "ms-2", "user_id": "u2", "provider": "microsoft", "last_synced": None},
            {"id": "ms-1", "user_id": "u2", "provider": "microsoft", "last_synced": None},
        ])
        supabase = MagicMock()
        supabase.table.return_value = query
        return supabase

    async def run_for_bucket(bucket: str):
        enqueue_batch_mock = MagicMock(return_value=True)
        queue_client_mock = SimpleNamespace(
            available=True,
            enqueue_batch=enqueue_batch_mock,
        )
        monkeypatch.setattr(cron, "get_service_role_client", build_supabase)
        monkeypatch.setattr(cron, "_get_batch_bucket", lambda: bucket)
        monkeypatch.setattr(queue_module, "queue_client", queue_client_mock)
        await cron.cron_incremental_sync(authorization="Bearer test")
        return [call.kwargs["dedup_id"] for call in enqueue_batch_mock.call_args_list]

    monkeypatch.setattr(cron, "verify_cron_auth", lambda *_: True)
    monkeypatch.setattr(cron, "capture_checkin", lambda *args, **kwargs: "checkin-id")
    monkeypatch.setattr(cron, "is_cron_batch_mode_enabled", lambda: True)
    monkeypatch.setenv("CRON_BATCH_SIZE", "2")

    first = await run_for_bucket("202604011200")
    second = await run_for_bucket("202604011200")

    assert first == second


@pytest.mark.asyncio
async def test_incremental_sync_batch_dedup_ids_refresh_when_bucket_changes(monkeypatch):
    from api.routers import cron
    import lib.queue as queue_module

    query = MagicMock()
    query.select.return_value = query
    query.in_.return_value = query
    query.eq.return_value = query
    query.execute.return_value = SimpleNamespace(data=[
        {"id": "google-2", "user_id": "u1", "provider": "google", "last_synced": None},
        {"id": "google-1", "user_id": "u1", "provider": "google", "last_synced": None},
    ])
    supabase = MagicMock()
    supabase.table.return_value = query

    monkeypatch.setattr(cron, "verify_cron_auth", lambda *_: True)
    monkeypatch.setattr(cron, "get_service_role_client", lambda: supabase)
    monkeypatch.setattr(cron, "capture_checkin", lambda *args, **kwargs: "checkin-id")
    monkeypatch.setattr(cron, "is_cron_batch_mode_enabled", lambda: True)
    monkeypatch.setenv("CRON_BATCH_SIZE", "2")

    first_enqueue = MagicMock(return_value=True)
    monkeypatch.setattr(queue_module, "queue_client", SimpleNamespace(available=True, enqueue_batch=first_enqueue))
    monkeypatch.setattr(cron, "_get_batch_bucket", lambda: "202604011200")
    await cron.cron_incremental_sync(authorization="Bearer test")

    second_enqueue = MagicMock(return_value=True)
    monkeypatch.setattr(queue_module, "queue_client", SimpleNamespace(available=True, enqueue_batch=second_enqueue))
    monkeypatch.setattr(cron, "_get_batch_bucket", lambda: "202604011201")
    await cron.cron_incremental_sync(authorization="Bearer test")

    first_dedup_ids = [call.kwargs["dedup_id"] for call in first_enqueue.call_args_list]
    second_dedup_ids = [call.kwargs["dedup_id"] for call in second_enqueue.call_args_list]

    assert first_dedup_ids != second_dedup_ids
