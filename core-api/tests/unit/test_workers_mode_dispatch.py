import os
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

# Prevent import-time settings failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def test_sync_payload_requires_exactly_one_target():
    from api.routers.workers import SyncPayload

    with pytest.raises(ValidationError):
        SyncPayload()

    with pytest.raises(ValidationError):
        SyncPayload(connection_id="conn-1", connection_ids=["conn-1"])

    payload = SyncPayload(connection_id="conn-1")
    assert payload.connection_id == "conn-1"


def test_sync_payload_webhook_mode_validation():
    from api.routers.workers import SyncPayload

    with pytest.raises(ValidationError):
        SyncPayload(connection_id="conn-1", email_address="a@b.c")  # missing history_id

    with pytest.raises(ValidationError):
        SyncPayload(connection_id="conn-1", channel_id="ch-1", history_id="123", email_address="a@b.c")

    with pytest.raises(ValidationError):
        SyncPayload(connection_ids=["conn-1"], channel_id="ch-1")

    with pytest.raises(ValidationError):
        SyncPayload(
            connection_id="conn-1",
            history_id="123",
            email_address="a@b.c",
            initial_sync=True,
        )

    payload = SyncPayload(connection_id="conn-1", history_id="123", email_address="a@b.c")
    assert payload.history_id == "123"
    assert payload.email_address == "a@b.c"

    # Backward compatibility for legacy queued webhook payloads.
    legacy_payload = SyncPayload(connection_id="conn-1", history_id="123")
    assert legacy_payload.history_id == "123"
    assert legacy_payload.email_address is None


def test_sync_payload_accepts_integer_history_id():
    """Google Pub/Sub sends historyId as int — must not 422."""
    from api.routers.workers import SyncPayload

    payload = SyncPayload(
        connection_id="conn-1",
        history_id=667993,
        email_address="user@example.com",
    )
    assert payload.history_id == "667993"


def test_run_batch_reports_counters():
    from api.routers.workers import _run_batch

    def processor(connection_id: str):
        if connection_id == "ok":
            return {"status": "ok"}
        if connection_id == "skip":
            return {"status": "skipped"}
        if connection_id == "error":
            return {"status": "error"}
        raise RuntimeError("boom")

    result = _run_batch(["ok", "skip", "error", "raise"], processor, "sync-gmail")

    assert result["status"] == "partial"
    assert result["processed"] == 1
    assert result["skipped"] == 1
    assert result["errors"] == 2
    assert result["failed_ids"] == ["error", "raise"]
    assert result["budget_exhausted"] is False
    assert result["remaining"] == 0
    assert result["duration_seconds"] >= 0


def test_run_batch_clean_run_returns_ok():
    from api.routers.workers import _run_batch

    result = _run_batch(["a", "b", "c"], lambda _: {"status": "ok"}, "sync-gmail")

    assert result["status"] == "ok"
    assert result["processed"] == 3
    assert result["errors"] == 0
    assert result["failed_ids"] == []


def test_run_batch_treats_quarantined_as_processed():
    from api.routers.workers import _run_batch

    result = _run_batch(
        ["a", "b"],
        lambda cid: {"status": "quarantined"} if cid == "a" else {"status": "ok"},
        "sync-gmail",
    )

    assert result["status"] == "ok"
    assert result["processed"] == 2
    assert result["errors"] == 0


def test_run_batch_respects_time_budget(monkeypatch):
    from api.routers import workers

    monkeypatch.setattr(workers, "BATCH_TIME_BUDGET_SECONDS", 0)
    result = workers._run_batch(["a", "b"], lambda _: {"status": "ok"}, "sync-gmail")

    assert result["processed"] == 0
    assert result["budget_exhausted"] is True
    assert result["remaining"] == 2
    assert result["remaining_ids"] == ["a", "b"]


def test_worker_sync_gmail_routes_webhook_mode():
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_id="conn-1",
        history_id="555",
        email_address="user@example.com",
    )

    with patch.object(workers, "_run_with_stream_lease", return_value={"status": "ok", "message": "done"}) as lease_mock:
        with patch.object(workers, "_process_gmail_webhook") as webhook_mock:
            with patch.object(workers, "_sync_single_gmail") as single_mock:
                result = workers.worker_sync_gmail(payload)

    assert result["status"] == "ok"
    lease_mock.assert_called_once()
    webhook_mock.assert_not_called()
    single_mock.assert_not_called()


def test_worker_sync_gmail_routes_batch_mode():
    from api.routers import workers

    payload = workers.SyncPayload(connection_ids=["conn-1", "conn-2"])

    with patch.object(workers, "_run_batch_endpoint", return_value={"status": "ok", "processed": 2}) as batch_mock:
        result = workers.worker_sync_gmail(payload)

    assert result["status"] == "ok"
    batch_mock.assert_called_once()
    assert batch_mock.call_args.args[0] == "sync-gmail"
    assert batch_mock.call_args.args[1] == payload
    assert callable(batch_mock.call_args.args[2])


def test_run_batch_endpoint_reenqueues_budget_tail(monkeypatch):
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_ids=["conn-1", "conn-2", "conn-3"],
        batch_token="202604011200-abc123",
    )
    queue_client_mock = MagicMock()
    queue_client_mock.enqueue_batch.return_value = True

    monkeypatch.setattr(workers, "queue_client", queue_client_mock)
    monkeypatch.setattr(
        workers,
        "_run_batch",
        lambda *_args, **_kwargs: {
            "status": "ok",
            "processed": 1,
            "skipped": 0,
            "errors": 0,
            "failed_ids": [],
            "budget_exhausted": True,
            "remaining": 2,
            "remaining_ids": ["conn-2", "conn-3"],
            "duration_seconds": 1.0,
        },
    )

    result = workers._run_batch_endpoint("sync-gmail", payload, lambda _: {"status": "ok"})

    assert result["status"] == "ok"
    assert result["continued"] is True
    assert result["re_enqueued"] == 2
    assert result["remaining"] == 2
    assert "remaining_ids" not in result
    queue_client_mock.enqueue_batch.assert_called_once()
    assert queue_client_mock.enqueue_batch.call_args.args == ("sync-gmail", ["conn-2", "conn-3"])
    assert queue_client_mock.enqueue_batch.call_args.kwargs["extra"]["batch_token"] == "202604011200-abc123"
    assert queue_client_mock.enqueue_batch.call_args.kwargs["dedup_id"] == (
        workers._build_continuation_dedup_id("sync-gmail", "202604011200-abc123", ["conn-2", "conn-3"])
    )


def test_run_batch_endpoint_raises_when_continuation_enqueue_fails(monkeypatch):
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_ids=["conn-1", "conn-2"],
        batch_token="202604011200-abc123",
    )
    queue_client_mock = MagicMock()
    queue_client_mock.enqueue_batch.return_value = False

    monkeypatch.setattr(workers, "queue_client", queue_client_mock)
    monkeypatch.setattr(
        workers,
        "_run_batch",
        lambda *_args, **_kwargs: {
            "status": "ok",
            "processed": 1,
            "skipped": 0,
            "errors": 0,
            "failed_ids": [],
            "budget_exhausted": True,
            "remaining": 1,
            "remaining_ids": ["conn-2"],
            "duration_seconds": 1.0,
        },
    )

    with pytest.raises(workers.HTTPException) as exc_info:
        workers._run_batch_endpoint("sync-gmail", payload, lambda _: {"status": "ok"})

    assert exc_info.value.status_code == 500
    queue_client_mock.enqueue_batch.assert_called_once()


def test_run_batch_endpoint_raises_on_zero_progress_budget_exhaustion(monkeypatch):
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_ids=["conn-1", "conn-2"],
        batch_token="202604011200-abc123",
    )
    queue_client_mock = MagicMock()

    monkeypatch.setattr(workers, "queue_client", queue_client_mock)
    monkeypatch.setattr(
        workers,
        "_run_batch",
        lambda *_args, **_kwargs: {
            "status": "ok",
            "processed": 0,
            "skipped": 0,
            "errors": 0,
            "failed_ids": [],
            "budget_exhausted": True,
            "remaining": 2,
            "remaining_ids": ["conn-1", "conn-2"],
            "duration_seconds": 1.0,
        },
    )

    with pytest.raises(workers.HTTPException) as exc_info:
        workers._run_batch_endpoint("sync-gmail", payload, lambda _: {"status": "ok"})

    assert exc_info.value.status_code == 500
    queue_client_mock.enqueue_batch.assert_not_called()


def test_run_batch_endpoint_keeps_partial_status_for_item_failures(monkeypatch):
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_ids=["conn-1", "conn-2"],
        batch_token="202604011200-abc123",
    )
    queue_client_mock = MagicMock()
    queue_client_mock.enqueue_batch.return_value = True

    monkeypatch.setattr(workers, "queue_client", queue_client_mock)
    monkeypatch.setattr(
        workers,
        "_run_batch",
        lambda *_args, **_kwargs: {
            "status": "partial",
            "processed": 1,
            "skipped": 0,
            "errors": 1,
            "failed_ids": ["conn-1"],
            "budget_exhausted": True,
            "remaining": 1,
            "remaining_ids": ["conn-2"],
            "duration_seconds": 1.0,
        },
    )

    result = workers._run_batch_endpoint("sync-gmail", payload, lambda _: {"status": "ok"})

    assert result["status"] == "partial"
    assert result["continued"] is True
    assert result["re_enqueued"] == 1
    queue_client_mock.enqueue_batch.assert_called_once()


def test_worker_sync_calendar_routes_webhook_mode():
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_id="conn-1",
        channel_id="channel-1",
        resource_state="exists",
        message_number="123",
    )

    with patch.object(workers, "_run_with_stream_lease", return_value={"status": "ok"}) as lease_mock:
        with patch.object(workers, "_process_calendar_webhook") as webhook_mock:
            with patch.object(workers, "_sync_single_calendar") as single_mock:
                result = workers.worker_sync_calendar(payload)

    assert result["status"] == "ok"
    lease_mock.assert_called_once()
    webhook_mock.assert_not_called()
    single_mock.assert_not_called()


def test_run_with_stream_lease_skips_when_no_claim():
    from api.routers import workers

    with patch.object(workers, "get_service_role_client", return_value=MagicMock()):
        with patch.object(workers, "claim_connection_sync_lease", return_value=None) as claim_mock:
            result = workers._run_with_stream_lease("conn-1", workers.SYNC_KIND_EMAIL, lambda: {"status": "ok"})

    assert result["status"] == "skipped"
    assert "already running or not dirty" in result["message"].lower()
    assert claim_mock.call_args.kwargs["claim_mode"] == workers.CLAIM_MODE_DIRTY_ONLY


def test_run_with_stream_lease_completes_success():
    from api.routers import workers

    with patch.object(workers, "get_service_role_client", return_value=MagicMock()):
        with patch.object(workers, "claim_connection_sync_lease", return_value={"connection_id": "conn-1"}):
            with patch.object(workers, "complete_connection_sync_lease") as complete_mock:
                with patch.object(workers, "fail_connection_sync_lease") as fail_mock:
                    result = workers._run_with_stream_lease(
                        "conn-1",
                        workers.SYNC_KIND_EMAIL,
                        lambda: {"status": "ok", "new_delta_link": "delta-1"},
                    )

    assert result["status"] == "ok"
    complete_mock.assert_called_once()
    fail_mock.assert_not_called()


def test_run_with_stream_lease_fails_error_result():
    from api.routers import workers

    with patch.object(workers, "get_service_role_client", return_value=MagicMock()):
        with patch.object(workers, "claim_connection_sync_lease", return_value={"connection_id": "conn-1"}):
            with patch.object(workers, "complete_connection_sync_lease") as complete_mock:
                with patch.object(workers, "fail_connection_sync_lease") as fail_mock:
                    result = workers._run_with_stream_lease(
                        "conn-1",
                        workers.SYNC_KIND_EMAIL,
                        lambda: {"status": "error", "message": "boom"},
                    )

    assert result["status"] == "error"
    fail_mock.assert_called_once()
    complete_mock.assert_not_called()


def test_run_with_stream_lease_quarantines_when_retry_cap_is_hit(monkeypatch):
    from api.routers import workers

    monkeypatch.setenv("MAX_FAILURE_RETRY_COUNT", "3")

    with patch.object(workers, "get_service_role_client", return_value=MagicMock()):
        with patch.object(
            workers,
            "claim_connection_sync_lease",
            return_value={"connection_id": "conn-1", "provider": "microsoft"},
        ):
            with patch.object(
                workers,
                "get_connection_sync_state",
                return_value={"retry_count": 2},
            ):
                with patch("api.services.syncs.failure_policy.complete_connection_sync_lease") as complete_mock:
                    with patch.object(workers, "fail_connection_sync_lease") as fail_mock:
                        with patch(
                            "api.services.syncs.failure_policy.deactivate_connection_with_subscriptions"
                        ) as deactivate_mock:
                            result = workers._run_with_stream_lease(
                                "conn-1",
                                workers.SYNC_KIND_EMAIL,
                                lambda: {"status": "error", "message": "boom"},
                            )

    assert result["status"] == "quarantined"
    complete_mock.assert_called_once()
    fail_mock.assert_not_called()
    deactivate_mock.assert_called_once()


def test_run_with_stream_lease_quarantines_on_permanent_microsoft_auth_error():
    from api.routers import workers

    with patch.object(workers, "get_service_role_client", return_value=MagicMock()):
        with patch.object(
            workers,
            "claim_connection_sync_lease",
            return_value={"connection_id": "conn-1", "provider": "microsoft"},
        ):
            with patch.object(
                workers,
                "get_connection_sync_state",
                return_value={"retry_count": 0},
            ):
                with patch("api.services.syncs.failure_policy.complete_connection_sync_lease") as complete_mock:
                    with patch.object(workers, "fail_connection_sync_lease") as fail_mock:
                        with patch(
                            "api.services.syncs.failure_policy.deactivate_connection_with_subscriptions"
                        ) as deactivate_mock:
                            result = workers._run_with_stream_lease(
                                "conn-1",
                                workers.SYNC_KIND_EMAIL,
                                lambda: {
                                    "status": "error",
                                    "message": "Refresh token is invalid - user must re-authenticate",
                                },
                            )

    assert result["status"] == "quarantined"
    complete_mock.assert_called_once()
    fail_mock.assert_not_called()
    deactivate_mock.assert_called_once()


def test_process_gmail_webhook_error_status_not_overwritten():
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_id="conn-1",
        history_id="555",
        email_address="user@example.com",
    )

    with patch.object(
        workers,
        "_load_connection",
        return_value=(None, {"id": "conn-1", "provider": "google", "provider_email": "user@example.com"}),
    ):
        with patch(
            "api.services.webhooks.process_gmail_notification",
            return_value={"status": "failed", "message": "provider failed"},
        ):
            result = workers._process_gmail_webhook(payload)

    assert result["status"] == "error"
    assert result["message"] == "provider failed"


def test_process_gmail_webhook_resolves_email_from_connection_when_missing():
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_id="conn-1",
        history_id="555",
    )

    with patch.object(
        workers,
        "_load_connection",
        return_value=(None, {"id": "conn-1", "provider": "google", "provider_email": "user@example.com"}),
    ):
        with patch.object(workers, "get_service_role_client", return_value=MagicMock()):
            with patch.object(workers, "_touch_last_synced", return_value=None):
                with patch(
                    "api.services.webhooks.process_gmail_notification",
                    return_value={"status": "ok", "message": "ok"},
                ) as process_mock:
                    result = workers._process_gmail_webhook(payload)

    assert result["status"] == "ok"
    process_mock.assert_called_once_with("user@example.com", "555")


def test_process_calendar_webhook_error_status_not_overwritten():
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_id="conn-1",
        channel_id="channel-1",
    )

    with patch(
        "api.services.webhooks.process_calendar_notification",
        return_value={"status": "failed", "message": "provider failed"},
    ):
        result = workers._process_calendar_webhook(payload)

    assert result["status"] == "error"
    assert result["message"] == "provider failed"


def test_process_calendar_webhook_does_not_touch_last_synced_for_non_sync_outcomes():
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_id="conn-1",
        channel_id="channel-1",
        resource_state="exists",
    )

    with patch.object(workers, "_touch_last_synced") as touch_mock:
        with patch(
            "api.services.webhooks.process_calendar_notification",
            return_value={"status": "ok", "message": "No active subscription"},
        ):
            result = workers._process_calendar_webhook(payload)

    assert result["status"] == "ok"
    touch_mock.assert_not_called()


def test_process_gmail_webhook_rejects_connection_email_mismatch():
    from api.routers import workers

    payload = workers.SyncPayload(
        connection_id="conn-1",
        history_id="555",
        email_address="user@example.com",
    )

    with patch.object(
        workers,
        "_load_connection",
        return_value=(None, {"id": "conn-1", "provider": "google", "provider_email": "other@example.com"}),
    ):
        with patch("api.services.webhooks.process_gmail_notification") as process_mock:
            result = workers._process_gmail_webhook(payload)

    assert result["status"] == "error"
    assert "does not match connection" in result["message"]
    process_mock.assert_not_called()
