import os
from unittest.mock import MagicMock, patch

import pytest

# Prevent import-time settings failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def test_dispatch_claimed_stream_uses_gmail_webhook_path_when_cursor_present():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "google",
        "sync_kind": stream_worker.SYNC_KIND_EMAIL,
        "latest_seen_cursor": "12345",
    }

    with patch.object(stream_worker.worker_router, "_process_gmail_webhook", return_value={"status": "ok"}) as webhook_mock:
        with patch.object(stream_worker.worker_router, "_sync_single_gmail") as sync_mock:
            result = stream_worker._dispatch_claimed_stream(
                claim,
                state={"latest_seen_cursor": "12345", "metadata": {"source": "gmail-webhook"}},
            )

    assert result["status"] == "ok"
    webhook_mock.assert_called_once()
    sync_mock.assert_not_called()


def test_dispatch_claimed_stream_uses_generic_gmail_path_for_non_webhook_source():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "google",
        "sync_kind": stream_worker.SYNC_KIND_EMAIL,
        "latest_seen_cursor": "12345",
    }

    with patch.object(stream_worker.worker_router, "_process_gmail_webhook", return_value={"status": "ok"}) as webhook_mock:
        with patch.object(stream_worker.worker_router, "_sync_single_gmail", return_value={"status": "ok"}) as sync_mock:
            result = stream_worker._dispatch_claimed_stream(
                claim,
                state={"latest_seen_cursor": "12345", "metadata": {"source": "incremental-cron"}},
            )

    assert result["status"] == "ok"
    webhook_mock.assert_not_called()
    sync_mock.assert_called_once()


def test_dispatch_claimed_stream_uses_calendar_webhook_path_when_metadata_requests_it():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "google",
        "sync_kind": stream_worker.SYNC_KIND_CALENDAR,
        "latest_seen_cursor": None,
    }
    state = {
        "metadata": {
            "source": "google-calendar-webhook",
            "channel_id": "channel-1",
            "resource_state": "exists",
        }
    }

    with patch.object(stream_worker, "_resolve_active_google_calendar_channel", return_value="channel-1"):
        with patch.object(stream_worker.worker_router, "_process_calendar_webhook", return_value={"status": "ok"}) as webhook_mock:
            with patch.object(stream_worker.worker_router, "_sync_single_calendar") as sync_mock:
                result = stream_worker._dispatch_claimed_stream(
                    claim,
                    state=state,
                    service_supabase=MagicMock(),
                )

    assert result["status"] == "ok"
    webhook_mock.assert_called_once()
    sync_mock.assert_not_called()


def test_dispatch_claimed_stream_falls_back_when_calendar_channel_is_stale():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "google",
        "sync_kind": stream_worker.SYNC_KIND_CALENDAR,
        "latest_seen_cursor": None,
    }
    state = {
        "metadata": {
            "source": "google-calendar-webhook",
            "channel_id": "stale-channel",
            "resource_state": "exists",
        }
    }

    with patch.object(stream_worker, "_resolve_active_google_calendar_channel", return_value="active-channel"):
        with patch.object(stream_worker.worker_router, "_process_calendar_webhook", return_value={"status": "ok"}) as webhook_mock:
            with patch.object(stream_worker.worker_router, "_sync_single_calendar", return_value={"status": "ok"}) as sync_mock:
                result = stream_worker._dispatch_claimed_stream(
                    claim,
                    state=state,
                    service_supabase=MagicMock(),
                )

    assert result["status"] == "ok"
    webhook_mock.assert_not_called()
    sync_mock.assert_called_once()


def test_dispatch_claimed_stream_passes_initial_sync_metadata_to_provider_worker():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "microsoft",
        "sync_kind": stream_worker.SYNC_KIND_EMAIL,
        "latest_seen_cursor": None,
    }
    state = {
        "metadata": {
            "source": "initial-sync",
            "initial_sync": True,
            "max_results": 50,
            "days_back": 20,
        }
    }

    def _assert_payload(connection_id, payload):
        assert connection_id == "conn-1"
        assert payload.initial_sync is True
        assert payload.max_results == 50
        assert payload.days_back == 20
        return {"status": "ok"}

    with patch.object(stream_worker.worker_router, "_sync_single_outlook", side_effect=_assert_payload) as sync_mock:
        result = stream_worker._dispatch_claimed_stream(claim, state=state)

    assert result["status"] == "ok"
    sync_mock.assert_called_once()


def test_process_one_claim_completes_on_success():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "microsoft",
        "sync_kind": stream_worker.SYNC_KIND_EMAIL,
        "latest_seen_cursor": None,
    }
    service_supabase = MagicMock()

    with patch.object(stream_worker, "get_connection_sync_state", return_value={"metadata": {}}):
        with patch.object(stream_worker, "_dispatch_claimed_stream", return_value={"status": "ok", "new_delta_link": "delta-1"}):
            with patch.object(stream_worker, "complete_connection_sync_lease") as complete_mock:
                with patch.object(stream_worker, "fail_connection_sync_lease") as fail_mock:
                    with patch.object(
                        stream_worker,
                        "start_connection_sync_lease_heartbeat",
                        return_value=(MagicMock(), MagicMock()),
                    ):
                        with patch.object(stream_worker, "stop_connection_sync_lease_heartbeat") as stop_mock:
                            result = stream_worker.process_one_claim(
                                claim,
                                worker_id="worker-1",
                                service_supabase=service_supabase,
                            )

    assert result["status"] == "ok"
    complete_mock.assert_called_once()
    fail_mock.assert_not_called()
    stop_mock.assert_called_once()


def test_process_one_claim_fails_lease_when_state_fetch_raises():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "microsoft",
        "sync_kind": stream_worker.SYNC_KIND_EMAIL,
        "latest_seen_cursor": None,
    }
    service_supabase = MagicMock()

    with patch.object(stream_worker, "get_connection_sync_state", side_effect=RuntimeError("boom")):
        with patch.object(stream_worker, "fail_connection_sync_lease") as fail_mock:
            with patch.object(stream_worker, "start_connection_sync_lease_heartbeat") as start_mock:
                result = stream_worker.process_one_claim(
                    claim,
                    worker_id="worker-1",
                    service_supabase=service_supabase,
                )

    assert result["status"] == "error"
    assert result["message"] == "boom"
    fail_mock.assert_called_once()
    start_mock.assert_not_called()


def test_process_one_claim_quarantines_on_permanent_google_oauth_error():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "google",
        "sync_kind": stream_worker.SYNC_KIND_EMAIL,
        "latest_seen_cursor": None,
    }
    service_supabase = MagicMock()

    with patch.object(
        stream_worker,
        "get_connection_sync_state",
        return_value={"metadata": {}, "retry_count": 0},
    ):
        with patch.object(
            stream_worker,
            "_dispatch_claimed_stream",
            return_value={"status": "error", "message": "Refresh token is invalid"},
        ):
            with patch("api.services.syncs.failure_policy.complete_connection_sync_lease") as complete_mock:
                with patch.object(stream_worker, "fail_connection_sync_lease") as fail_mock:
                    with patch(
                        "api.services.syncs.failure_policy.deactivate_connection_with_subscriptions"
                    ) as deactivate_mock:
                        with patch.object(
                            stream_worker,
                            "start_connection_sync_lease_heartbeat",
                            return_value=(MagicMock(), MagicMock()),
                        ):
                            with patch.object(stream_worker, "stop_connection_sync_lease_heartbeat"):
                                result = stream_worker.process_one_claim(
                                    claim,
                                    worker_id="worker-1",
                                    service_supabase=service_supabase,
                                )

    assert result["status"] == "quarantined"
    complete_mock.assert_called_once()
    fail_mock.assert_not_called()
    deactivate_mock.assert_called_once()


def test_process_one_claim_quarantines_on_permanent_microsoft_auth_error():
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "microsoft",
        "sync_kind": stream_worker.SYNC_KIND_EMAIL,
        "latest_seen_cursor": None,
    }
    service_supabase = MagicMock()

    with patch.object(
        stream_worker,
        "get_connection_sync_state",
        return_value={"metadata": {}, "retry_count": 0},
    ):
        with patch.object(
            stream_worker,
            "_dispatch_claimed_stream",
            return_value={"status": "error", "message": "Refresh token is invalid - user must re-authenticate"},
        ):
            with patch("api.services.syncs.failure_policy.complete_connection_sync_lease") as complete_mock:
                with patch.object(stream_worker, "fail_connection_sync_lease") as fail_mock:
                    with patch(
                        "api.services.syncs.failure_policy.deactivate_connection_with_subscriptions"
                    ) as deactivate_mock:
                        with patch.object(
                            stream_worker,
                            "start_connection_sync_lease_heartbeat",
                            return_value=(MagicMock(), MagicMock()),
                        ):
                            with patch.object(stream_worker, "stop_connection_sync_lease_heartbeat"):
                                result = stream_worker.process_one_claim(
                                    claim,
                                    worker_id="worker-1",
                                    service_supabase=service_supabase,
                                )

    assert result["status"] == "quarantined"
    complete_mock.assert_called_once()
    fail_mock.assert_not_called()
    deactivate_mock.assert_called_once()


def test_process_one_claim_quarantines_when_retry_cap_is_hit(monkeypatch):
    from api.services.syncs import stream_worker

    claim = {
        "connection_id": "conn-1",
        "provider": "microsoft",
        "sync_kind": stream_worker.SYNC_KIND_CALENDAR,
        "latest_seen_cursor": None,
    }
    service_supabase = MagicMock()
    monkeypatch.setenv("MAX_FAILURE_RETRY_COUNT", "3")

    with patch.object(
        stream_worker,
        "get_connection_sync_state",
        return_value={"metadata": {}, "retry_count": 2},
    ):
        with patch.object(
            stream_worker,
            "_dispatch_claimed_stream",
            return_value={"status": "error", "message": "calendar sync failed"},
        ):
            with patch("api.services.syncs.failure_policy.complete_connection_sync_lease") as complete_mock:
                with patch.object(stream_worker, "fail_connection_sync_lease") as fail_mock:
                    with patch(
                        "api.services.syncs.failure_policy.deactivate_connection_with_subscriptions"
                    ) as deactivate_mock:
                        with patch.object(
                            stream_worker,
                            "start_connection_sync_lease_heartbeat",
                            return_value=(MagicMock(), MagicMock()),
                        ):
                            with patch.object(stream_worker, "stop_connection_sync_lease_heartbeat"):
                                result = stream_worker.process_one_claim(
                                    claim,
                                    worker_id="worker-1",
                                    service_supabase=service_supabase,
                                )

    assert result["status"] == "quarantined"
    complete_mock.assert_called_once()
    fail_mock.assert_not_called()
    deactivate_mock.assert_called_once()


def test_run_forever_recovers_from_run_once_exception():
    from api.services.syncs import stream_worker

    with patch.object(stream_worker, "build_worker_id", return_value="worker-1"):
        with patch.object(stream_worker, "run_once", side_effect=[RuntimeError("boom"), KeyboardInterrupt]):
            with patch.object(stream_worker.time, "sleep") as sleep_mock:
                with pytest.raises(KeyboardInterrupt):
                    stream_worker.run_forever()

    sleep_mock.assert_called_once_with(5.0)


@pytest.mark.parametrize(
    ("configured_mode", "expected"),
    [
        (None, "any"),
        ("", "any"),
        ("mixed", "any"),
        ("realtime", "dirty_only"),
        ("dirty", "dirty_only"),
        ("reconcile", "reconcile_only"),
    ],
)
def test_resolve_worker_claim_mode_accepts_aliases(monkeypatch, configured_mode, expected):
    from api.services.syncs import stream_worker

    monkeypatch.delenv("SYNC_WORKER_MODE", raising=False)

    assert stream_worker.resolve_worker_claim_mode(configured_mode) == expected


def test_resolve_worker_claim_mode_rejects_invalid_mode(monkeypatch):
    from api.services.syncs import stream_worker

    monkeypatch.delenv("SYNC_WORKER_MODE", raising=False)

    with pytest.raises(ValueError, match="Unsupported SYNC_WORKER_MODE"):
        stream_worker.resolve_worker_claim_mode("burst-only")


def test_run_once_returns_none_when_nothing_claimed():
    from api.services.syncs import stream_worker

    service_supabase = MagicMock()

    with patch.object(stream_worker, "claim_connection_sync_lease", return_value=None):
        result = stream_worker.run_once(worker_id="worker-1", service_supabase=service_supabase)

    assert result is None


def test_run_once_passes_resolved_claim_mode_to_claim_rpc(monkeypatch):
    from api.services.syncs import stream_worker

    service_supabase = MagicMock()
    monkeypatch.setenv("SYNC_WORKER_MODE", "reconcile")

    with patch.object(stream_worker, "claim_connection_sync_lease", return_value=None) as claim_mock:
        result = stream_worker.run_once(worker_id="worker-1", service_supabase=service_supabase)

    assert result is None
    assert claim_mock.call_args.kwargs["claim_mode"] == stream_worker.CLAIM_MODE_RECONCILE_ONLY
