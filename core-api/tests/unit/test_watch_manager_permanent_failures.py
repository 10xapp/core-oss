import json
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError
from httplib2 import Response

# Prevent import-time Supabase initialization failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def _make_http_error(status: int, message: str) -> HttpError:
    payload = json.dumps({"error": {"message": message}}).encode("utf-8")
    return HttpError(Response({"status": str(status)}), payload, uri="https://googleapis.test")


def _make_service_supabase_mock() -> MagicMock:
    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.update.return_value = query
    query.insert.return_value = query
    query.execute.return_value = SimpleNamespace(data=[])

    supabase = MagicMock()
    supabase.table.return_value = query
    return supabase


def test_start_gmail_watch_service_role_permanent_failure_logs_warning_not_error():
    """
    Known permanent Gmail watch failures should be warning-level only.
    This prevents hourly Sentry spam for unrecoverable account capability issues.
    """
    from api.services.syncs.watch_manager import start_gmail_watch_service_role

    gmail_service = MagicMock()
    gmail_service.users.return_value.watch.return_value.execute.side_effect = _make_http_error(
        400,
        "Mail service not enabled",
    )

    supabase = _make_service_supabase_mock()

    with patch("api.services.syncs.watch_manager.settings", SimpleNamespace(
        google_pubsub_topic="projects/test/topics/gmail-sync-topic",
        webhook_base_url="https://core-api.test",
    )):
        with patch("api.services.syncs.watch_manager.logger") as logger_mock:
            result = start_gmail_watch_service_role(
                user_id="user-123",
                gmail_service=gmail_service,
                connection_id="conn-123",
                service_supabase=supabase,
            )

    assert result["success"] is False
    assert result["provider"] == "gmail"
    assert logger_mock.warning.called
    logger_mock.error.assert_not_called()


def test_start_calendar_watch_service_role_permanent_failure_logs_warning_not_error():
    """
    Known permanent Calendar watch failures should be warning-level only.
    """
    from api.services.syncs.watch_manager import start_calendar_watch_service_role

    calendar_service = MagicMock()
    calendar_service.events.return_value.watch.return_value.execute.side_effect = _make_http_error(
        403,
        "insufficientPermissions",
    )

    supabase = _make_service_supabase_mock()

    with patch("api.services.syncs.watch_manager.settings", SimpleNamespace(
        webhook_base_url="https://core-api.test",
        google_webhook_token="test-token",
    )):
        with patch("api.services.syncs.watch_manager.logger") as logger_mock:
            result = start_calendar_watch_service_role(
                user_id="user-123",
                calendar_service=calendar_service,
                connection_id="conn-123",
                service_supabase=supabase,
            )

    assert result["success"] is False
    assert result["provider"] == "calendar"
    assert logger_mock.warning.called
    logger_mock.error.assert_not_called()
    watch_call_kwargs = calendar_service.events.return_value.watch.call_args.kwargs
    assert watch_call_kwargs["body"]["token"] == "test-token"


def test_start_gmail_watch_service_role_transient_failure_still_logs_error():
    """
    Unknown/transient Gmail watch failures should remain error-level so we still
    capture genuinely unexpected incidents in Sentry.
    """
    from api.services.syncs.watch_manager import start_gmail_watch_service_role

    gmail_service = MagicMock()
    gmail_service.users.return_value.watch.return_value.execute.side_effect = _make_http_error(
        500,
        "backendError",
    )

    supabase = _make_service_supabase_mock()

    with patch("api.services.syncs.watch_manager.settings", SimpleNamespace(
        google_pubsub_topic="projects/test/topics/gmail-sync-topic",
        webhook_base_url="https://core-api.test",
    )):
        with patch("api.services.syncs.watch_manager.logger") as logger_mock:
            result = start_gmail_watch_service_role(
                user_id="user-123",
                gmail_service=gmail_service,
                connection_id="conn-123",
                service_supabase=supabase,
            )

    assert result["success"] is False
    assert result["provider"] == "gmail"
    assert any("Gmail API error" in str(call) for call in logger_mock.error.call_args_list)
    logger_mock.warning.assert_not_called()


def test_start_calendar_watch_service_role_transient_failure_still_logs_error():
    """
    Unknown/transient Calendar watch failures should remain error-level.
    """
    from api.services.syncs.watch_manager import start_calendar_watch_service_role

    calendar_service = MagicMock()
    calendar_service.events.return_value.watch.return_value.execute.side_effect = _make_http_error(
        500,
        "backendError",
    )

    supabase = _make_service_supabase_mock()

    with patch("api.services.syncs.watch_manager.settings", SimpleNamespace(
        webhook_base_url="https://core-api.test",
        google_webhook_token="test-token",
    )):
        with patch("api.services.syncs.watch_manager.logger") as logger_mock:
            result = start_calendar_watch_service_role(
                user_id="user-123",
                calendar_service=calendar_service,
                connection_id="conn-123",
                service_supabase=supabase,
            )

    assert result["success"] is False
    assert result["provider"] == "calendar"
    assert any("Calendar API error" in str(call) for call in logger_mock.error.call_args_list)
    logger_mock.warning.assert_not_called()
    watch_call_kwargs = calendar_service.events.return_value.watch.call_args.kwargs
    assert watch_call_kwargs["body"]["token"] == "test-token"
