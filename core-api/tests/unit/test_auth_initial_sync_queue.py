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


class _FakeSupabase:
    def rpc(self, _name, _params):
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=True))


@pytest.mark.asyncio
async def test_google_initial_sync_marks_both_streams_dirty(monkeypatch):
    from api.services import auth
    import lib.supabase_client as supabase_client_module
    from api.services.syncs import sync_dispatcher

    mark_mock = MagicMock(side_effect=[True, True])
    monkeypatch.setattr(sync_dispatcher, "mark_stream_dirty", mark_mock)
    monkeypatch.setattr(supabase_client_module, "get_service_role_client", lambda: _FakeSupabase())

    await auth._enqueue_or_fallback_google_initial_sync(
        connection_id="conn-1",
        user_id="user-1",
        access_token="access",
        refresh_token="refresh",
        provider_email="user@example.com",
    )

    assert mark_mock.call_count == 2

    gmail_call = mark_mock.call_args_list[0]
    assert gmail_call.args[1] == "conn-1"
    assert gmail_call.args[2] == "google"
    assert gmail_call.args[3] == "email"
    assert gmail_call.kwargs["metadata"]["source"] == "initial-sync"
    assert gmail_call.kwargs["metadata"]["initial_sync"] is True
    assert gmail_call.kwargs["metadata"]["max_results"] == 50
    assert gmail_call.kwargs["metadata"]["days_back"] == 20

    calendar_call = mark_mock.call_args_list[1]
    assert calendar_call.args[2] == "google"
    assert calendar_call.args[3] == "calendar"
    assert calendar_call.kwargs["metadata"]["days_past"] == 7
    assert calendar_call.kwargs["metadata"]["days_future"] == 60


@pytest.mark.asyncio
async def test_microsoft_initial_sync_marks_email_only_when_calendar_disabled(monkeypatch):
    from api.services import auth
    import lib.supabase_client as supabase_client_module
    from api.services.syncs import sync_dispatcher

    mark_mock = MagicMock(return_value=True)
    monkeypatch.setattr(sync_dispatcher, "mark_stream_dirty", mark_mock)
    monkeypatch.setattr(supabase_client_module, "get_service_role_client", lambda: _FakeSupabase())

    await auth._enqueue_or_fallback_microsoft_initial_sync(
        connection_id="conn-1",
        user_id="user-1",
        access_token="access",
        refresh_token="refresh",
        token_expires_at="2026-03-03T10:00:00+00:00",
        metadata={},
        provider_email="user@example.com",
        include_calendar=False,
    )

    assert mark_mock.call_count == 1
    only_call = mark_mock.call_args_list[0]
    assert only_call.args[1] == "conn-1"
    assert only_call.args[2] == "microsoft"
    assert only_call.args[3] == "email"
    assert only_call.kwargs["metadata"]["initial_sync"] is True
    assert only_call.kwargs["metadata"]["days_back"] == 20
