import json
import os
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

# Prevent import-time settings failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


class _FakeQuery:
    def __init__(self, data):
        self._data = data

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def in_(self, *_args, **_kwargs):
        return self

    def execute(self):
        return SimpleNamespace(data=self._data)


class _FakeSupabase:
    def __init__(self, connections):
        self._connections = connections

    def table(self, _name):
        return _FakeQuery(self._connections)


@pytest.mark.asyncio
async def test_calendar_sync_returns_accepted_when_streams_marked(monkeypatch):
    from api.routers import calendar as calendar_router
    import lib.supabase_client as supabase_client_module
    from api.services.syncs import sync_dispatcher

    fake_connections = [
        {"id": "conn-google-1", "provider": "google"},
        {"id": "conn-ms-1", "provider": "microsoft"},
    ]
    monkeypatch.setattr(
        supabase_client_module,
        "get_authenticated_supabase_client",
        lambda _jwt: _FakeSupabase(fake_connections),
    )
    monkeypatch.setattr(supabase_client_module, "get_service_role_client", lambda: _FakeSupabase([]))
    marked = {"calls": []}

    def _mark(*args, **kwargs):
        marked["calls"].append((args, kwargs))
        return True

    monkeypatch.setattr(sync_dispatcher, "mark_stream_dirty", _mark)

    response = await calendar_router.sync_google_calendar_endpoint(user_jwt="jwt", user_id="user-1")
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 202
    assert body["status"] == "accepted"
    assert body["jobs_enqueued"] == 2
    assert body["streams_marked"] == 2
    assert len(marked["calls"]) == 2
    assert all(
        call_kwargs["priority"] == sync_dispatcher.MANUAL_SYNC_PRIORITY
        for _, call_kwargs in marked["calls"]
    )


@pytest.mark.asyncio
async def test_calendar_sync_returns_202_when_streams_already_scheduled(monkeypatch):
    """When mark_stream_dirty returns False (already dirty/leased), still return 202."""
    from api.routers import calendar as calendar_router
    import lib.supabase_client as supabase_client_module
    from api.services.syncs import sync_dispatcher

    fake_connections = [{"id": "conn-google-1", "provider": "google"}]
    monkeypatch.setattr(
        supabase_client_module,
        "get_authenticated_supabase_client",
        lambda _jwt: _FakeSupabase(fake_connections),
    )
    monkeypatch.setattr(supabase_client_module, "get_service_role_client", lambda: _FakeSupabase([]))
    monkeypatch.setattr(sync_dispatcher, "mark_stream_dirty", lambda *_args, **_kwargs: False)

    response = await calendar_router.sync_google_calendar_endpoint(user_jwt="jwt", user_id="user-1")
    assert response.status_code == 202
