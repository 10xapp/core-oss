import base64
import json
import os
from types import SimpleNamespace

import google.oauth2.id_token as google_id_token
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


class _FakeRequest:
    def __init__(self, body):
        self._body = body
        self.query_params = {}

    async def body(self):
        return json.dumps(self._body).encode("utf-8")

    async def json(self):
        return self._body


def _gmail_request(email: str = "user@example.com", history_id: str = "123") -> _FakeRequest:
    payload = base64.b64encode(
        json.dumps({"emailAddress": email, "historyId": history_id}).encode("utf-8")
    ).decode("utf-8")
    return _FakeRequest({"message": {"data": payload}})


class _BodyOnlyRequest:
    def __init__(self, body):
        self._body = body
        self.query_params = {}

    async def body(self):
        return json.dumps(self._body).encode("utf-8")


class _FakeQuery:
    def __init__(self, data):
        self._data = data

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def execute(self):
        return SimpleNamespace(data=self._data)


class _FakeSupabase:
    def __init__(self, table_map):
        self._table_map = table_map

    def table(self, name):
        return _FakeQuery(self._table_map.get(name, []))


def _clear_gmail_ingress_dedup_cache(webhooks) -> None:
    with webhooks._GMAIL_INGRESS_DEDUP_LOCK:
        webhooks._GMAIL_INGRESS_DEDUP_CACHE.clear()


@pytest.mark.asyncio
async def test_gmail_webhook_marks_stream_dirty(monkeypatch):
    from api.routers import webhooks

    _clear_gmail_ingress_dedup_cache(webhooks)
    supabase = _FakeSupabase({"ext_connections": [{"id": "conn-1"}]})
    marked = {}

    monkeypatch.setattr(webhooks, "get_service_role_client", lambda: supabase)
    monkeypatch.setattr(webhooks, "_verify_google_pubsub_auth", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        webhooks,
        "mark_stream_dirty",
        lambda *args, **kwargs: marked.update({"args": args, "kwargs": kwargs}) or True,
    )

    result = await webhooks.gmail_webhook(_gmail_request())

    assert result["status"] == "ok"
    assert result["message"] == "Accepted for async sync"
    assert marked["args"][1] == "conn-1"
    assert marked["args"][2] == "google"
    assert marked["args"][3] == "email"
    assert marked["kwargs"]["latest_seen_cursor"] == "123"
    assert marked["kwargs"]["metadata"]["source"] == "gmail-webhook"
    assert marked["kwargs"]["metadata"]["history_id"] == "123"


@pytest.mark.asyncio
async def test_gmail_webhook_reads_buffered_body_without_request_json(monkeypatch):
    from api.routers import webhooks

    _clear_gmail_ingress_dedup_cache(webhooks)
    payload = base64.b64encode(
        json.dumps({"emailAddress": "user@example.com", "historyId": "456"}).encode("utf-8")
    ).decode("utf-8")
    request = _BodyOnlyRequest({"message": {"data": payload}})
    supabase = _FakeSupabase({"ext_connections": [{"id": "conn-1"}]})
    marked = {}

    monkeypatch.setattr(webhooks, "get_service_role_client", lambda: supabase)
    monkeypatch.setattr(webhooks, "_verify_google_pubsub_auth", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        webhooks,
        "mark_stream_dirty",
        lambda *args, **kwargs: marked.update({"args": args, "kwargs": kwargs}) or True,
    )

    result = await webhooks.gmail_webhook(request)

    assert result["status"] == "ok"
    assert marked["kwargs"]["latest_seen_cursor"] == "456"


@pytest.mark.asyncio
async def test_gmail_webhook_skips_recent_duplicate_before_supabase(monkeypatch):
    from api.routers import webhooks

    _clear_gmail_ingress_dedup_cache(webhooks)
    supabase = _FakeSupabase({"ext_connections": [{"id": "conn-1"}]})
    counters = {"clients": 0, "marks": 0}

    def _get_client():
        counters["clients"] += 1
        return supabase

    def _mark(*_args, **_kwargs):
        counters["marks"] += 1
        return True

    monkeypatch.setattr(webhooks, "get_service_role_client", _get_client)
    monkeypatch.setattr(webhooks, "_verify_google_pubsub_auth", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(webhooks, "mark_stream_dirty", _mark)

    first = await webhooks.gmail_webhook(_gmail_request(history_id="789"))
    second = await webhooks.gmail_webhook(_gmail_request(history_id="789"))

    assert first["status"] == "ok"
    assert second["status"] == "ok"
    assert second["message"] == "Accepted duplicate notification"
    assert counters == {"clients": 1, "marks": 1}


@pytest.mark.asyncio
async def test_calendar_webhook_marks_stream_dirty(monkeypatch):
    from api.routers import webhooks

    supabase = _FakeSupabase({"push_subscriptions": [{"ext_connection_id": "conn-2", "resource_id": "resource-1"}]})
    marked = {}

    monkeypatch.setattr(webhooks, "get_service_role_client", lambda: supabase)
    monkeypatch.setattr(
        webhooks,
        "verify_google_calendar_channel_token",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        webhooks,
        "mark_stream_dirty",
        lambda *args, **kwargs: marked.update({"args": args, "kwargs": kwargs}) or True,
    )

    result = await webhooks.calendar_webhook(
        _FakeRequest({}),
        x_goog_channel_id="channel-1",
        x_goog_channel_token="token-1",
        x_goog_resource_id="resource-1",
        x_goog_resource_state="exists",
        x_goog_message_number="7",
    )

    assert result["status"] == "ok"
    assert result["message"] == "Accepted for async sync"
    assert marked["args"][1] == "conn-2"
    assert marked["args"][2] == "google"
    assert marked["args"][3] == "calendar"
    assert marked["kwargs"]["metadata"]["source"] == "google-calendar-webhook"
    assert marked["kwargs"]["metadata"]["channel_id"] == "channel-1"


@pytest.mark.asyncio
async def test_gmail_webhook_rejects_invalid_pubsub_auth(monkeypatch):
    from api.routers import webhooks

    _clear_gmail_ingress_dedup_cache(webhooks)
    monkeypatch.setattr(
        webhooks,
        "_verify_google_pubsub_auth",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            HTTPException(status_code=401, detail="bad token")
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        await webhooks.gmail_webhook(_gmail_request(), authorization="Bearer bad")

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_gmail_webhook_acknowledges_transient_dirty_mark_failures(monkeypatch):
    from api.routers import webhooks

    _clear_gmail_ingress_dedup_cache(webhooks)
    supabase = _FakeSupabase({"ext_connections": [{"id": "conn-1"}]})

    monkeypatch.setattr(webhooks, "get_service_role_client", lambda: supabase)
    monkeypatch.setattr(webhooks, "_verify_google_pubsub_auth", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        webhooks,
        "mark_stream_dirty",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("supabase unavailable")),
    )

    result = await webhooks.gmail_webhook(_gmail_request())

    assert result["status"] == "ok"
    assert result["message"] == "Accepted without dirty mark; reconciliation will recover"


def test_verify_google_pubsub_auth_converts_invalid_token_errors_to_http_exception(
    monkeypatch,
):
    from api.routers import webhooks

    webhooks._PUBSUB_JWT_CACHE.clear()
    monkeypatch.setattr(
        webhooks.settings,
        "google_pubsub_push_service_account_email",
        "pubsub-push@test-project.iam.gserviceaccount.com",
    )
    monkeypatch.setattr(
        webhooks.settings,
        "google_pubsub_push_audience",
        "https://core-webhooks.example.com/api/webhooks/gmail",
    )
    monkeypatch.setattr(
        google_id_token,
        "verify_oauth2_token",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("Token expired")),
    )

    with pytest.raises(HTTPException) as exc_info:
        webhooks._verify_google_pubsub_auth("Bearer bad-token")

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid Pub/Sub authorization token"


@pytest.mark.asyncio
async def test_calendar_webhook_rejects_invalid_channel_token(monkeypatch):
    from api.routers import webhooks

    supabase = _FakeSupabase({"push_subscriptions": [{"ext_connection_id": "conn-2", "resource_id": "resource-1"}]})

    monkeypatch.setattr(webhooks, "get_service_role_client", lambda: supabase)
    monkeypatch.setattr(
        webhooks,
        "verify_google_calendar_channel_token",
        lambda *_args, **_kwargs: False,
    )

    with pytest.raises(HTTPException) as exc_info:
        await webhooks.calendar_webhook(
            _FakeRequest({}),
            x_goog_channel_id="channel-1",
            x_goog_channel_token="wrong",
            x_goog_resource_id="resource-1",
        )

    assert exc_info.value.status_code == 401
