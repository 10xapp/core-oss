"""Tests for the Pub/Sub push verification helper."""
from unittest.mock import patch

import pytest
from fastapi import Request

from lib import webhook_auth
from lib.webhook_auth import PubSubAuthError, verify_pubsub_push


def _request(headers=None, path="/api/webhooks/gmail"):
    headers = headers or {}
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "scheme": "http",
        "server": ("testserver", 80),
    }
    return Request(scope)


def test_rejects_when_service_account_not_configured():
    with patch.object(webhook_auth.settings, "pubsub_push_service_account", ""):
        with pytest.raises(PubSubAuthError, match="not configured"):
            verify_pubsub_push(_request({"Authorization": "Bearer x"}))


def test_rejects_missing_bearer_header():
    with patch.object(webhook_auth.settings, "pubsub_push_service_account", "sa@example.iam.gserviceaccount.com"):
        with pytest.raises(PubSubAuthError, match="missing bearer token"):
            verify_pubsub_push(_request({}))


def test_rejects_bad_token():
    with patch.object(webhook_auth.settings, "pubsub_push_service_account", "sa@example.iam.gserviceaccount.com"), \
         patch.object(webhook_auth.settings, "pubsub_push_audience", "https://api.example.com/api/webhooks/gmail"), \
         patch.object(webhook_auth.id_token, "verify_oauth2_token", side_effect=ValueError("bad signature")):
        with pytest.raises(PubSubAuthError, match="token verification failed"):
            verify_pubsub_push(_request({"Authorization": "Bearer junk"}))


def test_rejects_token_from_unexpected_service_account():
    claims = {"email": "someone-else@example.iam.gserviceaccount.com", "email_verified": True}
    with patch.object(webhook_auth.settings, "pubsub_push_service_account", "sa@example.iam.gserviceaccount.com"), \
         patch.object(webhook_auth.settings, "pubsub_push_audience", "https://api.example.com/api/webhooks/gmail"), \
         patch.object(webhook_auth.id_token, "verify_oauth2_token", return_value=claims):
        with pytest.raises(PubSubAuthError, match="unexpected token issuer"):
            verify_pubsub_push(_request({"Authorization": "Bearer good"}))


def test_accepts_valid_token():
    sa = "sa@example.iam.gserviceaccount.com"
    claims = {"email": sa, "email_verified": True}
    with patch.object(webhook_auth.settings, "pubsub_push_service_account", sa), \
         patch.object(webhook_auth.settings, "pubsub_push_audience", "https://api.example.com/api/webhooks/gmail"), \
         patch.object(webhook_auth.id_token, "verify_oauth2_token", return_value=claims):
        verify_pubsub_push(_request({"Authorization": "Bearer good"}))


def test_rejects_unverified_email_claim():
    sa = "sa@example.iam.gserviceaccount.com"
    claims = {"email": sa, "email_verified": False}
    with patch.object(webhook_auth.settings, "pubsub_push_service_account", sa), \
         patch.object(webhook_auth.settings, "pubsub_push_audience", "https://api.example.com/api/webhooks/gmail"), \
         patch.object(webhook_auth.id_token, "verify_oauth2_token", return_value=claims):
        with pytest.raises(PubSubAuthError, match="email is not verified"):
            verify_pubsub_push(_request({"Authorization": "Bearer good"}))
