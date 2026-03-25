import importlib
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Prevent module import failures from lib.supabase_client singleton init.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def _capture_credentials_kwargs():
    captured = {}

    def factory(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    return captured, factory


@pytest.mark.parametrize(
    "module_path",
    [
        "api.services.email.google_api_helpers",
        "api.services.calendar.google_api_helpers",
    ],
)
def test_service_helpers_build_refresh_capable_google_credentials(module_path):
    module = importlib.import_module(module_path)
    captured, factory = _capture_credentials_kwargs()
    connection_data = {
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "metadata": {},
    }

    with patch.object(module, "_refresh_google_token_if_needed", return_value="fresh-access-token"):
        with patch.object(module, "Credentials", side_effect=factory):
            with patch("api.config.settings", SimpleNamespace(
                google_client_id="client-id",
                google_client_secret="client-secret",
            )):
                credentials = module._get_google_credentials(connection_data)

    assert credentials.token == "fresh-access-token"
    assert captured == {
        "token": "fresh-access-token",
        "refresh_token": "refresh-token",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "client-id",
        "client_secret": "client-secret",
    }


def test_google_auth_returns_refresh_capable_credentials_when_token_is_still_valid():
    from api.services import google_auth

    captured, factory = _capture_credentials_kwargs()
    connection_data = {
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "token_expires_at": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
        "metadata": {},
    }

    with patch.object(google_auth, "Credentials", side_effect=factory):
        with patch.object(google_auth, "settings", SimpleNamespace(
            google_client_id="client-id",
            google_client_secret="client-secret",
        )):
            credentials = google_auth.get_valid_credentials(connection_data)

    assert credentials.token == "access-token"
    assert captured == {
        "token": "access-token",
        "refresh_token": "refresh-token",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "client-id",
        "client_secret": "client-secret",
    }


def test_google_auth_refresh_returns_the_full_refreshed_credentials_object():
    from api.services import google_auth

    connection_data = {
        "id": "connection-123",
        "user_id": "user-456",
        "access_token": "stale-access-token",
        "refresh_token": "refresh-token",
        "token_expires_at": (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(),
        "metadata": {},
    }

    refreshed_credentials = MagicMock()
    refreshed_credentials.token = "new-access-token"
    refreshed_credentials.refresh_token = "refresh-token"
    refreshed_credentials.refresh.return_value = None

    mock_supabase = MagicMock()
    mock_query = MagicMock()
    mock_supabase.table.return_value = mock_query
    mock_query.update.return_value.eq.return_value.execute.return_value = SimpleNamespace(data=[])

    with patch.object(google_auth, "Credentials", return_value=refreshed_credentials) as credentials_ctor:
        with patch.object(google_auth, "Request", return_value=object()):
            with patch.object(google_auth, "settings", SimpleNamespace(
                google_client_id="client-id",
                google_client_secret="client-secret",
            )):
                credentials = google_auth._refresh_and_save_token(connection_data, mock_supabase)

    assert credentials is refreshed_credentials
    credentials_ctor.assert_called_once_with(
        token="stale-access-token",
        refresh_token="refresh-token",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="client-id",
        client_secret="client-secret",
    )
