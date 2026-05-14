import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cryptography.fernet import Fernet

# Prevent module import failures from lib.supabase_client singleton init.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)

TEST_KEY = Fernet.generate_key().decode()


def _patch_settings(**overrides):
    defaults = {
        "token_encryption_key": TEST_KEY,
        "token_encryption_key_previous": "",
        "supabase_url": "",
        "supabase_anon_key": "",
        "supabase_service_role_key": "",
        "qstash_token": "",
        "qstash_worker_url": "",
        "qstash_url": "",
        "cron_secret": "",
    }
    defaults.update(overrides)
    return patch("api.config.settings", SimpleNamespace(**defaults))


def _make_encrypted_connection(provider: str) -> dict:
    with _patch_settings():
        from lib.token_encryption import encrypt_token

        return {
            "id": f"{provider}-conn-1",
            "user_id": "user-1",
            "provider": provider,
            "provider_email": f"{provider}@example.com",
            "access_token": encrypt_token(f"{provider}-access-token"),
            "refresh_token": encrypt_token(f"{provider}-refresh-token"),
            "token_expires_at": None,
            "metadata": {},
        }


def _make_email_lookup_client(email_row: dict):
    auth_supabase = MagicMock()
    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.maybe_single.return_value = query
    query.execute.return_value = MagicMock(data=email_row)
    auth_supabase.table.return_value = query
    return auth_supabase


def _make_subscription_client(subscription_row: dict):
    service_supabase = MagicMock()
    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.limit.return_value = query
    query.execute.return_value = MagicMock(data=[subscription_row])
    service_supabase.table.return_value = query
    return service_supabase


def test_get_email_attachment_uses_decrypted_joined_gmail_tokens():
    encrypted_connection = _make_encrypted_connection("google")
    auth_supabase = _make_email_lookup_client(
        {
            "id": "email-db-1",
            "external_id": "gmail-message-1",
            "body": "",
            "is_draft": False,
            "ext_connection_id": encrypted_connection["id"],
            "ext_connections": encrypted_connection,
        }
    )

    service = MagicMock()
    service.users.return_value.messages.return_value.attachments.return_value.get.return_value.execute.return_value = {
        "data": "ZGF0YQ==",
        "size": 4,
    }

    captured_connection = {}

    def _build_service(connection_data):
        captured_connection.update(connection_data)
        return service

    with _patch_settings():
        with patch(
            "api.services.email.get_email_details.get_authenticated_supabase_client",
            return_value=auth_supabase,
        ):
            with patch(
                "api.services.email.get_email_details.build_gmail_service_from_connection_data",
                side_effect=_build_service,
            ):
                from api.services.email.get_email_details import get_email_attachment

                result = get_email_attachment(
                    "user-1",
                    "jwt-token",
                    "gmail-message-1",
                    "attachment-1",
                )

    assert captured_connection["access_token"] == "google-access-token"
    assert captured_connection["refresh_token"] == "google-refresh-token"
    assert result["provider"] == "google"


def test_get_email_details_uses_decrypted_joined_outlook_tokens():
    encrypted_connection = _make_encrypted_connection("microsoft")
    auth_supabase = _make_email_lookup_client(
        {
            "id": "email-db-1",
            "external_id": "outlook-message-1",
            "body": "",
            "is_draft": False,
            "ext_connection_id": encrypted_connection["id"],
            "ext_connections": encrypted_connection,
        }
    )

    captured_kwargs = {}

    def _fake_outlook_details(**kwargs):
        captured_kwargs.update(kwargs)
        return {"provider": "microsoft", "email": {"id": "outlook-message-1"}}

    with _patch_settings():
        with patch(
            "api.services.email.get_email_details.get_authenticated_supabase_client",
            return_value=auth_supabase,
        ):
            with patch(
                "api.services.email.get_email_details._get_outlook_email_details",
                side_effect=_fake_outlook_details,
            ):
                from api.services.email.get_email_details import get_email_details

                result = get_email_details(
                    "user-1",
                    "jwt-token",
                    "outlook-message-1",
                )

    assert captured_kwargs["connection_data"]["access_token"] == "microsoft-access-token"
    assert captured_kwargs["connection_data"]["refresh_token"] == "microsoft-refresh-token"
    assert result["provider"] == "microsoft"


@pytest.mark.asyncio
async def test_process_microsoft_notification_decrypts_joined_tokens():
    encrypted_connection = _make_encrypted_connection("microsoft")
    service_supabase = _make_subscription_client(
        {
            "client_state": "client-state-1",
            "ext_connections": encrypted_connection,
        }
    )

    captured_connection = {}

    def _fake_process_notification(_self, _notification, connection_data):
        captured_connection.update(connection_data)
        return {"success": True, "path": "inline"}

    notification = {
        "subscriptionId": "subscription-1",
        "clientState": "client-state-1",
        "changeType": "updated",
        "resource": "me/messages",
    }

    with _patch_settings():
        with patch(
            "lib.supabase_client.get_service_role_client",
            return_value=service_supabase,
        ):
            with patch(
                "api.services.microsoft.microsoft_webhook_provider.MicrosoftWebhookProvider.validate_notification",
                return_value=True,
            ):
                with patch(
                    "api.services.microsoft.microsoft_webhook_provider.MicrosoftWebhookProvider.process_notification",
                    autospec=True,
                    side_effect=_fake_process_notification,
                ):
                    from api.routers.webhooks import process_microsoft_notification

                    result = await process_microsoft_notification(notification)

    assert captured_connection["access_token"] == "microsoft-access-token"
    assert captured_connection["refresh_token"] == "microsoft-refresh-token"
    assert result == {"success": True, "path": "inline"}


@pytest.mark.asyncio
async def test_mark_microsoft_notification_dirty_decrypts_tokens():
    encrypted_connection = _make_encrypted_connection("microsoft")
    service_supabase = _make_subscription_client(
        {
            "client_state": "client-state-1",
            "ext_connections": encrypted_connection,
        }
    )

    notification = {
        "subscriptionId": "subscription-1",
        "clientState": "client-state-1",
        "changeType": "updated",
        "resource": "me/messages",
    }

    with _patch_settings():
        with patch(
            "lib.supabase_client.get_service_role_client",
            return_value=service_supabase,
        ):
            with patch(
                "api.services.microsoft.microsoft_webhook_provider.MicrosoftWebhookProvider.validate_notification",
                return_value=True,
            ):
                with patch(
                    "api.routers.webhooks.mark_stream_dirty",
                    return_value=True,
                ) as dirty_mock:
                    from api.routers.webhooks import _mark_microsoft_notification_dirty

                    result = await _mark_microsoft_notification_dirty(notification)

    dirty_mock.assert_called_once()
    assert result == {"success": True, "marked": "sync-outlook"}
