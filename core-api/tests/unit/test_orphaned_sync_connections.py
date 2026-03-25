import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


# Prevent lib.supabase_client singleton init from failing during helper imports.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


ORPHANED_USER_ERROR = (
    "Batch upsert failed for 1 records: {'code': '23503', 'details': "
    "'Key (user_id)=(user-123) is not present in table \"users\".', "
    "'message': 'insert or update on table \"entities\" violates foreign key "
    "constraint \"entities_user_id_fkey\"'}"
)


def _make_query_builder(*, execute_side_effect=None, execute_return_value=None):
    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.single.return_value = query
    query.update.return_value = query
    if execute_side_effect is not None:
        query.execute.side_effect = execute_side_effect
    else:
        query.execute.return_value = execute_return_value or SimpleNamespace(data=[])
    return query


def test_sync_gmail_cron_deactivates_connection_when_user_is_orphaned():
    from api.services.syncs.sync_gmail_cron import sync_gmail_cron

    ext_connections_query = _make_query_builder(
        execute_side_effect=[
            SimpleNamespace(data={"last_synced": None}),
            SimpleNamespace(data=[]),
        ]
    )
    push_subscriptions_query = _make_query_builder(
        execute_return_value=SimpleNamespace(data=[])
    )

    service_supabase = MagicMock()
    service_supabase.table.side_effect = lambda name: {
        "ext_connections": ext_connections_query,
        "push_subscriptions": push_subscriptions_query,
    }[name]

    gmail_service = MagicMock()
    messages_api = gmail_service.users.return_value.messages.return_value
    messages_api.list.return_value.execute.return_value = {"messages": [{"id": "msg-1"}]}
    messages_api.get.return_value.execute.return_value = {
        "id": "msg-1",
        "threadId": "thread-1",
        "labelIds": ["INBOX"],
        "internalDate": "1710000000000",
        "payload": {"headers": []},
    }

    with patch("api.services.syncs.sync_gmail_cron.get_existing_external_ids", return_value=set()):
        with patch(
            "api.services.syncs.sync_gmail_cron.batch_upsert",
            return_value={"success_count": 0, "error_count": 1, "errors": [ORPHANED_USER_ERROR]},
        ):
            with patch("api.services.email.google_api_helpers.parse_email_headers", return_value={"subject": "Hi", "from": "sender@example.com"}):
                with patch("api.services.email.google_api_helpers.decode_email_body", return_value={"plain": "Hello"}):
                    with patch("api.services.email.google_api_helpers.get_attachment_info", return_value=[]):
                        result = sync_gmail_cron(
                            gmail_service=gmail_service,
                            connection_id="connection-123",
                            user_id="user-123",
                            service_supabase=service_supabase,
                        )

    assert result["status"] == "quarantined"
    assert "deactivated" in result["message"].lower()
    ext_connections_query.update.assert_called_once()
    push_subscriptions_query.update.assert_called_once()


def test_sync_google_calendar_cron_deactivates_connection_when_user_is_orphaned():
    from api.services.syncs.sync_google_calendar_cron import sync_google_calendar_cron

    ext_connections_query = _make_query_builder(
        execute_return_value=SimpleNamespace(data=[])
    )
    push_subscriptions_query = _make_query_builder(
        execute_return_value=SimpleNamespace(data=[])
    )

    service_supabase = MagicMock()
    service_supabase.table.side_effect = lambda name: {
        "ext_connections": ext_connections_query,
        "push_subscriptions": push_subscriptions_query,
    }[name]

    calendar_service = MagicMock()
    calendar_service.events.return_value.list.return_value.execute.return_value = {
        "items": [{"id": "event-1"}]
    }

    with patch("api.services.syncs.sync_google_calendar_cron.get_existing_external_ids", return_value=set()):
        with patch(
            "api.services.syncs.sync_google_calendar_cron.batch_upsert",
            return_value={"success_count": 0, "error_count": 1, "errors": [ORPHANED_USER_ERROR]},
        ):
            with patch(
                "api.services.syncs.sync_google_calendar_cron.parse_google_event_to_data",
                return_value={"external_id": "event-1", "user_id": "user-123"},
            ):
                result = sync_google_calendar_cron(
                    calendar_service=calendar_service,
                    connection_id="connection-123",
                    user_id="user-123",
                    service_supabase=service_supabase,
                )

    assert result["status"] == "quarantined"
    assert "deactivated" in result["message"].lower()
    ext_connections_query.update.assert_called_once()
    push_subscriptions_query.update.assert_called_once()
