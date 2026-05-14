import os
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch


# Prevent lib.supabase_client singleton init from failing during helper imports.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def _make_query_builder(*, execute_side_effect=None, execute_return_value=None):
    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.gte.return_value = query
    query.lte.return_value = query
    query.lt.return_value = query
    query.is_.return_value = query
    query.delete.return_value = query
    query.update.return_value = query
    query.maybe_single.return_value = query
    if execute_side_effect is not None:
        query.execute.side_effect = execute_side_effect
    else:
        query.execute.return_value = execute_return_value or SimpleNamespace(data=[])
    return query


def test_sync_google_calendar_cron_persists_sync_token_after_clean_sync():
    from api.services.syncs.sync_google_calendar_cron import sync_google_calendar_cron

    ext_connections_query = _make_query_builder(
        execute_side_effect=[
            SimpleNamespace(data={"provider_email": "user@example.com"}),
            SimpleNamespace(data=[]),
        ]
    )
    calendar_events_query = _make_query_builder(
        execute_side_effect=[
            SimpleNamespace(data=[]),
            SimpleNamespace(data=[]),
            SimpleNamespace(data=[]),
            SimpleNamespace(data=[]),
        ]
    )
    push_subscriptions_query = _make_query_builder(
        execute_return_value=SimpleNamespace(data=[])
    )

    service_supabase = MagicMock()
    service_supabase.table.side_effect = lambda name: {
        "ext_connections": ext_connections_query,
        "calendar_events": calendar_events_query,
        "push_subscriptions": push_subscriptions_query,
    }[name]

    calendar_service = MagicMock()
    calendar_service.events.return_value.list.return_value.execute.return_value = {
        "items": [{"id": "event-1"}],
        "nextSyncToken": "sync-123",
    }

    with patch("api.services.syncs.sync_google_calendar_cron.get_existing_external_ids", return_value=set()):
        with patch(
            "api.services.syncs.sync_google_calendar_cron.batch_upsert",
            return_value={"success_count": 1, "error_count": 0, "errors": []},
        ):
            with patch(
                "api.services.syncs.sync_google_calendar_cron.parse_google_event_to_data",
                return_value={"external_id": "event-1", "user_id": "user-123"},
            ):
                with patch(
                    "api.services.syncs.sync_google_calendar_cron.get_calendar_event_rows_by_external_ids",
                    return_value={},
                ):
                    with patch(
                        "api.services.syncs.sync_google_calendar_cron.reconcile_calendar_invite_notifications"
                    ):
                        result = sync_google_calendar_cron(
                            calendar_service=calendar_service,
                            connection_id="connection-123",
                            user_id="user-123",
                            service_supabase=service_supabase,
                        )

    assert result["status"] == "success"
    assert result["new_sync_token"] == "sync-123"
    push_subscriptions_query.update.assert_called_once_with(
        {"sync_token": "sync-123", "updated_at": ANY}
    )
    push_subscriptions_query.eq.assert_any_call("ext_connection_id", "connection-123")
    push_subscriptions_query.eq.assert_any_call("provider", "calendar")
    push_subscriptions_query.eq.assert_any_call("is_active", True)

