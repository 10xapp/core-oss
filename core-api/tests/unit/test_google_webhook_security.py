import os


os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def test_calendar_channel_token_round_trip(monkeypatch):
    from lib.google_webhook_security import (
        build_google_calendar_channel_token,
        verify_google_calendar_channel_token,
    )

    monkeypatch.setattr(
        "api.config.settings.google_calendar_webhook_secret",
        "test-secret",
    )

    token = build_google_calendar_channel_token("conn-1", "channel-1")

    assert token is not None
    assert verify_google_calendar_channel_token("conn-1", "channel-1", token) is True
    assert verify_google_calendar_channel_token("conn-1", "channel-2", token) is False


def test_calendar_channel_token_verification_is_disabled_without_secret(monkeypatch):
    from lib.google_webhook_security import (
        build_google_calendar_channel_token,
        verify_google_calendar_channel_token,
    )

    monkeypatch.setattr(
        "api.config.settings.google_calendar_webhook_secret",
        "",
    )

    assert build_google_calendar_channel_token("conn-1", "channel-1") is None
    assert verify_google_calendar_channel_token("conn-1", "channel-1", None) is True
