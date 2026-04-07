import os

# Prevent import-time settings failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)


def test_get_failed_sync_quarantine_reason_marks_permanent_microsoft_auth_errors():
    from api.services.syncs.failure_policy import get_failed_sync_quarantine_reason

    reason = get_failed_sync_quarantine_reason(
        provider="microsoft",
        error="Refresh token is invalid - user must re-authenticate",
        retry_count=0,
    )

    assert reason == (
        "Permanent Microsoft auth failure: "
        "Refresh token is invalid - user must re-authenticate"
    )
