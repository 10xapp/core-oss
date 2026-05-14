import os

import pytest

# Prevent import-time settings failures.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)

def test_cron_batch_mode_defaults_to_true(monkeypatch):
    from api.routers import cron

    monkeypatch.delenv("CRON_BATCH_MODE", raising=False)
    assert cron.is_cron_batch_mode_enabled() is True


def test_cron_batch_mode_false_values(monkeypatch):
    from api.routers import cron

    for value in ["0", "false", "False", "no", "off"]:
        monkeypatch.setenv("CRON_BATCH_MODE", value)
        assert cron.is_cron_batch_mode_enabled() is False


def test_cron_batch_mode_true_values(monkeypatch):
    from api.routers import cron

    for value in ["1", "true", "yes", "on"]:
        monkeypatch.setenv("CRON_BATCH_MODE", value)
        assert cron.is_cron_batch_mode_enabled() is True


def test_cron_batch_size_defaults_to_100(monkeypatch):
    from api.routers import cron

    monkeypatch.delenv("CRON_BATCH_SIZE", raising=False)
    assert cron.get_cron_batch_size() == 100


@pytest.mark.parametrize("value", ["1", "25", "100", "250"])
def test_cron_batch_size_accepts_valid_values(monkeypatch, value):
    from api.routers import cron

    monkeypatch.setenv("CRON_BATCH_SIZE", value)
    assert cron.get_cron_batch_size() == int(value)


@pytest.mark.parametrize("value", ["", "0", "-1", "251", "nope"])
def test_cron_batch_size_rejects_invalid_values(monkeypatch, value):
    from api.routers import cron

    monkeypatch.setenv("CRON_BATCH_SIZE", value)
    assert cron.get_cron_batch_size() == 100


@pytest.mark.asyncio
async def test_incremental_sync_returns_disabled_stub(monkeypatch):
    from api.routers import cron

    monkeypatch.setattr(cron, "verify_cron_auth", lambda *_: True)

    result = await cron.cron_incremental_sync(authorization="Bearer test")

    assert result["status"] == "disabled"
    assert "next_reconcile_at" in result["message"]
    assert result["streams_marked"] == 0
    assert result["jobs_enqueued"] == 0
    assert result["jobs_failed"] == 0
    assert result["batch_mode"] is False


@pytest.mark.asyncio
async def test_incremental_sync_requires_auth(monkeypatch):
    from api.routers import cron

    monkeypatch.setattr(cron, "verify_cron_auth", lambda *_: False)

    with pytest.raises(cron.HTTPException) as exc:
        await cron.cron_incremental_sync(authorization="Bearer test")

    assert exc.value.status_code == 401
