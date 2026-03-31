from pydantic import ValidationError
import pytest

from api.config import Settings


def test_tirith_timeout_must_be_positive():
    with pytest.raises(ValidationError, match="TIRITH_TIMEOUT must be greater than 0"):
        Settings(tirith_timeout=0)


def test_tirith_fail_mode_must_be_known():
    with pytest.raises(ValidationError, match="TIRITH_FAIL_MODE must be 'open' or 'closed'"):
        Settings(tirith_fail_mode="closd")


def test_tirith_settings_are_normalized():
    settings = Settings(
        tirith_fail_mode=" CLOSED ",
        tirith_path="  /usr/local/bin/tirith  ",
    )

    assert settings.tirith_fail_mode == "closed"
    assert settings.tirith_path == "/usr/local/bin/tirith"
