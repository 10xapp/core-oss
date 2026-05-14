"""
Tests for OAuth token encryption at rest.

Covers:
- Core encrypt/decrypt module (lib/token_encryption.py)
- Integration with write paths (encrypt before DB write)
- Integration with read paths (decrypt after DB read)
- Migration script logic
"""
import os
from unittest.mock import patch, MagicMock

import pytest
from cryptography.fernet import Fernet
from pydantic import ValidationError

# Prevent module import failures from lib.supabase_client singleton init.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRlc3QiLCJyb2xlIjoiYW5vbiJ9."
    "testsignature",
)

# Generate test keys
TEST_KEY = Fernet.generate_key().decode()
TEST_KEY_PREVIOUS = Fernet.generate_key().decode()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(**overrides):
    """Create a mock settings object with token encryption keys."""
    from types import SimpleNamespace
    defaults = {
        "token_encryption_key": TEST_KEY,
        "token_encryption_key_previous": "",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _patch_settings(**overrides):
    """Patch api.config.settings for token_encryption module."""
    return patch("api.config.settings", _make_settings(**overrides))


# ===========================================================================
# Core Module Tests — lib/token_encryption.py
# ===========================================================================

class TestEncryptDecryptRoundtrip:
    """Basic encrypt → decrypt should return original value."""

    def test_roundtrip(self):
        from lib.token_encryption import encrypt_token, decrypt_token
        with _patch_settings():
            original = "ya29.a0AfH6SMB_super_secret_access_token"
            encrypted = encrypt_token(original)
            assert encrypted != original
            assert decrypt_token(encrypted) == original

    def test_produces_unique_ciphertext(self):
        """Same plaintext encrypted twice should produce different ciphertexts (random IV)."""
        from lib.token_encryption import encrypt_token
        with _patch_settings():
            token = "ya29.a0AfH6SMB_test_token"
            enc1 = encrypt_token(token)
            enc2 = encrypt_token(token)
            assert enc1 != enc2  # Different IVs

    def test_decrypt_plaintext_passthrough(self):
        """Non-Fernet string should be returned as-is (migration safety)."""
        from lib.token_encryption import decrypt_token
        with _patch_settings():
            plaintext = "ya29.a0AfH6SMB_plaintext_token"
            assert decrypt_token(plaintext) == plaintext

    def test_encrypt_none_returns_none(self):
        from lib.token_encryption import encrypt_token
        with _patch_settings():
            assert encrypt_token(None) is None

    def test_encrypt_empty_returns_empty(self):
        from lib.token_encryption import encrypt_token
        with _patch_settings():
            assert encrypt_token("") == ""

    def test_decrypt_none_returns_none(self):
        from lib.token_encryption import decrypt_token
        with _patch_settings():
            assert decrypt_token(None) is None

    def test_decrypt_empty_returns_empty(self):
        from lib.token_encryption import decrypt_token
        with _patch_settings():
            assert decrypt_token("") == ""


class TestSettingsValidation:
    """Settings should fail fast on invalid token-encryption config."""

    def test_invalid_current_key_fails_settings_load(self):
        from api.config import Settings

        with pytest.raises(ValidationError, match="TOKEN_ENCRYPTION_KEY must be a valid Fernet key"):
            Settings(token_encryption_key="invalid-key")

    def test_invalid_previous_key_fails_settings_load(self):
        from api.config import Settings

        with pytest.raises(ValidationError, match="TOKEN_ENCRYPTION_KEY_PREVIOUS must be a valid Fernet key"):
            Settings(
                token_encryption_key=TEST_KEY,
                token_encryption_key_previous="invalid-key",
            )

    def test_previous_key_requires_current_key(self):
        from api.config import Settings

        with pytest.raises(
            ValidationError,
            match="TOKEN_ENCRYPTION_KEY_PREVIOUS requires TOKEN_ENCRYPTION_KEY to also be set",
        ):
            Settings(
                token_encryption_key="",
                token_encryption_key_previous=TEST_KEY_PREVIOUS,
            )


class TestIsEncrypted:
    """Heuristic detection of Fernet ciphertext."""

    def test_detects_fernet_token(self):
        from lib.token_encryption import encrypt_token, is_encrypted
        with _patch_settings():
            encrypted = encrypt_token("test_token")
            assert is_encrypted(encrypted) is True

    def test_rejects_plaintext(self):
        from lib.token_encryption import is_encrypted
        assert is_encrypted("ya29.a0AfH6SMB_not_encrypted") is False

    def test_rejects_short_string(self):
        from lib.token_encryption import is_encrypted
        assert is_encrypted("gAAAAA_short") is False

    def test_rejects_none(self):
        from lib.token_encryption import is_encrypted
        assert is_encrypted(None) is False


class TestEncryptTokenFields:
    """Dict-level encryption of access_token + refresh_token."""

    def test_encrypts_only_token_fields(self):
        from lib.token_encryption import encrypt_token_fields, is_encrypted
        with _patch_settings():
            data = {
                'access_token': 'at_plain',
                'refresh_token': 'rt_plain',
                'user_id': 'user-123',
                'provider': 'google',
            }
            result = encrypt_token_fields(data)

            assert is_encrypted(result['access_token'])
            assert is_encrypted(result['refresh_token'])
            assert result['user_id'] == 'user-123'
            assert result['provider'] == 'google'

    def test_preserves_missing_token_fields(self):
        """Dict without token fields should pass through unchanged."""
        from lib.token_encryption import encrypt_token_fields
        with _patch_settings():
            data = {'user_id': 'u1', 'provider': 'google'}
            result = encrypt_token_fields(data)
            assert result == data

    def test_skips_none_token_values(self):
        from lib.token_encryption import encrypt_token_fields
        with _patch_settings():
            data = {'access_token': 'at_plain', 'refresh_token': None}
            result = encrypt_token_fields(data)
            assert result['refresh_token'] is None


class TestDecryptTokenFields:
    """Dict-level decryption."""

    def test_decrypts_encrypted_fields(self):
        from lib.token_encryption import encrypt_token_fields, decrypt_token_fields
        with _patch_settings():
            original = {
                'access_token': 'at_secret',
                'refresh_token': 'rt_secret',
                'user_id': 'user-1',
            }
            encrypted = encrypt_token_fields(original)
            decrypted = decrypt_token_fields(encrypted)

            assert decrypted['access_token'] == 'at_secret'
            assert decrypted['refresh_token'] == 'rt_secret'
            assert decrypted['user_id'] == 'user-1'

    def test_handles_empty_dict(self):
        from lib.token_encryption import decrypt_token_fields
        assert decrypt_token_fields({}) == {}

    def test_handles_none(self):
        from lib.token_encryption import decrypt_token_fields
        assert decrypt_token_fields(None) is None


class TestKeyRotation:
    """Support decrypting with previous key during key rotation."""

    def test_decrypt_with_previous_key(self):
        from lib.token_encryption import encrypt_token, decrypt_token

        # Encrypt with the old key
        with _patch_settings(token_encryption_key=TEST_KEY_PREVIOUS):
            encrypted = encrypt_token("my_secret_token")

        # Decrypt with new primary key + old key as previous
        with _patch_settings(
            token_encryption_key=TEST_KEY,
            token_encryption_key_previous=TEST_KEY_PREVIOUS,
        ):
            decrypted = decrypt_token(encrypted)
            assert decrypted == "my_secret_token"

    def test_wrong_key_plaintext_fallback(self):
        """If decryption fails and value doesn't look like Fernet, return as-is."""
        from lib.token_encryption import decrypt_token
        wrong_key = Fernet.generate_key().decode()
        with _patch_settings(token_encryption_key=wrong_key):
            # This is plaintext that doesn't start with gAAAAA
            assert decrypt_token("ya29.plain_token") == "ya29.plain_token"

    def test_undecryptable_encrypted_value_raises(self):
        from lib.token_encryption import (
            TokenDecryptionError,
            decrypt_token,
            encrypt_token,
        )

        with _patch_settings(token_encryption_key=TEST_KEY_PREVIOUS):
            encrypted = encrypt_token("my_secret_token")

        with _patch_settings(token_encryption_key=TEST_KEY):
            with pytest.raises(
                TokenDecryptionError,
                match="Failed to decrypt encrypted OAuth token with configured keys",
            ):
                decrypt_token(encrypted)


class TestFailClosedRuntimeBehavior:
    """Runtime crypto errors must never silently fall back to plaintext."""

    def test_encrypt_failure_raises_instead_of_returning_plaintext(self):
        from lib.token_encryption import TokenEncryptionError, encrypt_token

        mock_fernet = MagicMock()
        mock_fernet.encrypt.side_effect = RuntimeError("boom")

        with _patch_settings():
            with patch("lib.token_encryption._get_current_fernet", return_value=mock_fernet):
                with pytest.raises(
                    TokenEncryptionError,
                    match="Failed to encrypt OAuth token",
                ):
                    encrypt_token("super-secret-token")


# ===========================================================================
# Integration Tests — Write Paths (encrypt before DB write)
# ===========================================================================

class TestWritePathEncryption:
    """Verify that token write paths encrypt before database operations."""

    def test_google_auth_refresh_encrypts_before_update(self):
        """_refresh_and_save_token should encrypt tokens before .update()."""
        from lib.token_encryption import is_encrypted
        from api.config import settings

        captured_update = {}

        def capture_update(data):
            captured_update.update(data)
            mock_chain = MagicMock()
            mock_chain.eq.return_value = mock_chain
            mock_chain.execute.return_value = MagicMock(data=[])
            return mock_chain

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.update = capture_update

        mock_credentials = MagicMock()
        mock_credentials.token = "new-access-token-from-google"
        mock_credentials.refresh_token = "new-refresh-token-from-google"

        connection_data = {
            'id': 'conn-123',
            'user_id': 'user-456',
            'access_token': 'old-token',
            'refresh_token': 'old-refresh',
            'token_expires_at': None,
            'metadata': {
                'client_id': 'test-client-id',
                'client_secret': 'test-client-secret',
            },
        }

        original_key = settings.token_encryption_key
        settings.token_encryption_key = TEST_KEY
        try:
            with patch('api.services.google_auth.get_service_role_client', return_value=mock_supabase):
                with patch('api.services.google_auth.Credentials', return_value=mock_credentials):
                    with patch('api.services.google_auth._refresh_creds'):
                        from api.services.google_auth import _refresh_and_save_token
                        _refresh_and_save_token(
                            connection_data,
                            mock_supabase,
                        )

            assert 'access_token' in captured_update
            assert is_encrypted(captured_update['access_token'])
        finally:
            settings.token_encryption_key = original_key


# ===========================================================================
# Integration Tests — Read Paths (decrypt after DB read)
# ===========================================================================

class TestReadPathDecryption:
    """Verify that token read paths decrypt after database operations."""

    def test_google_auth_get_credentials_decrypts(self):
        """get_credentials_for_connection should decrypt tokens from DB."""
        from lib.token_encryption import encrypt_token
        from api.config import settings
        from datetime import datetime, timezone, timedelta

        future_expiry = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

        original_key = settings.token_encryption_key
        settings.token_encryption_key = TEST_KEY
        try:
            encrypted_at = encrypt_token("real-access-token")
            encrypted_rt = encrypt_token("real-refresh-token")

            mock_result = MagicMock()
            mock_result.data = {
                'id': 'conn-1',
                'user_id': 'user-1',
                'access_token': encrypted_at,
                'refresh_token': encrypted_rt,
                'token_expires_at': future_expiry,
                'metadata': {},
            }

            mock_supabase = MagicMock()
            mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.single.return_value.execute.return_value = mock_result

            captured_creds_kwargs = {}

            def mock_credentials(**kwargs):
                captured_creds_kwargs.update(kwargs)
                return MagicMock()

            with patch('api.services.google_auth.Credentials', side_effect=mock_credentials):
                with patch('api.services.google_auth._is_token_expired', return_value=False):
                    from api.services.google_auth import get_credentials_for_connection
                    get_credentials_for_connection("conn-1", supabase_client=mock_supabase)

            # The Credentials constructor should receive the decrypted plaintext
            assert captured_creds_kwargs['token'] == "real-access-token"
            assert captured_creds_kwargs['refresh_token'] == "real-refresh-token"
        finally:
            settings.token_encryption_key = original_key
