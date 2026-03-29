"""
Application-level encryption for OAuth tokens at rest.

Uses Fernet (AES-128-CBC + HMAC-SHA256) to encrypt access_token and
refresh_token before writing to the database, and decrypt after reading.

If TOKEN_ENCRYPTION_KEY is empty/unset, all functions are no-ops (passthrough)
to allow gradual rollout.
"""
import logging
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

# Fields that contain sensitive tokens and should be encrypted
_TOKEN_FIELDS = ("access_token", "refresh_token")


class TokenEncryptionError(RuntimeError):
    """Raised when token encryption cannot be performed safely."""


class TokenDecryptionError(TokenEncryptionError):
    """Raised when encrypted token data cannot be decrypted."""


def _get_runtime_key_attr(attr_name: str) -> str:
    """Read a token-encryption key from settings.

    Real settings values are always strings because they come from the validated
    Settings model. Some unit tests patch `api.config.settings` with partial
    mocks, which can make missing attributes look truthy (e.g. MagicMock()) and
    accidentally trigger runtime key parsing. Treat non-string values as unset so
    those tests can override unrelated settings without enabling encryption.
    """
    from api.config import settings

    value = getattr(settings, attr_name, "")
    return value if isinstance(value, str) else ""


def _build_fernet(key_value: str, key_name: str) -> Fernet:
    """Construct a Fernet instance or raise a runtime encryption error."""
    try:
        return Fernet(key_value.encode())
    except Exception as exc:
        logger.error("Invalid %s configured at runtime", key_name)
        raise TokenEncryptionError(f"{key_name} is invalid") from exc


def _get_current_fernet() -> Optional[Fernet]:
    """Return the current encryption key, if encryption is enabled."""
    current_key = _get_runtime_key_attr("token_encryption_key")
    if not current_key:
        return None
    return _build_fernet(current_key, "TOKEN_ENCRYPTION_KEY")


def _get_decryption_fernets() -> list[Fernet]:
    """Return Fernet instances for current + previous keys (if set).

    Lazy import of settings to avoid circular imports at module load time.
    """
    current_fernet = _get_current_fernet()
    if current_fernet is None:
        return []

    instances = [current_fernet]
    previous_key = _get_runtime_key_attr("token_encryption_key_previous")
    if previous_key:
        instances.append(
            _build_fernet(
                previous_key,
                "TOKEN_ENCRYPTION_KEY_PREVIOUS",
            )
        )
    return instances


def is_encrypted(value: str) -> bool:
    """Heuristic check: Fernet tokens are base64 and always start with 'gAAAAA'."""
    return isinstance(value, str) and value.startswith("gAAAAA") and len(value) >= 44


def encrypt_token(plaintext: Optional[str]) -> Optional[str]:
    """Encrypt a single token value. Returns None/empty unchanged."""
    if not plaintext:
        return plaintext

    fernet = _get_current_fernet()
    if fernet is None:
        return plaintext  # passthrough when no key configured

    try:
        return fernet.encrypt(plaintext.encode()).decode()
    except Exception as exc:
        logger.exception("Failed to encrypt OAuth token")
        raise TokenEncryptionError("Failed to encrypt OAuth token") from exc


def decrypt_token(ciphertext: Optional[str]) -> Optional[str]:
    """Decrypt a single token value.

    Gracefully returns the value as-is if:
    - Value is None or empty
    - No encryption key is configured (passthrough mode)
    - Value is not a valid Fernet token (plaintext during migration)
    """
    if not ciphertext:
        return ciphertext

    instances = _get_decryption_fernets()
    if not instances:
        return ciphertext  # passthrough when no key configured

    # Try each key (current first, then previous for rotation)
    for fernet in instances:
        try:
            return fernet.decrypt(ciphertext.encode()).decode()
        except InvalidToken:
            continue
        except Exception:
            logger.exception("Unexpected error while decrypting OAuth token")
            continue

    # If no key could decrypt and value doesn't look encrypted, it's plaintext
    if not is_encrypted(ciphertext):
        return ciphertext

    logger.error("Failed to decrypt encrypted OAuth token with configured keys")
    raise TokenDecryptionError(
        "Failed to decrypt encrypted OAuth token with configured keys"
    )


def encrypt_token_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """Encrypt access_token and refresh_token in a dict before DB write.

    Returns a new dict with encrypted token fields. Other keys are unchanged.
    """
    result = dict(data)
    for field in _TOKEN_FIELDS:
        if field in result and result[field]:
            result[field] = encrypt_token(result[field])
    return result


def decrypt_token_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """Decrypt access_token and refresh_token in a dict after DB read.

    Returns a new dict with decrypted token fields. Other keys are unchanged.
    """
    if not data:
        return data
    result = dict(data)
    for field in _TOKEN_FIELDS:
        if field in result and result[field]:
            result[field] = decrypt_token(result[field])
    return result


def decrypt_ext_connection_tokens(
    connection_data: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Decrypt token-bearing ext_connections rows returned from the database."""
    if not connection_data:
        return connection_data
    return decrypt_token_fields(connection_data)
