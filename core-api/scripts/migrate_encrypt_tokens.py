"""
Migration script: Encrypt existing plaintext OAuth tokens in ext_connections.

Reads all rows via service role, encrypts access_token and refresh_token
using Fernet, and updates in place. Idempotent — skips already-encrypted values.

Usage:
    # From project root with venv activated:
    python -m scripts.migrate_encrypt_tokens

Requires TOKEN_ENCRYPTION_KEY to be set in environment or .env.
"""
import logging
import os
import sys

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.supabase_client import get_service_role_client
from lib.token_encryption import encrypt_token, is_encrypted

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 100


def migrate() -> None:
    """Encrypt all plaintext tokens in ext_connections."""
    from api.config import settings

    if not settings.token_encryption_key:
        logger.error("TOKEN_ENCRYPTION_KEY is not set. Aborting migration.")
        sys.exit(1)

    supabase = get_service_role_client()

    # Count total rows
    count_result = supabase.table('ext_connections').select('id', count='exact').execute()
    total = count_result.count or 0
    logger.info(f"Found {total} ext_connections rows to process")

    encrypted_count = 0
    skipped_count = 0
    processed_count = 0
    last_seen_id = None

    while True:
        query = supabase.table('ext_connections')\
            .select('id, access_token, refresh_token')\
            .order('id')\
            .limit(BATCH_SIZE)

        if last_seen_id is not None:
            query = query.gt('id', last_seen_id)

        result = query.execute()

        rows = result.data or []
        if not rows:
            break

        for row in rows:
            update_data = {}
            access_token = row.get('access_token')
            refresh_token = row.get('refresh_token')

            # Skip if already encrypted or empty
            if access_token and not is_encrypted(access_token):
                update_data['access_token'] = encrypt_token(access_token)
            if refresh_token and not is_encrypted(refresh_token):
                update_data['refresh_token'] = encrypt_token(refresh_token)

            if update_data:
                supabase.table('ext_connections')\
                    .update(update_data)\
                    .eq('id', row['id'])\
                    .execute()
                encrypted_count += 1
            else:
                skipped_count += 1

        processed_count += len(rows)
        last_seen_id = rows[-1]['id']
        logger.info(
            f"Processed {processed_count}/{total} rows "
            f"(encrypted: {encrypted_count}, skipped: {skipped_count})"
        )

    logger.info(f"Migration complete. Encrypted: {encrypted_count}, Skipped: {skipped_count}, Total: {total}")


if __name__ == '__main__':
    migrate()
