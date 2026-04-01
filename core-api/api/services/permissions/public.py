"""Public (unauthenticated) share link resolution."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, Optional, cast
import logging

from fastapi import HTTPException, status

from lib.supabase_client import get_async_service_role_client
from lib.r2_client import get_r2_client
from api.services.documents.get_documents import _enrich_documents_with_image_urls

logger = logging.getLogger(__name__)

# Presigned URL expiration for public file shares (1 hour)
_PUBLIC_FILE_URL_EXPIRY = 3600


def _parse_db_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None


async def _get_share_link_row(client: Any, token: str) -> Optional[Dict[str, Any]]:
    """Resolve a link token or slug through the permissions table.

    Public rendering already runs with service-role access, so this path avoids
    depending on the raw SECURITY DEFINER RPC payload while preserving the same
    token-first lookup semantics as the SQL functions.
    """
    base_query = (
        client.table("permissions")
        .select("resource_type, resource_id, permission, granted_by, expires_at")
        .eq("grantee_type", "link")
    )

    token_result = await (
        base_query
        .eq("link_token", token)
        .limit(1)
        .execute()
    )
    token_rows = token_result.data or []
    if token_rows:
        row = cast(Dict[str, Any], token_rows[0])
    else:
        slug_result = await (
            client.table("permissions")
            .select("resource_type, resource_id, permission, granted_by, expires_at")
            .eq("grantee_type", "link")
            .eq("link_slug", token.lower())
            .limit(1)
            .execute()
        )
        slug_rows = slug_result.data or []
        if not slug_rows:
            return None
        row = cast(Dict[str, Any], slug_rows[0])

    expires_at = _parse_db_timestamp(row.get("expires_at"))
    if expires_at is not None and expires_at < datetime.now(timezone.utc):
        return None

    return row


async def _fetch_shared_by(user_id: Optional[str]) -> Optional[Dict[str, str]]:
    """Fetch sharer display info. Returns only name and avatar — no PII."""
    if not user_id:
        return None
    client = await get_async_service_role_client()
    result = await client.table("users") \
        .select("name, avatar_url") \
        .eq("id", user_id) \
        .maybe_single() \
        .execute()
    return result.data


async def get_public_shared_resource(token: str) -> Dict[str, Any]:
    """Resolve a share link for public viewing (no auth required).

    Returns only the fields needed for rendering — no internal IDs,
    storage paths, or user PII beyond the sharer's display name.
    """
    client = await get_async_service_role_client()
    link_row = await _get_share_link_row(client, token)
    if not link_row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Share link not found")

    resource_type = link_row.get("resource_type")
    resource_id = link_row.get("resource_id")
    if not resource_type or not resource_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid share link response")

    shared_by = await _fetch_shared_by(link_row.get("granted_by"))

    if resource_type in ("document", "folder"):
        doc_result = await client.table("documents") \
            .select("id, title, content, is_folder, created_at, updated_at, file:files(r2_key, file_type)") \
            .eq("id", resource_id) \
            .maybe_single() \
            .execute()

        if not doc_result.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")

        doc = doc_result.data

        if doc.get("is_folder"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Folder sharing is not supported for public links")

        # Enrich image URLs (needs file.r2_key + file.file_type), then strip internal data
        doc = cast(Dict[str, Any], doc)
        _enrich_documents_with_image_urls([doc])
        doc.pop("file", None)
        doc.pop("is_folder", None)

        return {
            "resource_type": "document",
            "resource_id": resource_id,
            "permission": link_row.get("permission"),
            "shared_by": shared_by,
            "document": doc,
        }

    if resource_type == "file":
        file_result = await client.table("files") \
            .select("id, filename, content_type, file_size, r2_key, created_at") \
            .eq("id", resource_id) \
            .maybe_single() \
            .execute()

        if not file_result.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

        file_row = file_result.data
        r2_key = file_row.get("r2_key")
        if not r2_key:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File not available")

        r2_client = get_r2_client()
        download_url = r2_client.get_presigned_url(r2_key, expiration=_PUBLIC_FILE_URL_EXPIRY)

        return {
            "resource_type": "file",
            "resource_id": resource_id,
            "permission": link_row.get("permission"),
            "shared_by": shared_by,
            "file": {
                "id": file_row["id"],
                "filename": file_row.get("filename"),
                "content_type": file_row.get("content_type"),
                "file_size": file_row.get("file_size"),
                "created_at": file_row.get("created_at"),
                "download_url": download_url,
            },
        }

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Public sharing not supported for this resource type")
