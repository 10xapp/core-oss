"""Shared helpers for permissions services."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import logging
import re

from fastapi import HTTPException, status

from lib.supabase_client import get_authenticated_async_client, get_async_service_role_client
from api.services.users import get_users_by_ids

logger = logging.getLogger(__name__)

VALID_RESOURCE_TYPES = {
    "workspace_app",
    "folder",
    "document",
    "file",
    "project_board",
    "channel",
}

VALID_PERMISSION_LEVELS = {"read", "write", "admin"}
LINK_SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$")
HEX_LINK_TOKEN_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def normalize_permission_level(permission: str) -> str:
    """Normalize and validate permission level."""
    normalized = permission.strip().lower()
    if normalized not in VALID_PERMISSION_LEVELS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Permission must be one of: read, write, admin",
        )
    return normalized


def normalize_resource_type(resource_type: str) -> str:
    """Normalize and validate resource type."""
    normalized = resource_type.strip().lower()
    if normalized not in VALID_RESOURCE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid resource type",
        )
    return normalized


def normalize_link_slug(slug: str) -> str:
    """Normalize and validate a custom link slug."""
    normalized = slug.strip().lower()
    if not normalized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Link slug cannot be empty",
        )
    if not LINK_SLUG_PATTERN.fullmatch(normalized):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Link slug must be 3-64 chars, lowercase letters/numbers/hyphens, and cannot start or end with a hyphen",
        )
    if HEX_LINK_TOKEN_PATTERN.fullmatch(normalized):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Link slug cannot be a 32-character hexadecimal token",
        )
    return normalized


async def can_manage_shares(
    user_id: str,
    user_jwt: str,
    resource_type: str,
    resource_id: str,
) -> bool:
    """Return whether user can manage shares for the resource."""
    supabase = await get_authenticated_async_client(user_jwt)
    result = await supabase.rpc(
        "can_manage_shares",
        {
            "p_user_id": user_id,
            "p_resource_type": resource_type,
            "p_resource_id": resource_id,
        },
    ).execute()

    return bool(result.data)


async def assert_can_manage_shares(
    user_id: str,
    user_jwt: str,
    resource_type: str,
    resource_id: str,
) -> None:
    """Raise 403 if user cannot manage shares for the resource."""
    if not await can_manage_shares(user_id, user_jwt, resource_type, resource_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to manage sharing for this resource",
        )


async def resolve_resource_context(
    resource_type: str,
    resource_id: str,
) -> Dict[str, Any]:
    """Resolve workspace and metadata for a resource.

    Returns:
        Dict with: resource_type, resource_id, workspace_id, workspace_app_id,
        title, owner_id, file_id, document_id, is_folder.
    """
    normalized_type = normalize_resource_type(resource_type)
    client = await get_async_service_role_client()

    if normalized_type in ("document", "folder"):
        result = await client.table("documents") \
            .select("id, title, is_folder, workspace_id, workspace_app_id, user_id, file_id") \
            .eq("id", resource_id) \
            .maybe_single() \
            .execute()

        if not result.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")

        doc = result.data
        normalized_type = "folder" if doc.get("is_folder") else "document"
        return {
            "resource_type": normalized_type,
            "resource_id": resource_id,
            "workspace_id": doc.get("workspace_id"),
            "workspace_app_id": doc.get("workspace_app_id"),
            "title": doc.get("title"),
            "owner_id": doc.get("user_id"),
            "file_id": doc.get("file_id"),
            "document_id": doc.get("id") or resource_id,
            "is_folder": bool(doc.get("is_folder")),
        }

    if normalized_type == "file":
        document_result = await client.table("documents") \
            .select("id") \
            .eq("file_id", resource_id) \
            .limit(1) \
            .execute()

        result = await client.table("files") \
            .select("id, filename, workspace_id, workspace_app_id, user_id") \
            .eq("id", resource_id) \
            .maybe_single() \
            .execute()

        if not result.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

        file_row = result.data
        return {
            "resource_type": normalized_type,
            "resource_id": resource_id,
            "workspace_id": file_row.get("workspace_id"),
            "workspace_app_id": file_row.get("workspace_app_id"),
            "title": file_row.get("filename"),
            "owner_id": file_row.get("user_id"),
            "file_id": None,
            "document_id": (document_result.data or [{}])[0].get("id"),
            "is_folder": False,
        }

    if normalized_type == "project_board":
        result = await client.table("project_boards") \
            .select("id, name, workspace_id, workspace_app_id, created_by") \
            .eq("id", resource_id) \
            .maybe_single() \
            .execute()

        if not result.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project board not found")

        board = result.data
        return {
            "resource_type": normalized_type,
            "resource_id": resource_id,
            "workspace_id": board.get("workspace_id"),
            "workspace_app_id": board.get("workspace_app_id"),
            "title": board.get("name"),
            "owner_id": board.get("created_by"),
            "file_id": None,
            "document_id": None,
            "is_folder": False,
        }

    if normalized_type == "channel":
        result = await client.table("channels") \
            .select("id, name, workspace_app_id, created_by") \
            .eq("id", resource_id) \
            .maybe_single() \
            .execute()

        if not result.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Channel not found")

        channel = result.data
        workspace_app_id = channel.get("workspace_app_id")
        workspace_id = None
        if workspace_app_id:
            ws_result = await client.table("workspace_apps") \
                .select("workspace_id") \
                .eq("id", workspace_app_id) \
                .maybe_single() \
                .execute()
            workspace_id = ws_result.data.get("workspace_id") if ws_result.data else None

        return {
            "resource_type": normalized_type,
            "resource_id": resource_id,
            "workspace_id": workspace_id,
            "workspace_app_id": workspace_app_id,
            "title": channel.get("name"),
            "owner_id": channel.get("created_by"),
            "file_id": None,
            "document_id": None,
            "is_folder": False,
        }

    if normalized_type == "workspace_app":
        result = await client.table("workspace_apps") \
            .select("id, app_type, workspace_id") \
            .eq("id", resource_id) \
            .maybe_single() \
            .execute()

        if not result.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workspace app not found")

        app = result.data
        return {
            "resource_type": normalized_type,
            "resource_id": resource_id,
            "workspace_id": app.get("workspace_id"),
            "workspace_app_id": app.get("id"),
            "title": app.get("app_type"),
            "owner_id": None,
            "file_id": None,
            "document_id": None,
            "is_folder": False,
        }

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid resource type")


async def get_workspace_admin_ids(workspace_id: Optional[str]) -> List[str]:
    """Return workspace admin/owner user_ids for a workspace."""
    if not workspace_id:
        return []

    client = await get_async_service_role_client()
    result = await client.table("workspace_members") \
        .select("user_id, role") \
        .eq("workspace_id", workspace_id) \
        .in_("role", ["owner", "admin"]) \
        .execute()

    return [row.get("user_id") for row in (result.data or []) if row.get("user_id")]


async def get_workspace_members_with_profiles(workspace_id: Optional[str]) -> List[Dict[str, Any]]:
    """Return workspace members enriched with profile info (service role)."""
    if not workspace_id:
        return []

    client = await get_async_service_role_client()
    result = await client.table("workspace_members") \
        .select("id, user_id, role, joined_at") \
        .eq("workspace_id", workspace_id) \
        .order("joined_at") \
        .execute()

    members = result.data or []
    user_ids = list({m.get("user_id") for m in members if m.get("user_id")})
    user_map = await get_users_by_ids(user_ids)

    for member in members:
        user_info = user_map.get(member.get("user_id"), {})
        member["email"] = user_info.get("email")
        member["name"] = user_info.get("name")
        member["avatar_url"] = user_info.get("avatar_url")

    return members


def filter_active_permissions(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter out expired permissions in-memory."""
    now = datetime.now(timezone.utc)
    active: List[Dict[str, Any]] = []
    for row in rows:
        expires_at = row.get("expires_at")
        if not expires_at:
            active.append(row)
            continue
        try:
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if expires_dt > now:
                active.append(row)
        except Exception:
            # If parsing fails, keep the row to avoid hiding data unexpectedly.
            active.append(row)
    return active
