"""
Workspace app management
Handles workspace mini-apps (files, messages, projects, etc.).

Uses async Supabase client for non-blocking I/O.
"""
from typing import Dict, Any, List, Optional
import logging
from lib.supabase_client import get_authenticated_async_client

logger = logging.getLogger(__name__)

# Valid app types
VALID_APP_TYPES = (
    "chat",
    "files",
    "messages",
    "dashboard",
    "projects",
    "email",
    "calendar",
    "agents",
)


async def get_workspace_apps(
    workspace_id: str,
    user_jwt: str
) -> List[Dict[str, Any]]:
    """
    Get all apps in a workspace.
    Returns apps the user has access to (public or explicitly added).

    Args:
        workspace_id: Workspace ID
        user_jwt: User's Supabase JWT

    Returns:
        List of apps with access info
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        result = await supabase.table("workspace_apps")\
            .select("id, workspace_id, app_type, is_public, position, config, created_at")\
            .eq("workspace_id", workspace_id)\
            .order("position")\
            .execute()

        logger.info(f"Fetched {len(result.data or [])} apps for workspace {workspace_id}")
        return result.data or []

    except Exception as e:
        logger.exception(f"Error fetching apps for workspace {workspace_id}: {e}")
        raise


async def get_workspace_app(
    workspace_app_id: str,
    user_jwt: str
) -> Optional[Dict[str, Any]]:
    """
    Get a single workspace app by ID.

    Args:
        workspace_app_id: App ID
        user_jwt: User's Supabase JWT

    Returns:
        App data or None
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        result = await supabase.table("workspace_apps")\
            .select("*")\
            .eq("id", workspace_app_id)\
            .maybe_single()\
            .execute()

        return result.data

    except Exception as e:
        logger.exception(f"Error fetching app {workspace_app_id}: {e}")
        raise


async def get_workspace_app_by_type(
    workspace_id: str,
    app_type: str,
    user_jwt: str
) -> Optional[Dict[str, Any]]:
    """
    Get a workspace app by type.

    Args:
        workspace_id: Workspace ID
        app_type: App type (chat, files, messages, dashboard, projects, email, calendar, agents)
        user_jwt: User's Supabase JWT

    Returns:
        App data or None
    """
    try:
        if app_type not in VALID_APP_TYPES:
            raise ValueError(f"Invalid app type. Must be one of: {VALID_APP_TYPES}")

        supabase = await get_authenticated_async_client(user_jwt)

        result = await supabase.table("workspace_apps")\
            .select("*")\
            .eq("workspace_id", workspace_id)\
            .eq("app_type", app_type)\
            .maybe_single()\
            .execute()

        return result.data

    except Exception as e:
        logger.exception(f"Error fetching {app_type} app for workspace {workspace_id}: {e}")
        raise


async def update_workspace_app(
    workspace_app_id: str,
    user_jwt: str,
    is_public: Optional[bool] = None,
    position: Optional[int] = None,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Update a workspace app's settings.
    RLS ensures only admins/owners can update.

    Args:
        workspace_app_id: App ID
        user_jwt: User's Supabase JWT
        is_public: Whether app is visible to all workspace members
        position: Display order position
        config: App-specific configuration JSON

    Returns:
        Updated app data
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        update_data: Dict[str, Any] = {}
        if is_public is not None:
            update_data["is_public"] = is_public
        if position is not None:
            update_data["position"] = position
        if config is not None:
            update_data["config"] = config

        if not update_data:
            # Nothing to update, just fetch current data
            result = await supabase.table("workspace_apps")\
                .select("*")\
                .eq("id", workspace_app_id)\
                .single()\
                .execute()
            return result.data

        result = await supabase.table("workspace_apps")\
            .update(update_data)\
            .eq("id", workspace_app_id)\
            .execute()

        if not result.data:
            raise ValueError("App not found or not authorized to update")

        logger.info(f"Updated workspace app {workspace_app_id}")
        return result.data[0]

    except Exception as e:
        logger.exception(f"Error updating workspace app {workspace_app_id}: {e}")
        raise


async def add_app_member(
    workspace_app_id: str,
    user_jwt: str,
    member_user_id: str,
    added_by_user_id: str
) -> Dict[str, Any]:
    """
    Add a user to a private app.
    RLS ensures only admins/owners can add members.

    Args:
        workspace_app_id: App ID
        user_jwt: User's Supabase JWT
        member_user_id: ID of user to add
        added_by_user_id: ID of user adding the member

    Returns:
        Created app membership data

    Raises:
        ValueError: If user is already a member
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        # Check if user is already a member
        existing = await supabase.table("workspace_app_members")\
            .select("id")\
            .eq("workspace_app_id", workspace_app_id)\
            .eq("user_id", member_user_id)\
            .maybe_single()\
            .execute()

        if existing.data:
            raise ValueError("User already has access to this app")

        # Add member
        result = await supabase.table("workspace_app_members")\
            .insert({
                "workspace_app_id": workspace_app_id,
                "user_id": member_user_id,
                "added_by": added_by_user_id
            })\
            .execute()

        if not result.data:
            raise Exception("Failed to add app member - not authorized")

        logger.info(f"Added user {member_user_id} to app {workspace_app_id}")
        return result.data[0]

    except Exception as e:
        logger.exception(f"Error adding member to app {workspace_app_id}: {e}")
        raise


async def remove_app_member(
    workspace_app_id: str,
    user_jwt: str,
    member_user_id: str
) -> bool:
    """
    Remove a user from a private app.
    RLS ensures only admins/owners can remove members.

    Args:
        workspace_app_id: App ID
        user_jwt: User's Supabase JWT
        member_user_id: ID of member to remove

    Returns:
        True if removed successfully

    Raises:
        ValueError: If member not found
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        # Check if member exists
        existing = await supabase.table("workspace_app_members")\
            .select("id")\
            .eq("workspace_app_id", workspace_app_id)\
            .eq("user_id", member_user_id)\
            .maybe_single()\
            .execute()

        if not existing.data:
            raise ValueError("User does not have explicit access to this app")

        # Remove member
        result = await supabase.table("workspace_app_members")\
            .delete()\
            .eq("workspace_app_id", workspace_app_id)\
            .eq("user_id", member_user_id)\
            .execute()

        if not result.data:
            raise ValueError("Failed to remove member - not authorized")

        logger.info(f"Removed user {member_user_id} from app {workspace_app_id}")
        return True

    except Exception as e:
        logger.exception(f"Error removing member from app {workspace_app_id}: {e}")
        raise


async def create_workspace_app(
    workspace_id: str,
    app_type: str,
    user_jwt: str,
    is_public: bool = True,
    position: Optional[int] = None
) -> Dict[str, Any]:
    """
    Create a new app in a workspace.
    RLS ensures only admins/owners can create apps.

    Args:
        workspace_id: Workspace ID
        app_type: App type (chat, files, messages, dashboard, projects, email, calendar, agents)
        user_jwt: User's Supabase JWT
        is_public: Whether app is visible to all workspace members
        position: Display order position (defaults to end)

    Returns:
        Created app data

    Raises:
        ValueError: If app type is invalid or already exists
    """
    if app_type not in VALID_APP_TYPES:
        raise ValueError(f"Invalid app type. Must be one of: {VALID_APP_TYPES}")

    try:
        supabase = await get_authenticated_async_client(user_jwt)
    except Exception as e:
        logger.exception(f"Failed to get authenticated client: {e}")
        raise ValueError(f"Authentication failed: {str(e)}")

    try:
        # Check if app already exists
        existing = await supabase.table("workspace_apps")\
            .select("id")\
            .eq("workspace_id", workspace_id)\
            .eq("app_type", app_type)\
            .execute()

        if existing and existing.data:
            raise ValueError(f"App type '{app_type}' already exists in this workspace")
    except ValueError:
        raise
    except Exception as e:
        logger.exception(f"Error checking existing app: {e}")
        raise ValueError(f"Failed to check existing apps: {str(e)}")

    try:
        # Get max position if not specified
        if position is None:
            max_pos_result = await supabase.table("workspace_apps")\
                .select("position")\
                .eq("workspace_id", workspace_id)\
                .order("position", desc=True)\
                .limit(1)\
                .execute()
            position = (max_pos_result.data[0]["position"] + 1) if max_pos_result.data else 0
    except Exception as e:
        logger.exception(f"Error getting max position: {e}")
        position = 0  # Default to 0 if we can't get max

    try:
        # Create app
        logger.info(f"Creating app: workspace_id={workspace_id}, app_type={app_type}, is_public={is_public}, position={position}")
        result = await supabase.table("workspace_apps")\
            .insert({
                "workspace_id": workspace_id,
                "app_type": app_type,
                "is_public": is_public,
                "position": position
            })\
            .execute()

        if not result.data:
            raise ValueError("Failed to create app - RLS policy may have blocked the insert")

        logger.info(f"Created {app_type} app in workspace {workspace_id}")
        return result.data[0]

    except ValueError:
        raise
    except Exception as e:
        logger.exception(f"Error inserting app: {e}")
        raise ValueError(f"Failed to create app: {str(e)}")


async def delete_workspace_app(
    workspace_app_id: str,
    user_jwt: str
) -> bool:
    """
    Delete a workspace app.
    RLS ensures only admins/owners can delete apps.

    Args:
        workspace_app_id: App ID
        user_jwt: User's Supabase JWT

    Returns:
        True if deleted successfully

    Raises:
        ValueError: If app not found
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        # Delete app (this will cascade delete app members)
        result = await supabase.table("workspace_apps")\
            .delete()\
            .eq("id", workspace_app_id)\
            .execute()

        if not result.data:
            raise ValueError("App not found or not authorized to delete")

        logger.info(f"Deleted workspace app {workspace_app_id}")
        return True

    except Exception as e:
        logger.exception(f"Error deleting workspace app {workspace_app_id}: {e}")
        raise


async def get_app_members(
    workspace_app_id: str,
    user_jwt: str
) -> List[Dict[str, Any]]:
    """
    Get all members with explicit access to a private app.

    Args:
        workspace_app_id: App ID
        user_jwt: User's Supabase JWT

    Returns:
        List of app members
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        result = await supabase.table("workspace_app_members")\
            .select("id, user_id, added_by, added_at")\
            .eq("workspace_app_id", workspace_app_id)\
            .order("added_at")\
            .execute()

        return result.data or []

    except Exception as e:
        logger.exception(f"Error fetching members for app {workspace_app_id}: {e}")
        raise


async def reorder_workspace_apps(
    workspace_id: str,
    user_jwt: str,
    app_positions: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Reorder workspace apps by updating their positions using RPC function.
    All updates happen atomically in a single database transaction.

    Args:
        workspace_id: Workspace ID
        user_jwt: User's Supabase JWT
        app_positions: List of dicts with 'id' and 'position' keys
                       e.g., [{"id": "uuid1", "position": 0}, {"id": "uuid2", "position": 1}]

    Returns:
        Dict with success message and count of updated apps
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        # Use RPC function for atomic batch update
        # This ensures all position updates happen in a single transaction
        result = await supabase.rpc(
            "reorder_workspace_apps",
            {
                "p_workspace_id": workspace_id,
                "p_app_positions": app_positions
            }
        ).execute()

        updated_count = result.data if result.data else 0

        logger.info(f"Reordered {updated_count} apps in workspace {workspace_id}")

        return {
            "message": "Apps reordered successfully",
            "updated_count": updated_count
        }

    except Exception as e:
        # Fallback to individual updates if RPC function doesn't exist yet
        if "function reorder_workspace_apps" in str(e).lower() or "does not exist" in str(e).lower():
            logger.warning("RPC function not available, falling back to individual updates")
            return await _reorder_apps_fallback(workspace_id, app_positions, supabase)

        logger.exception(f"Error reordering apps in workspace {workspace_id}: {e}")
        raise


async def _reorder_apps_fallback(
    workspace_id: str,
    app_positions: List[Dict[str, Any]],
    supabase: Any
) -> Dict[str, Any]:
    """
    Fallback method for reordering when RPC function is not available.
    Uses concurrent per-app updates (not atomic).
    """
    import asyncio

    async def update_position(app_id: str, position: int) -> bool:
        """Update a single app's position."""
        result = await supabase.table("workspace_apps")\
            .update({"position": position})\
            .eq("id", app_id)\
            .eq("workspace_id", workspace_id)\
            .execute()
        return bool(result.data)

    # Filter valid items and create update tasks
    tasks = []
    for item in app_positions:
        app_id = item.get("id")
        position = item.get("position")
        if app_id is not None and position is not None:
            tasks.append(update_position(app_id, position))

    # Execute all updates in parallel
    if tasks:
        results = await asyncio.gather(*tasks)
        updated_count = sum(1 for r in results if r)
    else:
        updated_count = 0

    return {
        "message": "Apps reordered successfully",
        "updated_count": updated_count
    }
