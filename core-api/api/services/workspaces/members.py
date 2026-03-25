"""
Workspace member management
Handles adding, removing, and updating workspace members.

Uses async Supabase client for non-blocking I/O.
"""
from typing import Dict, Any, List, Optional
import logging
from lib.supabase_client import get_authenticated_async_client
from api.services.users import get_users_by_ids

logger = logging.getLogger(__name__)


async def _enrich_members_with_user_info(members: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enrich member records with email and name from auth.users.

    Args:
        members: List of member dicts with user_id

    Returns:
        Members with email and name added
    """
    if not members:
        return members

    try:
        # De-duplicate user_ids
        user_ids = list({m.get("user_id") for m in members if m.get("user_id")})

        if not user_ids:
            return members

        # Use the users service to fetch user info
        user_map = await get_users_by_ids(user_ids)

        # Enrich members
        for member in members:
            user_info = user_map.get(member.get("user_id"), {})
            member["email"] = user_info.get("email")
            member["name"] = user_info.get("name")
            member["avatar_url"] = user_info.get("avatar_url")

        return members

    except Exception as e:
        logger.warning(f"Failed to enrich members with user info: {e}")
        # Return members without enrichment on failure
        return members


async def get_workspace_members(
    workspace_id: str,
    user_jwt: str
) -> List[Dict[str, Any]]:
    """
    Get all members of a workspace with their email and name.
    RLS ensures user can only see members if they're also a member.

    Args:
        workspace_id: Workspace ID
        user_jwt: User's Supabase JWT

    Returns:
        List of members with their roles, email, and name
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        result = await supabase.table("workspace_members")\
            .select("id, user_id, role, joined_at")\
            .eq("workspace_id", workspace_id)\
            .order("joined_at")\
            .execute()

        members = result.data or []

        # Enrich with email and name
        members = await _enrich_members_with_user_info(members)

        logger.info(f"Fetched {len(members)} members for workspace {workspace_id}")
        return members

    except Exception as e:
        logger.exception(f"Error fetching members for workspace {workspace_id}: {e}")
        raise


async def add_workspace_member(
    workspace_id: str,
    user_jwt: str,
    member_user_id: str,
    role: str = "member"
) -> Dict[str, Any]:
    """
    Add a user to a workspace.
    RLS ensures only admins/owners can add members.

    Args:
        workspace_id: Workspace ID
        user_jwt: User's Supabase JWT
        member_user_id: ID of user to add
        role: Role to assign ('member' or 'admin')

    Returns:
        Created membership data

    Raises:
        ValueError: If role is invalid or user is already a member
    """
    try:
        if role not in ("member", "admin"):
            raise ValueError("Role must be 'member' or 'admin'")

        supabase = await get_authenticated_async_client(user_jwt)

        # Check if user is already a member
        existing = await supabase.table("workspace_members")\
            .select("id")\
            .eq("workspace_id", workspace_id)\
            .eq("user_id", member_user_id)\
            .limit(1)\
            .execute()

        if existing.data and len(existing.data) > 0:
            raise ValueError("User is already a member of this workspace")

        # Add member
        result = await supabase.table("workspace_members")\
            .insert({
                "workspace_id": workspace_id,
                "user_id": member_user_id,
                "role": role
            })\
            .execute()

        if not result.data:
            raise Exception("Failed to add member - not authorized")

        logger.info(f"Added user {member_user_id} to workspace {workspace_id} as {role}")
        return result.data[0]

    except Exception as e:
        logger.exception(f"Error adding member to workspace {workspace_id}: {e}")
        raise


async def update_member_role(
    workspace_id: str,
    user_jwt: str,
    member_user_id: str,
    new_role: str
) -> Dict[str, Any]:
    """
    Update a member's role in a workspace.
    RLS ensures only admins/owners can update roles.
    Cannot promote to owner (only one owner allowed).

    Args:
        workspace_id: Workspace ID
        user_jwt: User's Supabase JWT
        member_user_id: ID of member to update
        new_role: New role ('member' or 'admin')

    Returns:
        Updated membership data

    Raises:
        ValueError: If role is invalid or trying to change owner
    """
    try:
        if new_role not in ("member", "admin"):
            raise ValueError("Role must be 'member' or 'admin'")

        supabase = await get_authenticated_async_client(user_jwt)

        # Check current role
        existing = await supabase.table("workspace_members")\
            .select("role")\
            .eq("workspace_id", workspace_id)\
            .eq("user_id", member_user_id)\
            .limit(1)\
            .execute()

        if not existing.data or len(existing.data) == 0:
            raise ValueError("Member not found")

        if existing.data[0].get("role") == "owner":
            raise ValueError("Cannot change the owner's role")

        # Update role
        result = await supabase.table("workspace_members")\
            .update({"role": new_role})\
            .eq("workspace_id", workspace_id)\
            .eq("user_id", member_user_id)\
            .execute()

        if not result.data:
            raise ValueError("Failed to update role - not authorized")

        logger.info(f"Updated role for user {member_user_id} in workspace {workspace_id} to {new_role}")
        return result.data[0]

    except Exception as e:
        logger.exception(f"Error updating member role in workspace {workspace_id}: {e}")
        raise


async def remove_workspace_member(
    workspace_id: str,
    user_jwt: str,
    member_user_id: str
) -> bool:
    """
    Remove a member from a workspace.
    RLS ensures only admins/owners can remove members.
    Cannot remove the owner.

    Args:
        workspace_id: Workspace ID
        user_jwt: User's Supabase JWT
        member_user_id: ID of member to remove

    Returns:
        True if removed successfully

    Raises:
        ValueError: If trying to remove owner or member not found
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        # Check if member exists and their role
        existing = await supabase.table("workspace_members")\
            .select("role")\
            .eq("workspace_id", workspace_id)\
            .eq("user_id", member_user_id)\
            .limit(1)\
            .execute()

        if not existing.data or len(existing.data) == 0:
            raise ValueError("Member not found")

        if existing.data[0].get("role") == "owner":
            raise ValueError("Cannot remove the workspace owner")

        # Remove member
        result = await supabase.table("workspace_members")\
            .delete()\
            .eq("workspace_id", workspace_id)\
            .eq("user_id", member_user_id)\
            .execute()

        if not result.data:
            raise ValueError("Failed to remove member - not authorized")

        logger.info(f"Removed user {member_user_id} from workspace {workspace_id}")

        # Clean up empty DM channels with the removed user
        try:
            # Find workspace apps for this workspace
            apps = await supabase.table("workspace_apps")\
                .select("id")\
                .eq("workspace_id", workspace_id)\
                .execute()

            for app in (apps.data or []):
                # Find DM channels where removed user is a participant
                dms = await supabase.table("channels")\
                    .select("id")\
                    .eq("workspace_app_id", app["id"])\
                    .eq("is_dm", True)\
                    .contains("dm_participants", [member_user_id])\
                    .execute()

                for dm in (dms.data or []):
                    # Check if DM has any messages
                    msgs = await supabase.table("channel_messages")\
                        .select("id")\
                        .eq("channel_id", dm["id"])\
                        .limit(1)\
                        .execute()

                    if not msgs.data:
                        await supabase.table("channels")\
                            .delete()\
                            .eq("id", dm["id"])\
                            .execute()
                        logger.info(f"Deleted empty DM channel {dm['id']} with removed user {member_user_id}")
        except Exception as cleanup_err:
            # Don't fail the member removal if DM cleanup fails
            logger.warning(f"Failed to clean up DMs for removed user {member_user_id}: {cleanup_err}")

        return True

    except Exception as e:
        logger.exception(f"Error removing member from workspace {workspace_id}: {e}")
        raise


async def leave_workspace(
    workspace_id: str,
    user_id: str,
    user_jwt: str
) -> bool:
    """
    Allow a user to leave a workspace voluntarily.
    Owners cannot leave — they must transfer ownership or delete the workspace.

    Args:
        workspace_id: Workspace ID
        user_id: ID of the user leaving
        user_jwt: User's Supabase JWT

    Returns:
        True if left successfully

    Raises:
        ValueError: If user is the owner or not a member
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        # Check membership and role
        existing = await supabase.table("workspace_members")\
            .select("role")\
            .eq("workspace_id", workspace_id)\
            .eq("user_id", user_id)\
            .limit(1)\
            .execute()

        if not existing.data or len(existing.data) == 0:
            raise ValueError("You are not a member of this workspace")

        if existing.data[0].get("role") == "owner":
            raise ValueError("Workspace owners cannot leave. Transfer ownership or delete the workspace instead.")

        # Remove self from workspace
        result = await supabase.table("workspace_members")\
            .delete()\
            .eq("workspace_id", workspace_id)\
            .eq("user_id", user_id)\
            .execute()

        if not result.data:
            raise ValueError("Failed to leave workspace")

        logger.info(f"User {user_id} left workspace {workspace_id}")

        # Clean up empty DM channels with the leaving user
        try:
            apps = await supabase.table("workspace_apps")\
                .select("id")\
                .eq("workspace_id", workspace_id)\
                .execute()

            for app in (apps.data or []):
                dms = await supabase.table("channels")\
                    .select("id")\
                    .eq("workspace_app_id", app["id"])\
                    .eq("is_dm", True)\
                    .contains("dm_participants", [user_id])\
                    .execute()

                for dm in (dms.data or []):
                    msgs = await supabase.table("channel_messages")\
                        .select("id")\
                        .eq("channel_id", dm["id"])\
                        .limit(1)\
                        .execute()

                    if not msgs.data:
                        await supabase.table("channels")\
                            .delete()\
                            .eq("id", dm["id"])\
                            .execute()
                        logger.info(f"Deleted empty DM channel {dm['id']} for departing user {user_id}")
        except Exception as cleanup_err:
            logger.warning(f"Failed to clean up DMs for departing user {user_id}: {cleanup_err}")

        return True

    except Exception as e:
        logger.exception(f"Error leaving workspace {workspace_id}: {e}")
        raise


async def get_user_workspace_role(
    workspace_id: str,
    user_id: str,
    user_jwt: str
) -> Optional[str]:
    """
    Get the user's role in a workspace.

    Args:
        workspace_id: Workspace ID
        user_id: User's ID
        user_jwt: User's Supabase JWT

    Returns:
        Role string ('owner', 'admin', 'member') or None if not a member
    """
    try:
        supabase = await get_authenticated_async_client(user_jwt)

        result = await supabase.table("workspace_members")\
            .select("role")\
            .eq("workspace_id", workspace_id)\
            .eq("user_id", user_id)\
            .limit(1)\
            .execute()

        if result.data and len(result.data) > 0:
            return result.data[0].get("role")
        return None

    except Exception as e:
        logger.exception(f"Error getting user role for workspace {workspace_id}: {e}")
        raise
