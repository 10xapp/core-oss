"""Channel management service for workspace messaging."""

from typing import Dict, Any, List, Optional
import logging
from postgrest.exceptions import APIError
from lib.supabase_client import get_authenticated_async_client

logger = logging.getLogger(__name__)


# =============================================================================
# UNREAD INDICATORS
# =============================================================================


async def get_unread_counts(
    workspace_app_id: str,
    user_jwt: str,
) -> Dict[str, int]:
    """
    Get unread message counts for all channels in a workspace.

    Args:
        workspace_app_id: The workspace app ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        Dict mapping channel_id to unread count
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        # Call the RPC function we created in the migration
        result = await supabase.rpc(
            "get_workspace_unread_counts",
            {"p_workspace_app_id": workspace_app_id}
        ).execute()

        # Convert to dict
        counts = {}
        for row in (result.data or []):
            counts[row["channel_id"]] = row["unread_count"]

        logger.info(f"Got unread counts for workspace app {workspace_app_id}: {len(counts)} channels")
        return counts

    except Exception as e:
        logger.error(f"Error getting unread counts: {e}")
        raise


async def mark_channel_read(
    channel_id: str,
    user_jwt: str,
) -> bool:
    """
    Mark a channel as read for the current user.

    Args:
        channel_id: The channel ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        True if successful
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        # Call the RPC function (uses auth.uid() internally)
        await supabase.rpc(
            "mark_channel_read",
            {"p_channel_id": channel_id}
        ).execute()

        logger.info(f"Marked channel {channel_id} as read")
        return True

    except Exception as e:
        logger.error(f"Error marking channel read: {e}")
        raise


async def get_channels(
    workspace_app_id: str,
    user_jwt: str,
) -> List[Dict[str, Any]]:
    """
    Get all channels in a workspace app.

    RLS automatically filters private channels to those the user is a member of.

    Args:
        workspace_app_id: The workspace app ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        List of channels
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        # RLS will automatically filter to accessible channels
        # Exclude DMs - they're fetched separately via get_user_dms
        result = await (
            supabase.table("channels")
            .select("*, created_by_user:users!channels_created_by_public_users_fkey(id, email, name, avatar_url)")
            .eq("workspace_app_id", workspace_app_id)
            .eq("is_dm", False)
            .order("created_at")
            .execute()
        )

        channels = result.data or []
        logger.info(f"Retrieved {len(channels)} channels for workspace app {workspace_app_id}")
        return channels

    except Exception as e:
        logger.error(f"Error getting channels: {e}")
        raise


async def get_channel(
    channel_id: str,
    user_jwt: str,
) -> Optional[Dict[str, Any]]:
    """
    Get a single channel by ID.

    Args:
        channel_id: The channel ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        Channel data or None if not found
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        result = await (
            supabase.table("channels")
            .select("*, created_by_user:users!channels_created_by_public_users_fkey(id, email, name, avatar_url)")
            .eq("id", channel_id)
            .limit(1)
            .execute()
        )

        if result.data and len(result.data) > 0:
            return result.data[0]
        return None

    except Exception as e:
        logger.error(f"Error getting channel {channel_id}: {e}")
        raise


async def create_channel(
    workspace_app_id: str,
    user_id: str,
    user_jwt: str,
    name: str,
    description: Optional[str] = None,
    is_private: bool = False,
) -> Dict[str, Any]:
    """
    Create a new channel.

    Args:
        workspace_app_id: The workspace app ID
        user_id: User creating the channel
        user_jwt: User's JWT for authenticated requests
        name: Channel name (will be lowercased, spaces replaced with hyphens)
        description: Optional channel description
        is_private: Whether the channel is private

    Returns:
        Created channel data
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Normalize channel name (lowercase, hyphens instead of spaces)
    normalized_name = name.lower().strip().replace(" ", "-")

    try:
        result = await (
            supabase.table("channels")
            .insert({
                "workspace_app_id": workspace_app_id,
                "name": normalized_name,
                "description": description,
                "is_private": is_private,
                "created_by": user_id,
            })
            .execute()
        )

        if result.data and len(result.data) > 0:
            channel = result.data[0]
            logger.info(f"Created channel '{normalized_name}' ({channel['id']}) in workspace app {workspace_app_id}")

            # Safety net: explicitly add the creator to channel_members for private channels.
            # A DB trigger also does this, but we add it here in case the trigger fails silently.
            # Both use ON CONFLICT DO NOTHING, so double-insertion is safe.
            if is_private:
                try:
                    await (
                        supabase.table("channel_members")
                        .insert({
                            "channel_id": channel["id"],
                            "user_id": user_id,
                            "role": "owner",
                        })
                        .execute()
                    )
                except APIError as e:
                    # 409/duplicate means the trigger already inserted — safe to ignore
                    if "duplicate" in str(e).lower() or "conflict" in str(e).lower():
                        logger.debug(f"Creator already in channel_members (trigger handled it): {channel['id']}")
                    else:
                        logger.warning(f"Failed to add creator to channel_members for {channel['id']}: {e}")

            return channel

        raise Exception("Failed to create channel")

    except Exception as e:
        logger.error(f"Error creating channel: {e}")
        raise


async def update_channel(
    channel_id: str,
    user_jwt: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update a channel.

    Args:
        channel_id: The channel ID
        user_jwt: User's JWT for authenticated requests
        name: New channel name (optional)
        description: New description (optional)

    Returns:
        Updated channel data
    """
    supabase = await get_authenticated_async_client(user_jwt)

    update_data = {"updated_at": "now()"}

    if name is not None:
        update_data["name"] = name.lower().strip().replace(" ", "-")
    if description is not None:
        update_data["description"] = description

    try:
        result = await (
            supabase.table("channels")
            .update(update_data)
            .eq("id", channel_id)
            .execute()
        )

        if result.data and len(result.data) > 0:
            logger.info(f"Updated channel {channel_id}")
            return result.data[0]

        raise Exception("Channel not found or no permission")

    except Exception as e:
        logger.error(f"Error updating channel {channel_id}: {e}")
        raise


async def delete_channel(
    channel_id: str,
    user_jwt: str,
) -> bool:
    """
    Delete a channel.

    Args:
        channel_id: The channel ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        True if successful
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        await (
            supabase.table("channels")
            .delete()
            .eq("id", channel_id)
            .execute()
        )

        logger.info(f"Deleted channel {channel_id}")
        return True

    except Exception as e:
        logger.error(f"Error deleting channel {channel_id}: {e}")
        raise


async def get_channel_members(
    channel_id: str,
    user_jwt: str,
) -> List[Dict[str, Any]]:
    """
    Get members of a channel (handles public, private, and DMs).

    Args:
        channel_id: The channel ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        List of channel members with user info
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        # Use RPC that validates access and returns members with minimal profiles
        result = await (
            supabase
            .rpc(
                "get_channel_members_with_profiles",
                {"p_channel_id": channel_id}
            )
            .execute()
        )
        return result.data or []

    except APIError as e:
        # supabase-python raises APIError on RPC failures (e.g., access denied)
        logger.error(f"RPC error getting channel members: {e.message}")
        raise
    except Exception as e:
        logger.error(f"Error getting channel members: {e}")
        raise


async def add_channel_member(
    channel_id: str,
    member_user_id: str,
    user_jwt: str,
    role: str = "member",
) -> Dict[str, Any]:
    """
    Add a member to a private channel.

    Args:
        channel_id: The channel ID
        member_user_id: User ID to add
        user_jwt: User's JWT for authenticated requests
        role: Member role (owner, moderator, member)

    Returns:
        Created membership data
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        # Validate that the channel exists, is private, and is not a DM
        channel_result = await (
            supabase.table("channels")
            .select("is_private, is_dm")
            .eq("id", channel_id)
            .limit(1)
            .execute()
        )
        if not channel_result.data:
            raise ValueError("Channel not found or not accessible")
        ch = channel_result.data[0]
        if ch.get("is_dm"):
            raise ValueError("Cannot add members to DM channels — use DM endpoints")
        if not ch.get("is_private"):
            raise ValueError("Can only add members to private channels")

        result = await (
            supabase.table("channel_members")
            .insert({
                "channel_id": channel_id,
                "user_id": member_user_id,
                "role": role,
            })
            .execute()
        )

        if result.data and len(result.data) > 0:
            logger.info(f"Added user {member_user_id} to channel {channel_id}")
            return result.data[0]

        raise Exception("Failed to add member")

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"Error adding channel member: {e}")
        raise


async def remove_channel_member(
    channel_id: str,
    member_user_id: str,
    user_jwt: str,
) -> bool:
    """
    Remove a member from a private channel.

    Args:
        channel_id: The channel ID
        member_user_id: User ID to remove
        user_jwt: User's JWT for authenticated requests

    Returns:
        True if successful
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        await (
            supabase.table("channel_members")
            .delete()
            .eq("channel_id", channel_id)
            .eq("user_id", member_user_id)
            .execute()
        )

        logger.info(f"Removed user {member_user_id} from channel {channel_id}")
        return True

    except Exception as e:
        logger.error(f"Error removing channel member: {e}")
        raise


# =============================================================================
# DIRECT MESSAGES (DMs)
# =============================================================================


async def get_or_create_dm(
    workspace_app_id: str,
    user_id: str,
    user_jwt: str,
    participant_ids: List[str],
) -> Dict[str, Any]:
    """
    Get or create a DM channel between participants.

    Args:
        workspace_app_id: The workspace app ID
        user_id: Current user ID (must be in participant_ids)
        user_jwt: User's JWT for authenticated requests
        participant_ids: List of user IDs for the DM (including current user)

    Returns:
        DM channel data
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Ensure current user is in participants
    if user_id not in participant_ids:
        participant_ids = [user_id] + participant_ids

    # Sort for consistent lookup
    sorted_participants = sorted(participant_ids)

    try:
        # Call the RPC function we created in the migration
        result = await supabase.rpc(
            "get_or_create_dm",
            {
                "p_workspace_app_id": workspace_app_id,
                "p_participant_ids": sorted_participants,
            }
        ).execute()

        if result.data:
            channel_id = result.data
            # Fetch the full channel with participant info
            channel = await get_dm_channel(channel_id, user_jwt)
            if channel:
                logger.info(f"Got/created DM channel {channel_id}")
                return channel

        raise Exception("Failed to get or create DM")

    except Exception as e:
        logger.error(f"Error getting/creating DM: {e}")
        raise


async def get_dm_channel(
    channel_id: str,
    user_jwt: str,
) -> Optional[Dict[str, Any]]:
    """
    Get a DM channel with participant info.

    Args:
        channel_id: The channel ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        DM channel data with participants
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        result = await (
            supabase.table("channels")
            .select("*")
            .eq("id", channel_id)
            .eq("is_dm", True)
            .limit(1)
            .execute()
        )

        if result.data and len(result.data) > 0:
            channel = result.data[0]

            # Fetch participant user info
            if channel.get("dm_participants"):
                users_result = await (
                    supabase.table("users")
                    .select("id, email, name, avatar_url")
                    .in_("id", channel["dm_participants"])
                    .execute()
                )
                channel["participants"] = users_result.data or []

            return channel
        return None

    except Exception as e:
        logger.error(f"Error getting DM channel {channel_id}: {e}")
        raise


async def get_user_dms(
    workspace_app_id: str,
    user_id: str,
    user_jwt: str,
) -> List[Dict[str, Any]]:
    """
    Get all DM channels for a user in a workspace.

    Args:
        workspace_app_id: The workspace app ID
        user_id: The user ID
        user_jwt: User's JWT for authenticated requests

    Returns:
        List of DM channels with participant info
    """
    supabase = await get_authenticated_async_client(user_jwt)

    try:
        # Get DMs where user is a participant
        result = await (
            supabase.table("channels")
            .select("*")
            .eq("workspace_app_id", workspace_app_id)
            .eq("is_dm", True)
            .contains("dm_participants", [user_id])
            .order("updated_at", desc=True)
            .execute()
        )

        dms = result.data or []

        if not dms:
            return dms

        # Get workspace_id from workspace_app to check membership
        app_result = await (
            supabase.table("workspace_apps")
            .select("workspace_id")
            .eq("id", workspace_app_id)
            .limit(1)
            .execute()
        )
        workspace_id = (app_result.data[0]["workspace_id"]) if app_result.data else None

        # Get current workspace member IDs to filter out DMs with departed members
        active_member_ids: set = set()
        if workspace_id:
            members_result = await (
                supabase.table("workspace_members")
                .select("user_id")
                .eq("workspace_id", workspace_id)
                .execute()
            )
            active_member_ids = {m["user_id"] for m in (members_result.data or [])}

        # Filter DMs: only keep those where ALL other participants are still workspace members
        if active_member_ids:
            filtered_dms = []
            for dm in dms:
                other_participants = [
                    pid for pid in (dm.get("dm_participants") or [])
                    if pid != user_id
                ]
                # Keep DM if all other participants are still active members
                if all(pid in active_member_ids for pid in other_participants):
                    filtered_dms.append(dm)
            dms = filtered_dms

        # Fetch participant info for all DMs
        all_participant_ids = set()
        for dm in dms:
            if dm.get("dm_participants"):
                all_participant_ids.update(dm["dm_participants"])

        if all_participant_ids:
            users_result = await (
                supabase.table("users")
                .select("id, email, name, avatar_url")
                .in_("id", list(all_participant_ids))
                .execute()
            )
            users_by_id = {u["id"]: u for u in (users_result.data or [])}

            # Attach participant info to each DM
            for dm in dms:
                dm["participants"] = [
                    users_by_id.get(pid, {"id": pid})
                    for pid in (dm.get("dm_participants") or [])
                ]

        logger.info(f"Retrieved {len(dms)} DMs for user {user_id}")
        return dms

    except Exception as e:
        logger.error(f"Error getting user DMs: {e}")
        raise
