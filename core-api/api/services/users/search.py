"""
User search operations
Handles searching for users by email.

Uses the public 'users' table which mirrors auth.users data.
"""
from typing import Dict, Any, List, Optional
import logging
from lib.supabase_client import get_async_service_role_client

logger = logging.getLogger(__name__)
_AUTH_USER_PAGE_SIZE = 200


def _mask_email(email: str) -> str:
    """Mask email for logging (PII protection)."""
    if len(email) <= 3:
        return "***"
    return email[:3] + "***"


async def search_users_by_email(
    email_query: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Search for users by email prefix/partial match.

    Args:
        email_query: Email to search for (partial match)
        limit: Max number of results to return

    Returns:
        List of users with id and email (no sensitive data)
    """
    try:
        supabase = await get_async_service_role_client()

        # Escape SQL wildcards to prevent pattern widening
        escaped_query = email_query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

        # Search users table by email using ilike for prefix match
        result = await supabase.table("users")\
            .select("id, email, name")\
            .ilike("email", f"{escaped_query}%")\
            .limit(limit)\
            .execute()

        # Return only safe fields
        users = []
        for user in result.data or []:
            users.append({
                "id": user.get("id"),
                "email": user.get("email"),
                "name": user.get("name"),
            })

        logger.info(f"Found {len(users)} users matching '{_mask_email(email_query)}'")
        return users

    except Exception as e:
        logger.exception(f"Error searching users by email: {e}")
        raise


async def get_user_by_email(
    email: str
) -> Optional[Dict[str, Any]]:
    """
    Get a single user by exact email match.

    Args:
        email: Exact email to look up

    Returns:
        User dict with id and email, or None if not found
    """
    try:
        supabase = await get_async_service_role_client()

        # Query the public users table
        result = await supabase.table("users")\
            .select("id, email, name")\
            .eq("email", email.lower())\
            .limit(1)\
            .execute()

        if not result.data or len(result.data) == 0:
            return None

        user = result.data[0]
        return {
            "id": user.get("id"),
            "email": user.get("email"),
            "name": user.get("name")
        }

    except Exception as e:
        logger.exception(f"Error getting user by email: {e}")
        raise


async def get_users_by_ids(
    user_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    Get multiple users by their IDs.

    Args:
        user_ids: List of user IDs to look up

    Returns:
        Dict mapping user_id to user info (id, email, name, avatar_url)
    """
    if not user_ids:
        return {}

    try:
        supabase = await get_async_service_role_client()

        # Query the public users table
        result = await supabase.table("users")\
            .select("id, email, name, avatar_url")\
            .in_("id", user_ids)\
            .execute()

        user_map = {}
        for user in result.data or []:
            user_id = user.get("id")
            user_map[user_id] = {
                "id": user_id,
                "email": user.get("email"),
                "name": user.get("name"),
                "avatar_url": user.get("avatar_url")
            }

        logger.info(f"Fetched {len(user_map)} users by IDs")
        return user_map

    except Exception as e:
        logger.exception(f"Error getting users by IDs: {e}")
        return {}


async def get_public_profiles_by_ids(
    user_ids: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    Get minimal public profile fields for multiple users.

    This is safe to use for backend enrichment when authenticated clients
    should not have broad direct access to ``public.users``.
    """
    user_map = await get_users_by_ids(user_ids)
    return {
        user_id: {
            "id": user_id,
            "name": user.get("name"),
            "avatar_url": user.get("avatar_url"),
        }
        for user_id, user in user_map.items()
    }


async def attach_public_profiles(
    rows: List[Dict[str, Any]],
    *,
    source_field: str = "user_id",
    target_field: str = "user",
) -> None:
    """Attach minimal public profiles to rows in-place."""
    if not rows:
        return

    user_ids = list({
        row.get(source_field)
        for row in rows
        if row.get(source_field)
    })
    if not user_ids:
        return

    user_map = await get_public_profiles_by_ids(user_ids)
    for row in rows:
        user_id = row.get(source_field)
        if not user_id:
            row[target_field] = None
            continue
        row[target_field] = user_map.get(user_id, {"id": user_id})


async def _find_auth_user_by_email(auth_admin: Any, normalized_email: str) -> Any:
    """Paginate through auth users to find an exact email match."""
    page = 1
    while True:
        users = await auth_admin.list_users(page=page, per_page=_AUTH_USER_PAGE_SIZE)
        if not users:
            return None

        for user in users:
            if (getattr(user, "email", None) or "").strip().lower() == normalized_email:
                return user

        if len(users) < _AUTH_USER_PAGE_SIZE:
            return None
        page += 1


async def get_auth_user_by_email(
    email: str,
) -> Optional[Dict[str, Any]]:
    """
    Resolve an exact email match against the auth user store.

    Falls back to the mirrored ``public.users`` table only when running against
    lightweight unit-test doubles that do not expose the auth admin API.
    """
    normalized_email = email.strip().lower()
    if not normalized_email:
        return None

    try:
        supabase = await get_async_service_role_client()
        auth_admin = getattr(getattr(supabase, "auth", None), "admin", None)
        if auth_admin is None:
            return await get_user_by_email(normalized_email)

        matched_user = await _find_auth_user_by_email(auth_admin, normalized_email)
        if matched_user is None:
            return None

        profile_map = await get_users_by_ids([matched_user.id])
        profile = profile_map.get(matched_user.id, {})
        return {
            "id": matched_user.id,
            "email": getattr(matched_user, "email", None),
            "name": profile.get("name"),
            "avatar_url": profile.get("avatar_url"),
        }
    except Exception as e:
        logger.exception(f"Error getting auth user by email: {e}")
        raise
