"""
Comment service - CRUD operations for project issue comments.

Provides GitHub-style flat, chronological comments on issues with reactions.
"""
from typing import Dict, Any, List, cast
import logging
from lib.supabase_client import get_authenticated_async_client
from api.services.notifications.subscriptions import subscribe
from api.services.notifications.create import notify_subscribers, NotificationType
from api.services.notifications.helpers import get_actor_info
from api.services.users import attach_public_profiles

logger = logging.getLogger(__name__)


def extract_plain_text(blocks: List[Dict[str, Any]]) -> str:
    """
    Extract plain text content from blocks for search indexing.

    Args:
        blocks: Array of content blocks

    Returns:
        Plain text string
    """
    text_parts = []

    for block in blocks:
        block_type = block.get("type")
        data = block.get("data", {})

        if block_type == "text":
            text_parts.append(data.get("content", ""))
        elif block_type == "mention":
            text_parts.append(f"@{data.get('display_name', '')}")
        elif block_type == "code":
            text_parts.append(data.get("content", ""))
        elif block_type == "quote":
            text_parts.append(data.get("preview", ""))

    return " ".join(text_parts).strip()


async def get_comments(
    user_jwt: str,
    issue_id: str,
    limit: int = 100,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    Get comments for an issue, ordered chronologically.

    Args:
        user_jwt: User's Supabase JWT
        issue_id: Issue UUID
        limit: Max comments to return
        offset: Pagination offset

    Returns:
        Dict with comments list, page_count, and total_count
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Get total count first
    count_result = await supabase.table("project_issue_comments")\
        .select("id", count="exact")\
        .eq("issue_id", issue_id)\
        .execute()
    total_count = count_result.count or 0

    # Get comments with user info
    result = await supabase.table("project_issue_comments")\
        .select("*")\
        .eq("issue_id", issue_id)\
        .order("created_at")\
        .range(offset, offset + limit - 1)\
        .execute()

    comments = result.data or []
    await attach_public_profiles(comments)

    # Get reactions for all comments in one query
    if comments:
        comment_ids = [c["id"] for c in comments]
        reactions_result = await supabase.table("project_comment_reactions")\
            .select("*")\
            .in_("comment_id", comment_ids)\
            .execute()

        # Build reaction lookup by comment_id
        reactions_by_comment: Dict[str, List[Dict[str, Any]]] = {}
        for reaction in (reactions_result.data or []):
            comment_id = reaction["comment_id"]
            reactions_by_comment.setdefault(comment_id, []).append(reaction)

        # Attach reactions to comments
        for comment in comments:
            comment["reactions"] = reactions_by_comment.get(comment["id"], [])

    return {
        "comments": comments,
        "count": len(comments),  # Page count
        "total_count": total_count,  # Total comments for this issue
    }


async def create_comment(
    user_id: str,
    user_jwt: str,
    issue_id: str,
    blocks: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Create a new comment on an issue.

    Args:
        user_id: Current user's UUID
        user_jwt: User's Supabase JWT
        issue_id: Issue UUID
        blocks: Content blocks

    Returns:
        Created comment dict with user info
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Get issue to obtain workspace_app_id, workspace_id, and title
    issue_result = await supabase.table("project_issues")\
        .select("workspace_app_id, workspace_id, title, board_id")\
        .eq("id", issue_id)\
        .single()\
        .execute()

    issue = issue_result.data
    if not issue:
        raise ValueError(f"Issue not found: {issue_id}")

    # Extract plain text for search
    content = extract_plain_text(blocks)

    # Create comment
    insert_data = {
        "workspace_app_id": issue["workspace_app_id"],
        "workspace_id": issue["workspace_id"],
        "issue_id": issue_id,
        "user_id": user_id,
        "content": content,
        "blocks": blocks,
    }

    result = await supabase.table("project_issue_comments")\
        .insert(insert_data)\
        .execute()

    comment = result.data[0] if result.data else None
    if not comment:
        raise ValueError("Failed to create comment")

    # Fetch with user info
    full_result = await supabase.table("project_issue_comments")\
        .select("*")\
        .eq("id", comment["id"])\
        .single()\
        .execute()

    comment = cast(Dict[str, Any], full_result.data)
    await attach_public_profiles([comment])
    comment["reactions"] = []

    # Auto-subscribe commenter and notify subscribers
    try:
        await subscribe(user_id=user_id, resource_type="issue", resource_id=issue_id, reason="commenter")

        actor = await get_actor_info(user_id)
        comment_preview = (content[:100] + "...") if len(content) > 100 else content
        await notify_subscribers(
            resource_type="issue",
            resource_id=issue_id,
            type=NotificationType.COMMENT_ADDED,
            title=f"{actor['actor_name']} commented on: {issue['title']}",
            body=comment_preview if comment_preview else None,
            actor_id=user_id,
            workspace_id=issue["workspace_id"],
            data={
                "board_id": issue.get("board_id"),
                "issue_title": issue["title"],
                "comment_preview": comment_preview,
                **actor,
            },
        )
    except Exception as e:
        logger.warning(f"Notification failed for comment create: {e}")

    return comment


async def update_comment(
    user_jwt: str,
    comment_id: str,
    blocks: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Update a comment's content (author only via RLS).

    Args:
        user_jwt: User's Supabase JWT
        comment_id: Comment UUID
        blocks: New content blocks

    Returns:
        Updated comment dict
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Extract plain text for search
    content = extract_plain_text(blocks)

    # Update comment and fetch with user info in one call
    result = await supabase.table("project_issue_comments")\
        .update({
            "content": content,
            "blocks": blocks,
            "is_edited": True,
            "edited_at": "now()",
        })\
        .eq("id", comment_id)\
        .select("*")\
        .single()\
        .execute()

    if not result.data:
        raise ValueError(f"Comment not found or unauthorized: {comment_id}")

    comment = result.data
    await attach_public_profiles([comment])

    # Get reactions (separate query - can't be combined with update)
    reactions_result = await supabase.table("project_comment_reactions")\
        .select("*")\
        .eq("comment_id", comment_id)\
        .execute()

    comment["reactions"] = reactions_result.data or []

    return comment


async def delete_comment(
    user_jwt: str,
    comment_id: str,
) -> Dict[str, Any]:
    """
    Delete a comment (author or admin only via RLS).

    Args:
        user_jwt: User's Supabase JWT
        comment_id: Comment UUID

    Returns:
        Status dict

    Raises:
        ValueError: If comment not found or unauthorized
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Delete directly and check result - avoids pre-check blocking admin deletes
    # when admin has DELETE permission but not SELECT permission
    result = await supabase.table("project_issue_comments")\
        .delete()\
        .eq("id", comment_id)\
        .execute()

    # If nothing was deleted, the comment didn't exist or user wasn't authorized
    if not result.data:
        raise ValueError(f"Comment not found or unauthorized: {comment_id}")

    return {"status": "deleted"}


async def add_reaction(
    user_id: str,
    user_jwt: str,
    comment_id: str,
    emoji: str,
) -> Dict[str, Any]:
    """
    Add a reaction to a comment.

    Args:
        user_id: Current user's UUID
        user_jwt: User's Supabase JWT
        comment_id: Comment UUID
        emoji: Emoji string

    Returns:
        Created reaction dict
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Get comment to obtain workspace_app_id and workspace_id
    comment_result = await supabase.table("project_issue_comments")\
        .select("workspace_app_id, workspace_id")\
        .eq("id", comment_id)\
        .single()\
        .execute()

    comment = comment_result.data
    if not comment:
        raise ValueError(f"Comment not found: {comment_id}")

    # Create reaction
    insert_data = {
        "workspace_app_id": comment["workspace_app_id"],
        "workspace_id": comment["workspace_id"],
        "comment_id": comment_id,
        "user_id": user_id,
        "emoji": emoji,
    }

    result = await supabase.table("project_comment_reactions")\
        .insert(insert_data)\
        .execute()

    return result.data[0] if result.data else {}


async def remove_reaction(
    user_id: str,
    user_jwt: str,
    comment_id: str,
    emoji: str,
) -> bool:
    """
    Remove a reaction from a comment (own reaction only).

    Args:
        user_id: Current user's UUID
        user_jwt: User's Supabase JWT
        comment_id: Comment UUID
        emoji: Emoji string to remove

    Returns:
        True if successful
    """
    supabase = await get_authenticated_async_client(user_jwt)

    # Explicit user_id filter for defense-in-depth (RLS also enforces this)
    await supabase.table("project_comment_reactions")\
        .delete()\
        .eq("comment_id", comment_id)\
        .eq("emoji", emoji)\
        .eq("user_id", user_id)\
        .execute()

    return True
