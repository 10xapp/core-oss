"""Workspace invitation service layer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import secrets
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status

from api.config import settings
from api.services.workspaces.members import get_user_workspace_role
from lib.supabase_client import get_async_service_role_client
from lib.resend_client import send_workspace_invitation_email

logger = logging.getLogger(__name__)

INVITE_EXPIRY_DAYS = 14
INVITE_NOTIFICATION_TYPE = "workspace_invite"
INVITE_NOTIFICATION_RESOURCE_TYPE = "workspace_invitation"
PERSONAL_WORKSPACE_INVITE_ERROR = "Personal workspace cannot be shared"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _parse_db_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None


async def _get_workspace(workspace_id: str) -> Dict[str, Any]:
    client = await get_async_service_role_client()
    result = await client.table("workspaces") \
        .select("id, name, is_default") \
        .eq("id", workspace_id) \
        .maybe_single() \
        .execute()

    if not result.data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workspace not found")
    return result.data


def _assert_workspace_invitable(workspace: Dict[str, Any]) -> None:
    if workspace.get("is_default"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=PERSONAL_WORKSPACE_INVITE_ERROR,
        )


async def _get_user_record(user_id: str) -> Dict[str, Any]:
    client = await get_async_service_role_client()
    result = await client.table("users") \
        .select("id, email, name") \
        .eq("id", user_id) \
        .maybe_single() \
        .execute()

    if not result.data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Authenticated user profile not found",
        )
    return result.data


async def _get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    from api.services.users import get_auth_user_by_email
    return await get_auth_user_by_email(email)


async def _is_workspace_member(workspace_id: str, user_id: str) -> bool:
    client = await get_async_service_role_client()
    result = await client.table("workspace_members") \
        .select("id") \
        .eq("workspace_id", workspace_id) \
        .eq("user_id", user_id) \
        .limit(1) \
        .execute()
    return bool(result.data)


async def _get_pending_invitation_by_workspace_email(
    workspace_id: str,
    normalized_email: str,
) -> Optional[Dict[str, Any]]:
    client = await get_async_service_role_client()
    result = await client.table("workspace_invitations") \
        .select("*") \
        .eq("workspace_id", workspace_id) \
        .eq("email", normalized_email) \
        .eq("status", "pending") \
        .limit(1) \
        .execute()
    return (result.data or [None])[0]


async def _archive_invitation_notifications(
    invitation_id: str,
    recipient_user_id: Optional[str] = None,
) -> None:
    client = await get_async_service_role_client()
    query = client.table("notifications") \
        .update({"archived": True, "read": True, "seen": True}) \
        .eq("resource_type", INVITE_NOTIFICATION_RESOURCE_TYPE) \
        .eq("resource_id", invitation_id)

    if recipient_user_id:
        query = query.eq("user_id", recipient_user_id)

    await query.execute()


async def _revoke_personal_workspace_invitation(
    invitation: Dict[str, Any],
    recipient_user_id: Optional[str] = None,
) -> None:
    """Invalidate legacy personal-workspace invitations and archive notifications."""
    client = await get_async_service_role_client()
    await client.table("workspace_invitations") \
        .update(
            {
                "status": "revoked",
                "revoked_at": _now_iso(),
                "accepted_at": None,
                "accepted_by_user_id": None,
                "declined_at": None,
            }
        ) \
        .eq("id", invitation["id"]) \
        .eq("status", "pending") \
        .execute()

    await _archive_invitation_notifications(
        invitation["id"],
        recipient_user_id=recipient_user_id,
    )


async def _assert_invitation_not_for_personal_workspace(
    invitation: Dict[str, Any],
    recipient_user_id: Optional[str] = None,
) -> None:
    workspace = await _get_workspace(invitation["workspace_id"])
    if workspace.get("is_default"):
        await _revoke_personal_workspace_invitation(
            invitation,
            recipient_user_id=recipient_user_id,
        )
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Invitation is not valid for personal workspace",
        )


def _invitation_notification_matches(
    notification: Optional[Dict[str, Any]],
    desired_notification: Dict[str, Any],
) -> bool:
    if not notification:
        return False

    return (
        notification.get("workspace_id") == desired_notification["workspace_id"]
        and notification.get("title") == desired_notification["title"]
        and notification.get("body") == desired_notification["body"]
        and notification.get("actor_id") == desired_notification["actor_id"]
        and notification.get("data") == desired_notification["data"]
    )


async def _ensure_invitation_notification(
    invitation: Dict[str, Any],
    recipient_user_id: str,
    workspace_name: str,
    inviter_name: str,
) -> None:
    """Create or refresh a notification for a workspace invitation.

    Idempotent on bootstrap: unchanged active rows are left alone so a read
    invite does not become unread again on every app load. If invite details
    change, the active row is refreshed in place and reopened as unread.

    Race-safe: checks for an existing active row first, then inserts when
    missing. The DB unique partial index (uq_notifications_workspace_invite_active)
    prevents duplicates, and duplicate-key insert races are reconciled against
    the row that won.
    """
    client = await get_async_service_role_client()

    title = f"{inviter_name} invited you to join {workspace_name}"
    body = f"Role: {invitation.get('role', 'member')}"
    data = {
        "invitation_id": invitation["id"],
        "workspace_id": invitation["workspace_id"],
        "workspace_name": workspace_name,
        "inviter_name": inviter_name,
        "role": invitation.get("role", "member"),
        "status": invitation.get("status", "pending"),
    }
    desired_notification = {
        "workspace_id": invitation["workspace_id"],
        "type": INVITE_NOTIFICATION_TYPE,
        "title": title,
        "body": body,
        "resource_type": INVITE_NOTIFICATION_RESOURCE_TYPE,
        "resource_id": invitation["id"],
        "actor_id": invitation.get("invited_by_user_id"),
        "data": data,
    }

    existing_result = await client.table("notifications") \
        .select("*") \
        .eq("user_id", recipient_user_id) \
        .eq("type", INVITE_NOTIFICATION_TYPE) \
        .eq("resource_type", INVITE_NOTIFICATION_RESOURCE_TYPE) \
        .eq("resource_id", invitation["id"]) \
        .eq("archived", False) \
        .limit(1) \
        .execute()
    existing_notification = (existing_result.data or [None])[0]

    if existing_notification:
        if _invitation_notification_matches(existing_notification, desired_notification):
            return

        await client.table("notifications") \
            .update({
                **desired_notification,
                "read": False,
                "seen": False,
                "archived": False,
            }) \
            .eq("id", existing_notification["id"]) \
            .execute()
        return

    # No active notification exists — insert a new one.
    # The unique partial index guards against concurrent duplicates.
    try:
        await client.table("notifications") \
            .insert({
                "user_id": recipient_user_id,
                **desired_notification,
            }) \
            .execute()
    except Exception as err:
        if "duplicate key value" not in str(err).lower():
            raise

        raced_result = await client.table("notifications") \
            .select("*") \
            .eq("user_id", recipient_user_id) \
            .eq("type", INVITE_NOTIFICATION_TYPE) \
            .eq("resource_type", INVITE_NOTIFICATION_RESOURCE_TYPE) \
            .eq("resource_id", invitation["id"]) \
            .eq("archived", False) \
            .limit(1) \
            .execute()
        raced_notification = (raced_result.data or [None])[0]
        if not raced_notification:
            return

        if _invitation_notification_matches(raced_notification, desired_notification):
            return

        await client.table("notifications") \
            .update({
                **desired_notification,
                "read": False,
                "seen": False,
                "archived": False,
            }) \
            .eq("id", raced_notification["id"]) \
            .execute()


async def _expire_if_needed(invitation: Dict[str, Any]) -> Dict[str, Any]:
    if invitation.get("status") != "pending":
        return invitation

    expires_at = _parse_db_timestamp(invitation.get("expires_at"))
    if expires_at and expires_at <= datetime.now(timezone.utc):
        client = await get_async_service_role_client()
        result = await client.table("workspace_invitations") \
            .update({"status": "expired"}) \
            .eq("id", invitation["id"]) \
            .eq("status", "pending") \
            .execute()
        if result.data:
            await _archive_invitation_notifications(invitation["id"])
            return result.data[0]

        refreshed = await client.table("workspace_invitations") \
            .select("*") \
            .eq("id", invitation["id"]) \
            .limit(1) \
            .execute()
        return (refreshed.data or [invitation])[0]

    return invitation


async def _expire_pending_invitations(
    *,
    workspace_id: Optional[str] = None,
    normalized_email: Optional[str] = None,
    recipient_user_id: Optional[str] = None,
) -> List[str]:
    """Transition expired pending invitations and archive their notifications."""
    if not workspace_id and not normalized_email:
        return []

    client = await get_async_service_role_client()
    query = client.table("workspace_invitations") \
        .select("id") \
        .eq("status", "pending") \
        .lte("expires_at", _now_iso())

    if workspace_id:
        query = query.eq("workspace_id", workspace_id)
    if normalized_email:
        query = query.eq("email", normalized_email)

    stale = await query.execute()
    stale_ids = [row.get("id") for row in (stale.data or []) if row.get("id")]

    if not stale_ids:
        return []

    # Batch update all stale invitations in one query
    updated = await client.table("workspace_invitations") \
        .update({"status": "expired"}) \
        .in_("id", stale_ids) \
        .eq("status", "pending") \
        .execute()

    expired_ids = [row["id"] for row in (updated.data or []) if row.get("id")]

    for invitation_id in expired_ids:
        await _archive_invitation_notifications(
            invitation_id,
            recipient_user_id=recipient_user_id,
        )

    return expired_ids


async def create_or_refresh_workspace_invitation(
    workspace_id: str,
    invited_email: str,
    role: str,
    inviter_user_id: str,
    inviter_user_jwt: str,
) -> Dict[str, Any]:
    """Create a new invitation or resend/refresh an existing pending one."""
    if role not in ("member", "admin"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role must be 'member' or 'admin'",
        )

    requester_role = await get_user_workspace_role(workspace_id, inviter_user_id, inviter_user_jwt)
    if requester_role not in ("owner", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins and owners can send invitations",
        )

    normalized_email = _normalize_email(invited_email)
    if not normalized_email:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email is required")

    workspace = await _get_workspace(workspace_id)
    _assert_workspace_invitable(workspace)
    inviter = await _get_user_record(inviter_user_id)
    inviter_name = inviter.get("name") or inviter.get("email") or "Someone"

    invited_user = await _get_user_by_email(normalized_email)
    if invited_user and await _is_workspace_member(workspace_id, invited_user["id"]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User is already a workspace member",
        )

    client = await get_async_service_role_client()
    existing_pending = await _get_pending_invitation_by_workspace_email(workspace_id, normalized_email)

    now = datetime.now(timezone.utc)
    expires_at = (now + timedelta(days=INVITE_EXPIRY_DAYS)).isoformat()
    token = secrets.token_urlsafe(32)

    previous_state: Optional[Dict[str, Any]] = None
    created_new = False

    if existing_pending:
        previous_state = {
            "token": existing_pending.get("token"),
            "expires_at": existing_pending.get("expires_at"),
            "role": existing_pending.get("role"),
            "invited_by_user_id": existing_pending.get("invited_by_user_id"),
            "last_email_sent_at": existing_pending.get("last_email_sent_at"),
            "last_email_error": existing_pending.get("last_email_error"),
        }
        update_result = await client.table("workspace_invitations") \
            .update(
                {
                    "email": normalized_email,
                    "role": role,
                    "invited_by_user_id": inviter_user_id,
                    "token": token,
                    "expires_at": expires_at,
                    "last_email_error": None,
                }
            ) \
            .eq("id", existing_pending["id"]) \
            .eq("status", "pending") \
            .execute()

        invitation = (update_result.data or [None])[0]
        if not invitation:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Pending invitation changed, please retry",
            )
    else:
        created_new = True
        insert_result = await client.table("workspace_invitations") \
            .insert(
                {
                    "workspace_id": workspace_id,
                    "email": normalized_email,
                    "role": role,
                    "invited_by_user_id": inviter_user_id,
                    "status": "pending",
                    "token": token,
                    "expires_at": expires_at,
                }
            ) \
            .execute()

        invitation = (insert_result.data or [None])[0]
        if not invitation:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create invitation")

    try:
        await send_workspace_invitation_email(
            recipient_email=normalized_email,
            inviter_name=inviter_name,
            workspace_name=workspace.get("name") or "Workspace",
            role=role,
            token=invitation["token"],
        )
    except Exception as send_error:
        error_text = str(send_error)
        logger.error("Failed to send invitation email for workspace %s: %s", workspace_id, error_text)

        if created_new:
            await client.table("workspace_invitations").delete().eq("id", invitation["id"]).execute()
        elif previous_state:
            await client.table("workspace_invitations") \
                .update(
                    {
                        "token": previous_state["token"],
                        "expires_at": previous_state["expires_at"],
                        "role": previous_state["role"],
                        "invited_by_user_id": previous_state["invited_by_user_id"],
                        "last_email_sent_at": previous_state["last_email_sent_at"],
                        "last_email_error": error_text,
                    }
                ) \
                .eq("id", invitation["id"]) \
                .execute()

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to send invitation email",
        )

    refreshed = await client.table("workspace_invitations") \
        .update({"last_email_sent_at": _now_iso(), "last_email_error": None}) \
        .eq("id", invitation["id"]) \
        .execute()
    invitation = (refreshed.data or [invitation])[0]

    if invited_user:
        await _ensure_invitation_notification(
            invitation=invitation,
            recipient_user_id=invited_user["id"],
            workspace_name=workspace.get("name") or "Workspace",
            inviter_name=inviter_name,
        )

    invitation["workspace_name"] = workspace.get("name")
    invitation["inviter_name"] = inviter_name
    invitation["recipient_user_exists"] = bool(invited_user)
    return invitation


async def list_workspace_invitations(
    workspace_id: str,
    requester_user_id: str,
    requester_user_jwt: str,
) -> List[Dict[str, Any]]:
    """List invitations for a workspace (admin/owner only)."""
    requester_role = await get_user_workspace_role(workspace_id, requester_user_id, requester_user_jwt)
    if requester_role not in ("owner", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins and owners can view invitations",
        )

    workspace = await _get_workspace(workspace_id)
    _assert_workspace_invitable(workspace)

    client = await get_async_service_role_client()

    # Keep status and notification feed accurate for stale pending rows.
    await _expire_pending_invitations(workspace_id=workspace_id)

    result = await client.table("workspace_invitations") \
        .select("*") \
        .eq("workspace_id", workspace_id) \
        .order("created_at", desc=True) \
        .execute()

    return result.data or []


async def _get_invitation_or_404(*, invitation_id: Optional[str] = None, token: Optional[str] = None) -> Dict[str, Any]:
    client = await get_async_service_role_client()
    query = client.table("workspace_invitations").select("*")

    if invitation_id:
        query = query.eq("id", invitation_id)
    elif token:
        query = query.eq("token", token)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invitation lookup key is required")

    result = await query.limit(1).execute()
    rows = result.data if result else []
    if not rows:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invitation not found")

    invitation = await _expire_if_needed(rows[0])
    return invitation


def _assert_pending_or_gone(invitation: Dict[str, Any]) -> None:
    status_value = invitation.get("status")
    if status_value == "pending":
        return
    raise HTTPException(status_code=status.HTTP_410_GONE, detail=f"Invitation is {status_value}")


async def _get_authenticated_user_email(
    user_id: str,
    claimed_email: Optional[str] = None,
) -> str:
    if claimed_email:
        return _normalize_email(claimed_email)

    user = await _get_user_record(user_id)
    return _normalize_email(user["email"])


async def _accept_invitation(
    invitation: Dict[str, Any],
    user_id: str,
    user_email: str,
) -> Dict[str, Any]:
    normalized_user_email = _normalize_email(user_email)
    normalized_invite_email = _normalize_email(invitation.get("email") or "")
    if normalized_user_email != normalized_invite_email:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invitation email does not match authenticated user",
        )

    if invitation.get("status") == "accepted":
        return {
            "invitation": invitation,
            "already_processed": True,
            "membership_created": False,
        }

    _assert_pending_or_gone(invitation)
    await _assert_invitation_not_for_personal_workspace(
        invitation,
        recipient_user_id=user_id,
    )

    client = await get_async_service_role_client()

    membership_created = False
    if not await _is_workspace_member(invitation["workspace_id"], user_id):
        try:
            member_insert = await client.table("workspace_members") \
                .insert(
                    {
                        "workspace_id": invitation["workspace_id"],
                        "user_id": user_id,
                        "role": invitation.get("role", "member"),
                    }
                ) \
                .execute()
            if member_insert.data:
                membership_created = True
        except Exception as insert_error:
            if "duplicate key value" not in str(insert_error).lower():
                raise

    update_result = await client.table("workspace_invitations") \
        .update(
            {
                "status": "accepted",
                "accepted_by_user_id": user_id,
                "accepted_at": _now_iso(),
                "declined_at": None,
                "revoked_at": None,
            }
        ) \
        .eq("id", invitation["id"]) \
        .eq("status", "pending") \
        .execute()

    if update_result.data:
        invitation = update_result.data[0]
    else:
        invitation = await _get_invitation_or_404(invitation_id=invitation["id"])
        if invitation.get("status") != "accepted":
            _assert_pending_or_gone(invitation)

    await _archive_invitation_notifications(invitation["id"], recipient_user_id=user_id)

    return {
        "invitation": invitation,
        "already_processed": not membership_created,
        "membership_created": membership_created,
    }


async def accept_workspace_invitation(
    invitation_id: str,
    user_id: str,
    user_email: Optional[str] = None,
) -> Dict[str, Any]:
    """Accept invitation by invitation id."""
    invitation = await _get_invitation_or_404(invitation_id=invitation_id)
    authenticated_email = await _get_authenticated_user_email(user_id, user_email)
    return await _accept_invitation(invitation, user_id=user_id, user_email=authenticated_email)


async def accept_workspace_invitation_by_token(
    token: str,
    user_id: str,
    user_email: Optional[str] = None,
) -> Dict[str, Any]:
    """Accept invitation by token."""
    invitation = await _get_invitation_or_404(token=token)
    authenticated_email = await _get_authenticated_user_email(user_id, user_email)
    return await _accept_invitation(invitation, user_id=user_id, user_email=authenticated_email)


async def decline_workspace_invitation(
    invitation_id: str,
    user_id: str,
    user_email: Optional[str] = None,
) -> Dict[str, Any]:
    """Decline an invitation by id (recipient only)."""
    invitation = await _get_invitation_or_404(invitation_id=invitation_id)
    normalized_user_email = await _get_authenticated_user_email(user_id, user_email)
    normalized_invite_email = _normalize_email(invitation.get("email") or "")
    if normalized_user_email != normalized_invite_email:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invitation email does not match authenticated user",
        )

    if invitation.get("status") == "declined":
        return {"invitation": invitation, "already_processed": True}

    _assert_pending_or_gone(invitation)
    await _assert_invitation_not_for_personal_workspace(
        invitation,
        recipient_user_id=user_id,
    )

    client = await get_async_service_role_client()
    result = await client.table("workspace_invitations") \
        .update(
            {
                "status": "declined",
                "declined_at": _now_iso(),
                "accepted_at": None,
                "accepted_by_user_id": None,
            }
        ) \
        .eq("id", invitation_id) \
        .eq("status", "pending") \
        .execute()

    if not result.data:
        invitation = await _get_invitation_or_404(invitation_id=invitation_id)
    else:
        invitation = result.data[0]

    await _archive_invitation_notifications(invitation_id, recipient_user_id=user_id)
    return {"invitation": invitation, "already_processed": False}


async def revoke_workspace_invitation(
    invitation_id: str,
    actor_user_id: str,
    actor_user_jwt: str,
) -> Dict[str, Any]:
    """Revoke pending invitation (admin/owner only)."""
    invitation = await _get_invitation_or_404(invitation_id=invitation_id)

    actor_role = await get_user_workspace_role(
        invitation["workspace_id"],
        actor_user_id,
        actor_user_jwt,
    )
    if actor_role not in ("owner", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins and owners can revoke invitations",
        )

    workspace = await _get_workspace(invitation["workspace_id"])
    _assert_workspace_invitable(workspace)

    if invitation.get("status") == "revoked":
        return {"invitation": invitation, "already_processed": True}

    _assert_pending_or_gone(invitation)

    client = await get_async_service_role_client()
    result = await client.table("workspace_invitations") \
        .update(
            {
                "status": "revoked",
                "revoked_at": _now_iso(),
                "accepted_at": None,
                "accepted_by_user_id": None,
                "declined_at": None,
            }
        ) \
        .eq("id", invitation_id) \
        .eq("status", "pending") \
        .execute()

    if not result.data:
        invitation = await _get_invitation_or_404(invitation_id=invitation_id)
    else:
        invitation = result.data[0]

    await _archive_invitation_notifications(invitation_id)
    return {"invitation": invitation, "already_processed": False}


async def get_workspace_invitation_share_link(
    invitation_id: str,
    requester_user_id: str,
    requester_user_jwt: str,
) -> Dict[str, Any]:
    """Return an admin-only sharable invite link for a pending invitation."""
    invitation = await _get_invitation_or_404(invitation_id=invitation_id)

    requester_role = await get_user_workspace_role(
        invitation["workspace_id"],
        requester_user_id,
        requester_user_jwt,
    )
    if requester_role not in ("owner", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins and owners can share invitations",
        )

    workspace = await _get_workspace(invitation["workspace_id"])
    _assert_workspace_invitable(workspace)

    _assert_pending_or_gone(invitation)

    frontend_base = settings.frontend_url.rstrip("/")
    if not frontend_base:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Frontend URL is not configured",
        )

    return {
        "invitation_id": invitation["id"],
        "invite_url": f"{frontend_base}/invite/{invitation['token']}",
        "expires_at": invitation.get("expires_at"),
    }


async def resolve_post_signup_pending_invitations(
    user_id: str,
    user_email: Optional[str] = None,
) -> Dict[str, Any]:
    """Find pending invitations for the authenticated user and ensure notifications."""
    normalized_email = await _get_authenticated_user_email(user_id, user_email)

    client = await get_async_service_role_client()

    await _expire_pending_invitations(
        normalized_email=normalized_email,
        recipient_user_id=user_id,
    )

    pending_result = await client.table("workspace_invitations") \
        .select("*") \
        .eq("status", "pending") \
        .eq("email", normalized_email) \
        .gt("expires_at", _now_iso()) \
        .order("created_at", desc=True) \
        .execute()

    pending = pending_result.data or []
    valid_pending: List[Dict[str, Any]] = []

    workspace_name_cache: Dict[str, str] = {}
    inviter_name_cache: Dict[str, str] = {}

    for invitation in pending:
        workspace_id = invitation["workspace_id"]
        invited_by = invitation.get("invited_by_user_id")

        if workspace_id not in workspace_name_cache:
            workspace = await _get_workspace(workspace_id)
            if workspace.get("is_default"):
                await _revoke_personal_workspace_invitation(
                    invitation,
                    recipient_user_id=user_id,
                )
                continue

            workspace_name_cache[workspace_id] = workspace.get("name") or "Workspace"

        if invited_by:
            if invited_by not in inviter_name_cache:
                try:
                    inviter = await _get_user_record(invited_by)
                    inviter_name_cache[invited_by] = inviter.get("name") or inviter.get("email") or "Someone"
                except HTTPException:
                    inviter_name_cache[invited_by] = "Someone"
            inviter_name = inviter_name_cache[invited_by]
        else:
            inviter_name = "Someone"

        await _ensure_invitation_notification(
            invitation=invitation,
            recipient_user_id=user_id,
            workspace_name=workspace_name_cache[workspace_id],
            inviter_name=inviter_name,
        )
        valid_pending.append(invitation)

    return {
        "pending_invitations": valid_pending,
        "count": len(valid_pending),
    }
