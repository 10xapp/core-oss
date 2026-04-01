"""Workspace invitation router."""

from __future__ import annotations

from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field

from api.dependencies import get_current_user_email, get_current_user_id, get_current_user_jwt
from api.exceptions import handle_api_exception
from api.services.workspaces.invitations import (
    create_or_refresh_workspace_invitation,
    list_workspace_invitations,
    accept_workspace_invitation,
    accept_workspace_invitation_by_token,
    decline_workspace_invitation,
    revoke_workspace_invitation,
    get_workspace_invitation_share_link,
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/workspaces", tags=["workspace invitations"])


class CreateInvitationRequest(BaseModel):
    email: EmailStr
    role: str = Field(default="member", description="member|admin")


class InvitationItemResponse(BaseModel):
    id: str
    workspace_id: str
    email: str
    role: str
    status: str
    token: str
    expires_at: str
    invited_by_user_id: Optional[str] = None
    accepted_by_user_id: Optional[str] = None
    accepted_at: Optional[str] = None
    declined_at: Optional[str] = None
    revoked_at: Optional[str] = None
    last_email_sent_at: Optional[str] = None
    last_email_error: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        extra = "allow"


class InvitationSingleResponse(BaseModel):
    invitation: InvitationItemResponse


class InvitationListResponse(BaseModel):
    invitations: List[InvitationItemResponse]
    count: int


class InvitationActionResponse(BaseModel):
    invitation: InvitationItemResponse
    already_processed: bool = False
    membership_created: Optional[bool] = None


class InvitationShareLinkResponse(BaseModel):
    invitation_id: str
    invite_url: str
    expires_at: Optional[str] = None


class AcceptByTokenRequest(BaseModel):
    token: str


@router.post("/{workspace_id}/invitations", response_model=InvitationSingleResponse, status_code=status.HTTP_201_CREATED)
async def create_invitation_endpoint(
    workspace_id: str,
    request: CreateInvitationRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Create or resend/refresh a workspace invitation."""
    try:
        invitation = await create_or_refresh_workspace_invitation(
            workspace_id=workspace_id,
            invited_email=request.email,
            role=request.role,
            inviter_user_id=user_id,
            inviter_user_jwt=user_jwt,
        )
        return {"invitation": invitation}
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to create invitation", logger)


@router.get("/{workspace_id}/invitations", response_model=InvitationListResponse)
async def list_invitations_endpoint(
    workspace_id: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List workspace invitations (admin/owner only)."""
    try:
        invitations = await list_workspace_invitations(
            workspace_id=workspace_id,
            requester_user_id=user_id,
            requester_user_jwt=user_jwt,
        )
        return {"invitations": invitations, "count": len(invitations)}
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to list invitations", logger)


@router.post("/invitations/{invitation_id}/accept", response_model=InvitationActionResponse)
async def accept_invitation_endpoint(
    invitation_id: str,
    user_id: Annotated[str, Depends(get_current_user_id)],
    user_email: Annotated[str, Depends(get_current_user_email)],
):
    """Accept an invitation by id."""
    try:
        result = await accept_workspace_invitation(
            invitation_id=invitation_id,
            user_id=user_id,
            user_email=user_email,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to accept invitation", logger)


@router.post("/invitations/{invitation_id}/decline", response_model=InvitationActionResponse)
async def decline_invitation_endpoint(
    invitation_id: str,
    user_id: Annotated[str, Depends(get_current_user_id)],
    user_email: Annotated[str, Depends(get_current_user_email)],
):
    """Decline an invitation by id."""
    try:
        result = await decline_workspace_invitation(
            invitation_id=invitation_id,
            user_id=user_id,
            user_email=user_email,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to decline invitation", logger)


@router.post("/invitations/{invitation_id}/revoke", response_model=InvitationActionResponse)
async def revoke_invitation_endpoint(
    invitation_id: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Revoke a pending invitation (admin/owner only)."""
    try:
        result = await revoke_workspace_invitation(
            invitation_id=invitation_id,
            actor_user_id=user_id,
            actor_user_jwt=user_jwt,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to revoke invitation", logger)


@router.get("/invitations/{invitation_id}/share-link", response_model=InvitationShareLinkResponse)
async def get_invitation_share_link_endpoint(
    invitation_id: str,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Return a sharable invite URL for a pending invitation (admin/owner only)."""
    try:
        return await get_workspace_invitation_share_link(
            invitation_id=invitation_id,
            requester_user_id=user_id,
            requester_user_jwt=user_jwt,
        )
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to generate invite share link", logger)


@router.post("/invitations/accept-by-token", response_model=InvitationActionResponse)
async def accept_by_token_endpoint(
    request: AcceptByTokenRequest,
    user_id: Annotated[str, Depends(get_current_user_id)],
    user_email: Annotated[str, Depends(get_current_user_email)],
):
    """Accept an invitation by token (auth required)."""
    try:
        result = await accept_workspace_invitation_by_token(
            token=request.token,
            user_id=user_id,
            user_email=user_email,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to accept invitation by token", logger)
