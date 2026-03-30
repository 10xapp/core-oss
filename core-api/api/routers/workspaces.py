"""
Workspaces router - HTTP endpoints for workspace management
"""
from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import Optional, Dict, Any, List
from api.request_types import WorkspaceRole, WorkspaceAppType
from api.services.workspaces import (
    get_workspaces,
    get_workspace_by_id,
    create_workspace,
    update_workspace,
    delete_workspace,
    get_default_workspace,
    get_workspace_members,
    update_member_role,
    remove_workspace_member,
    leave_workspace,
    get_user_workspace_role,
    get_workspace_apps,
    create_workspace_app,
    delete_workspace_app,
    update_workspace_app,
    add_app_member,
    remove_app_member,
    reorder_workspace_apps,
)
from api.services.workspaces.apps import get_app_members
from api.services.users import search_users_by_email
from api.dependencies import get_current_user_jwt, get_current_user_id
from api.exceptions import handle_api_exception
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/workspaces", tags=["workspaces"])


# ============================================================================
# Request/Response Models
# ============================================================================

class CreateWorkspaceRequest(BaseModel):
    """Request model for creating a workspace"""
    name: str = Field(..., min_length=1, max_length=100)
    create_default_apps: bool = Field(default=True, description="Create the 5 default mini-apps")

    class Config:
        json_schema_extra = {
            "example": {
                "name": "My Project",
                "create_default_apps": True
            }
        }


class UpdateWorkspaceRequest(BaseModel):
    """Request model for updating a workspace"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    emoji: Optional[str] = Field(None, max_length=10, description="Emoji icon for workspace")
    icon_r2_key: Optional[str] = Field(None, description="R2 key for workspace icon (from file upload)")
    clear_icon: bool = False


class AddMemberRequest(BaseModel):
    """Request model for adding a workspace member"""
    email: EmailStr = Field(..., description="Email of user to add")
    role: WorkspaceRole = Field(default=WorkspaceRole.MEMBER, description="Role: 'member' or 'admin'")

    @field_validator("role", mode="before")
    @classmethod
    def normalize_role(cls, value: WorkspaceRole | str) -> WorkspaceRole | str:
        return value.strip().lower() if isinstance(value, str) else value

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "role": "member"
            }
        }


class UpdateMemberRoleRequest(BaseModel):
    """Request model for updating a member's role"""
    role: WorkspaceRole = Field(..., description="New role: 'member' or 'admin'")

    @field_validator("role", mode="before")
    @classmethod
    def normalize_role(cls, value: WorkspaceRole | str) -> WorkspaceRole | str:
        return value.strip().lower() if isinstance(value, str) else value


class CreateAppRequest(BaseModel):
    """Request model for creating a workspace app"""
    app_type: WorkspaceAppType = Field(..., description="App type (chat, files, messages, dashboard, projects, email, calendar, agents)")
    is_public: bool = Field(default=True, description="Whether app is visible to all members")
    position: Optional[int] = Field(None, ge=0, description="Display order position")

    @field_validator("app_type", mode="before")
    @classmethod
    def normalize_app_type(cls, value: WorkspaceAppType | str) -> WorkspaceAppType | str:
        return value.strip().lower() if isinstance(value, str) else value

    class Config:
        json_schema_extra = {
            "example": {
                "app_type": "chat",
                "is_public": True
            }
        }


class UpdateAppRequest(BaseModel):
    """Request model for updating a workspace app"""
    is_public: Optional[bool] = Field(None, description="Whether app is visible to all members")
    position: Optional[int] = Field(None, ge=0, description="Display order position")
    config: Optional[Dict[str, Any]] = Field(None, description="App-specific configuration")


class AddAppMemberRequest(BaseModel):
    """Request model for adding a user to a private app"""
    user_id: str = Field(..., description="User ID to add")


class AppPosition(BaseModel):
    """Single app position for reordering"""
    id: str = Field(..., description="App ID")
    position: int = Field(..., ge=0, description="New position (0-indexed)")


class ReorderAppsRequest(BaseModel):
    """Request model for reordering workspace apps"""
    app_positions: List[AppPosition] = Field(..., description="List of app positions")

    class Config:
        json_schema_extra = {
            "example": {
                "app_positions": [
                    {"id": "uuid1", "position": 0},
                    {"id": "uuid2", "position": 1},
                    {"id": "uuid3", "position": 2}
                ]
            }
        }


# ============================================================================
# Response Models
# ============================================================================

class WorkspaceItemResponse(BaseModel):
    """Response model for a single workspace."""
    id: str
    name: str
    owner_user_id: Optional[str] = None
    is_default: bool = False
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    role: Optional[str] = None

    class Config:
        extra = "allow"


class WorkspaceListResponse(BaseModel):
    """Response model for workspace list."""
    workspaces: List[WorkspaceItemResponse]
    count: int


class WorkspaceSingleResponse(BaseModel):
    """Response wrapping a single workspace."""
    workspace: WorkspaceItemResponse


class CreateWorkspaceResponse(BaseModel):
    """Response for workspace creation, includes optional welcome_note_id."""
    workspace: WorkspaceItemResponse
    welcome_note_id: Optional[str] = None


class WorkspaceDeleteResponse(BaseModel):
    """Response for workspace deletion."""
    success: bool
    message: str


class MemberItemResponse(BaseModel):
    """Response model for a workspace/app member."""
    id: Optional[str] = None
    user_id: str
    workspace_id: Optional[str] = None
    role: Optional[str] = None
    joined_at: Optional[str] = None
    email: Optional[str] = None
    name: Optional[str] = None
    avatar_url: Optional[str] = None

    class Config:
        extra = "allow"


class MemberListResponse(BaseModel):
    """Response model for member list."""
    members: List[MemberItemResponse]
    count: int


class MemberSingleResponse(BaseModel):
    """Response wrapping a single member."""
    member: MemberItemResponse


class MemberRemoveResponse(BaseModel):
    """Response for member removal."""
    success: bool
    message: str


class AppItemResponse(BaseModel):
    """Response model for a workspace app."""
    id: str
    workspace_id: str
    app_type: str
    is_public: bool = True
    position: int = 0
    config: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None

    class Config:
        extra = "allow"


class AppListResponse(BaseModel):
    """Response model for app list."""
    apps: List[AppItemResponse]
    count: int


class AppSingleResponse(BaseModel):
    """Response wrapping a single app."""
    app: AppItemResponse


class AppMemberRemoveResponse(BaseModel):
    """Response for app member removal."""
    success: bool
    message: str


class ReorderAppsResponse(BaseModel):
    """Response for app reorder operation."""
    message: str
    updated_count: int


# ============================================================================
# User Search Endpoint
# ============================================================================

class UserSearchResult(BaseModel):
    """Response model for user search."""
    id: str
    email: str
    name: Optional[str] = None


class UserSearchResponse(BaseModel):
    """Response model for user search results."""
    users: List[UserSearchResult]
    count: int


def _mask_email(email: str) -> str:
    """Mask email for logging (PII protection)."""
    if len(email) <= 3:
        return "***"
    return email[:3] + "***"


@router.get("/{workspace_id}/users/search", response_model=UserSearchResponse)
async def search_users_endpoint(
    workspace_id: str,
    email: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Search for users by email within workspace context.

    Requires admin or owner role in the workspace.
    Used to find users to add to a workspace.
    Returns matching users with their id, email, and name.
    """
    try:
        # Check user has admin/owner role in workspace
        role = await get_user_workspace_role(workspace_id, user_id, user_jwt)
        if role not in ("admin", "owner"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only workspace admins and owners can search for users to add"
            )

        if len(email) < 3:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email query must be at least 3 characters"
            )

        logger.info(f"User {user_id} searching for users (query: {_mask_email(email)})")
        users = await search_users_by_email(email, limit=10)
        return {
            "users": users,
            "count": len(users)
        }
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to search users", logger)


# ============================================================================
# Workspace CRUD Endpoints
# ============================================================================

@router.get("", response_model=WorkspaceListResponse)
async def list_workspaces_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    List all workspaces the current user is a member of.

    Returns workspaces with the user's role in each.
    """
    try:
        logger.info(f"Listing workspaces for user {user_id}")
        workspaces = await get_workspaces(user_id, user_jwt)
        return {
            "workspaces": workspaces,
            "count": len(workspaces)
        }
    except Exception as e:
        handle_api_exception(e, "Failed to list workspaces", logger)


@router.get("/default", response_model=WorkspaceSingleResponse)
async def get_default_workspace_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Get the user's default workspace.

    Every user has one default workspace created automatically.
    """
    try:
        logger.info(f"Getting default workspace for user {user_id}")
        workspace = await get_default_workspace(user_id, user_jwt)

        if not workspace:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Default workspace not found"
            )

        return {"workspace": workspace}
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to get default workspace", logger)


@router.get("/{workspace_id}", response_model=WorkspaceSingleResponse)
async def get_workspace_endpoint(
    workspace_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Get a single workspace by ID.

    User must be a member of the workspace.
    """
    try:
        logger.info(f"Getting workspace {workspace_id} for user {user_id}")
        workspace = await get_workspace_by_id(workspace_id, user_jwt)

        if not workspace:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Workspace not found"
            )

        return {"workspace": workspace}
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to get workspace", logger)


@router.post("", response_model=CreateWorkspaceResponse, status_code=status.HTTP_201_CREATED)
async def create_workspace_endpoint(
    request: CreateWorkspaceRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Create a new workspace.

    The current user becomes the owner of the workspace.
    By default, creates the standard set of workspace apps (chat, files, messages, dashboard, projects, email, calendar, agents).
    """
    try:
        logger.info(f"Creating workspace '{request.name}' for user {user_id}")
        workspace = await create_workspace(
            user_id=user_id,
            user_jwt=user_jwt,
            name=request.name,
            create_default_apps=request.create_default_apps
        )
        welcome_note_id = workspace.pop("welcome_note_id", None)
        return {"workspace": workspace, "welcome_note_id": welcome_note_id}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to create workspace", logger)


@router.patch("/{workspace_id}", response_model=WorkspaceSingleResponse)
async def update_workspace_endpoint(
    workspace_id: str,
    request: UpdateWorkspaceRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Update a workspace's settings.

    Only admins and owners can update workspace settings.
    """
    try:
        logger.info(f"Updating workspace {workspace_id} for user {user_id}")
        workspace = await update_workspace(
            workspace_id=workspace_id,
            user_jwt=user_jwt,
            name=request.name,
            emoji=request.emoji,
            icon_r2_key=request.icon_r2_key,
            clear_icon=request.clear_icon
        )
        return {"workspace": workspace}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to update workspace", logger)


@router.delete("/{workspace_id}", response_model=WorkspaceDeleteResponse)
async def delete_workspace_endpoint(
    workspace_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Delete a workspace.

    Only the workspace owner can delete it.
    Default workspaces cannot be deleted.
    """
    try:
        logger.info(f"Deleting workspace {workspace_id} for user {user_id}")
        await delete_workspace(workspace_id, user_jwt)
        return {"success": True, "message": "Workspace deleted"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to delete workspace", logger)


# ============================================================================
# Member Management Endpoints
# ============================================================================

@router.get("/{workspace_id}/members", response_model=MemberListResponse)
async def list_members_endpoint(
    workspace_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    List all members of a workspace.

    User must be a member of the workspace.
    """
    try:
        logger.info(f"Listing members for workspace {workspace_id}")
        members = await get_workspace_members(workspace_id, user_jwt)
        return {
            "members": members,
            "count": len(members)
        }
    except Exception as e:
        handle_api_exception(e, "Failed to list members", logger)


@router.post("/{workspace_id}/members", response_model=MemberSingleResponse, status_code=status.HTTP_201_CREATED)
async def add_member_endpoint(
    workspace_id: str,
    _request: AddMemberRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Deprecated endpoint.

    Direct member add is no longer supported for normal clients.
    Use workspace invitations instead.
    """
    logger.warning(
        "Deprecated direct member add called by user %s for workspace %s",
        user_id,
        workspace_id,
    )
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail="Direct member add is deprecated. Use workspace invitation endpoints.",
    )


@router.patch("/{workspace_id}/members/{member_user_id}", response_model=MemberSingleResponse)
async def update_member_role_endpoint(
    workspace_id: str,
    member_user_id: str,
    request: UpdateMemberRoleRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Update a member's role in a workspace.

    Only admins and owners can update roles.
    Cannot change the owner's role.
    """
    try:
        logger.info(f"Updating role for user {member_user_id} in workspace {workspace_id}")
        member = await update_member_role(
            workspace_id=workspace_id,
            user_jwt=user_jwt,
            member_user_id=member_user_id,
            new_role=request.role
        )
        return {"member": member}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to update member role", logger)


@router.delete("/{workspace_id}/members/{member_user_id}", response_model=MemberRemoveResponse)
async def remove_member_endpoint(
    workspace_id: str,
    member_user_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Remove a member from a workspace.

    Only admins and owners can remove members.
    Cannot remove the workspace owner.
    """
    try:
        logger.info(f"Removing user {member_user_id} from workspace {workspace_id}")
        await remove_workspace_member(
            workspace_id=workspace_id,
            user_jwt=user_jwt,
            member_user_id=member_user_id
        )
        return {"success": True, "message": "Member removed"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to remove member", logger)


@router.post("/{workspace_id}/leave", response_model=MemberRemoveResponse)
async def leave_workspace_endpoint(
    workspace_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Leave a workspace voluntarily.

    Any non-owner member can leave a workspace.
    Owners must transfer ownership or delete the workspace instead.
    """
    try:
        logger.info(f"User {user_id} leaving workspace {workspace_id}")
        await leave_workspace(
            workspace_id=workspace_id,
            user_id=user_id,
            user_jwt=user_jwt
        )
        return {"success": True, "message": "Left workspace"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to leave workspace", logger)


# ============================================================================
# App Management Endpoints
# ============================================================================

@router.get("/{workspace_id}/apps", response_model=AppListResponse)
async def list_apps_endpoint(
    workspace_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    List all apps in a workspace.

    Returns apps the user has access to.
    """
    try:
        logger.info(f"Listing apps for workspace {workspace_id}")
        apps = await get_workspace_apps(workspace_id, user_jwt)
        return {
            "apps": apps,
            "count": len(apps)
        }
    except Exception as e:
        handle_api_exception(e, "Failed to list apps", logger)


@router.post("/{workspace_id}/apps", response_model=AppSingleResponse, status_code=status.HTTP_201_CREATED)
async def create_app_endpoint(
    workspace_id: str,
    request: CreateAppRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Create a new app in a workspace.

    Only admins and owners can create apps.
    """
    try:
        logger.info(f"Creating {request.app_type} app in workspace {workspace_id}")
        app = await create_workspace_app(
            workspace_id=workspace_id,
            app_type=request.app_type,
            user_jwt=user_jwt,
            is_public=request.is_public,
            position=request.position
        )
        return {"app": app}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to create app", logger)


@router.delete("/{workspace_id}/apps/{app_id}", response_model=AppMemberRemoveResponse)
async def delete_app_endpoint(
    workspace_id: str,
    app_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Delete a workspace app.

    Only admins and owners can delete apps.
    This will also delete all app-specific data.
    """
    try:
        logger.info(f"Deleting app {app_id} from workspace {workspace_id}")
        await delete_workspace_app(app_id, user_jwt)
        return {"success": True, "message": "App deleted"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to delete app", logger)


@router.patch("/{workspace_id}/apps/{app_id}", response_model=AppSingleResponse)
async def update_app_endpoint(
    workspace_id: str,
    app_id: str,
    request: UpdateAppRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Update a workspace app's settings.

    Only admins and owners can update app settings.
    """
    try:
        logger.info(f"Updating app {app_id} in workspace {workspace_id}")
        app = await update_workspace_app(
            workspace_app_id=app_id,
            user_jwt=user_jwt,
            is_public=request.is_public,
            position=request.position,
            config=request.config
        )
        return {"app": app}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to update app", logger)


@router.post("/{workspace_id}/apps/reorder", response_model=ReorderAppsResponse)
async def reorder_apps_endpoint(
    workspace_id: str,
    request: ReorderAppsRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Reorder apps within a workspace.

    Updates the position of multiple apps atomically.
    Only admins and owners can reorder apps.
    """
    try:
        logger.info(f"Reordering apps in workspace {workspace_id}")

        positions = [
            {"id": p.id, "position": p.position}
            for p in request.app_positions
        ]

        result = await reorder_workspace_apps(
            workspace_id=workspace_id,
            user_jwt=user_jwt,
            app_positions=positions
        )

        logger.info(f"Reordered apps: {result.get('updated_count', 0)} updated")
        return result

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to reorder apps", logger)


@router.get("/{workspace_id}/apps/{app_id}/members", response_model=MemberListResponse)
async def list_app_members_endpoint(
    workspace_id: str,
    app_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    List members with explicit access to a private app.

    Only available for private apps (is_public=false).
    """
    try:
        logger.info(f"Listing members for app {app_id}")
        members = await get_app_members(app_id, user_jwt)
        return {
            "members": members,
            "count": len(members)
        }
    except Exception as e:
        handle_api_exception(e, "Failed to list app members", logger)


@router.post("/{workspace_id}/apps/{app_id}/members", response_model=MemberSingleResponse, status_code=status.HTTP_201_CREATED)
async def add_app_member_endpoint(
    workspace_id: str,
    app_id: str,
    request: AddAppMemberRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Add a user to a private app.

    Only admins and owners can add members to private apps.
    """
    try:
        logger.info(f"Adding user {request.user_id} to app {app_id}")
        member = await add_app_member(
            workspace_app_id=app_id,
            user_jwt=user_jwt,
            member_user_id=request.user_id,
            added_by_user_id=user_id
        )
        return {"member": member}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to add app member", logger)


@router.delete("/{workspace_id}/apps/{app_id}/members/{member_user_id}", response_model=AppMemberRemoveResponse)
async def remove_app_member_endpoint(
    workspace_id: str,
    app_id: str,
    member_user_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Remove a user from a private app.

    Only admins and owners can remove members from private apps.
    """
    try:
        logger.info(f"Removing user {member_user_id} from app {app_id}")
        await remove_app_member(
            workspace_app_id=app_id,
            user_jwt=user_jwt,
            member_user_id=member_user_id
        )
        return {"success": True, "message": "App member removed"}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        handle_api_exception(e, "Failed to remove app member", logger)
