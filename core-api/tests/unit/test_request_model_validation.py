from pydantic import ValidationError
import pytest

from api.routers.invitations import CreateInvitationRequest
from api.routers.permissions import (
    AccessRequestResolve,
    CreateLinkRequest,
    ShareResourceRequest,
)
from api.routers.workspaces import CreateAppRequest, UpdateMemberRoleRequest


def test_workspace_role_fields_normalize_valid_input() -> None:
    member = UpdateMemberRoleRequest(role=" ADMIN ")
    invitation = CreateInvitationRequest(email="user@example.com", role=" member ")

    assert member.role == "admin"
    assert invitation.role == "member"


def test_workspace_role_fields_reject_invalid_values() -> None:
    with pytest.raises(ValidationError):
        UpdateMemberRoleRequest(role="owner")

    with pytest.raises(ValidationError):
        CreateInvitationRequest(email="user@example.com", role="viewer")


def test_create_app_request_normalizes_app_type() -> None:
    request = CreateAppRequest(app_type=" Calendar ", is_public=True)

    assert request.app_type == "calendar"


def test_create_app_request_rejects_unknown_app_type() -> None:
    with pytest.raises(ValidationError):
        CreateAppRequest(app_type="tasks", is_public=True)


def test_permission_requests_normalize_values() -> None:
    share = ShareResourceRequest(
        resource_type=" Document ",
        resource_id="doc-1",
        grantee_email="user@example.com",
        permission=" WRITE ",
    )
    link = CreateLinkRequest(resource_type=" file ", resource_id="file-1", permission=" admin ")
    resolve = AccessRequestResolve(status=" Approved ", permission=" read ")

    assert share.resource_type == "document"
    assert share.permission == "write"
    assert link.resource_type == "file"
    assert link.permission == "admin"
    assert resolve.status == "approved"
    assert resolve.permission == "read"


def test_permission_requests_reject_invalid_values() -> None:
    with pytest.raises(ValidationError):
        ShareResourceRequest(
            resource_type="workspace",
            resource_id="doc-1",
            grantee_email="user@example.com",
            permission="read",
        )

    with pytest.raises(ValidationError):
        CreateLinkRequest(resource_type="file", resource_id="file-1", permission="owner")

    with pytest.raises(ValidationError):
        AccessRequestResolve(status="pending", permission="read")
