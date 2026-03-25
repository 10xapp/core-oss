from unittest.mock import AsyncMock

import pytest

from tests.conftest import TEST_USER_ID, TEST_USER_JWT


@pytest.mark.asyncio
async def test_get_project_board_returns_board_issue_summaries(monkeypatch):
    import api.services.projects as project_service
    import api.services.workspaces as workspace_service

    from lib.tools.base import ToolContext
    from lib.tools.definitions import projects as project_tools

    board = {
        "id": "board-1",
        "workspace_id": "ws-1",
        "workspace_app_id": "app-1",
        "name": "Core Board",
        "key": "CORE",
        "position": 0,
        "next_issue_number": 8,
    }
    states = [
        {"id": "state-1", "name": "To Do", "position": 0, "is_done": False, "color": "#ef4444"},
        {"id": "state-2", "name": "Done", "position": 1, "is_done": True, "color": "#10b981"},
    ]
    issues = [
        {
            "id": "issue-1",
            "board_id": "board-1",
            "state_id": "state-1",
            "number": 7,
            "title": "Ship board chat access",
            "description": "Add enough board context for chat to reason over project cards.",
            "priority": 2,
            "position": 0,
            "label_objects": [{"id": "label-1", "name": "backend", "color": "#000000"}],
            "assignees": [{"user_id": "user-2"}],
            "image_urls": [],
            "created_at": "2026-03-23T10:00:00",
            "updated_at": "2026-03-23T11:00:00",
        },
        {
            "id": "issue-2",
            "board_id": "board-1",
            "state_id": "state-2",
            "number": 6,
            "title": "Older task",
            "description": None,
            "priority": 4,
            "position": 0,
            "label_objects": [],
            "assignees": [],
            "completed_at": "2026-03-22T09:00:00",
        },
    ]
    labels = [{"id": "label-1", "name": "backend", "color": "#000000"}]
    members = [{"user_id": "user-2", "name": "Ava", "email": "ava@example.com", "avatar_url": None}]

    monkeypatch.setattr(project_service, "get_board_by_id", AsyncMock(return_value=board))
    monkeypatch.setattr(project_service, "get_states", AsyncMock(return_value=states))
    monkeypatch.setattr(project_service, "get_issues", AsyncMock(return_value=issues))
    monkeypatch.setattr(project_service, "get_labels", AsyncMock(return_value=labels))
    monkeypatch.setattr(workspace_service, "get_workspace_members", AsyncMock(return_value=members))

    ctx = ToolContext(
        user_id=TEST_USER_ID,
        user_jwt=TEST_USER_JWT,
        workspace_ids=["ws-1"],
    )

    result = await project_tools.get_project_board({"board_id": "board-1"}, ctx)

    assert result.status == "success"
    assert result.data["board"]["name"] == "Core Board"
    assert result.data["board"]["open_issue_count"] == 1
    assert result.data["total_issue_count"] == 2
    # include_done defaults to False, so only the open issue is returned
    assert result.data["filtered_issue_count"] == 1
    assert result.data["issues"][0]["reference"] == "CORE-7"
    assert result.data["issues"][0]["state_name"] == "To Do"
    assert result.data["issues"][0]["labels"] == [{"id": "label-1", "name": "backend", "color": "#000000"}]
    assert result.data["issues"][0]["assignees"] == [
        {
            "user_id": "user-2",
            "name": "Ava",
            "email": "ava@example.com",
            "avatar_url": None,
        }
    ]
    assert result.data["states"][0]["issue_count"] == 1
    assert result.data["states"][1]["issue_count"] == 1


@pytest.mark.asyncio
async def test_get_project_board_rejects_out_of_scope_board(monkeypatch):
    import api.services.projects as project_service

    from lib.tools.base import ToolContext
    from lib.tools.definitions import projects as project_tools

    monkeypatch.setattr(
        project_service,
        "get_board_by_id",
        AsyncMock(
            return_value={
                "id": "board-1",
                "workspace_id": "ws-2",
                "workspace_app_id": "app-1",
                "name": "Other Board",
            }
        ),
    )

    ctx = ToolContext(
        user_id=TEST_USER_ID,
        user_jwt=TEST_USER_JWT,
        workspace_ids=["ws-1"],
    )

    result = await project_tools.get_project_board({"board_id": "board-1"}, ctx)

    assert result.status == "error"
    assert result.data["error"] == "Project board is outside the current workspace scope"


@pytest.mark.asyncio
async def test_get_project_issue_returns_full_issue_details(monkeypatch):
    import api.services.projects as project_service
    import api.services.workspaces as workspace_service

    from lib.tools.base import ToolContext
    from lib.tools.definitions import projects as project_tools

    issue = {
        "id": "issue-1",
        "workspace_id": "ws-1",
        "board_id": "board-1",
        "state_id": "state-1",
        "number": 42,
        "title": "Support board mentions in chat",
        "description": "Need a tool call so chat can read the referenced board.",
        "priority": 1,
        "label_objects": [{"id": "label-1", "name": "chat", "color": "#111111"}],
        "assignees": [{"user_id": "user-3"}],
        "image_urls": ["https://example.com/image.png"],
    }
    board = {"id": "board-1", "workspace_id": "ws-1", "name": "Core Board", "key": "CORE"}
    states = [{"id": "state-1", "name": "In Progress", "position": 1, "is_done": False}]
    members = [{"user_id": "user-3", "name": "Kai", "email": "kai@example.com", "avatar_url": None}]

    monkeypatch.setattr(project_service, "get_issue_by_id", AsyncMock(return_value=issue))
    monkeypatch.setattr(project_service, "get_board_by_id", AsyncMock(return_value=board))
    monkeypatch.setattr(project_service, "get_states", AsyncMock(return_value=states))
    monkeypatch.setattr(workspace_service, "get_workspace_members", AsyncMock(return_value=members))

    ctx = ToolContext(
        user_id=TEST_USER_ID,
        user_jwt=TEST_USER_JWT,
        workspace_ids=["ws-1"],
    )

    result = await project_tools.get_project_issue({"issue_id": "issue-1"}, ctx)

    assert result.status == "success"
    assert result.data["issue"]["reference"] == "CORE-42"
    assert result.data["issue"]["board_name"] == "Core Board"
    assert result.data["issue"]["state_name"] == "In Progress"
    assert result.data["issue"]["assignees"] == [
        {
            "user_id": "user-3",
            "name": "Kai",
            "email": "kai@example.com",
            "avatar_url": None,
        }
    ]
    assert result.data["issue"]["image_count"] == 1
