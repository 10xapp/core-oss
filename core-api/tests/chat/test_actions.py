import json
import pytest
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime, timezone


from tests.conftest import MockAPIResponse, _create_async_query_builder, TEST_USER_ID, TEST_USER_JWT


def make_async_client_with_side_effects(responses):
    client = MagicMock()
    qb = _create_async_query_builder(None)
    qb.execute = AsyncMock(side_effect=responses)
    client.table.return_value = qb
    client._query_builder = qb
    return client


def test_mark_action_executed_success(client, monkeypatch):
    from api.routers import chat

    # Sequence: message_check, conv_check, message_data (with action), update
    responses = [
        MockAPIResponse(data=[{"id": "mid-1", "conversation_id": "cid-1"}]),
        MockAPIResponse(data=[{"id": "cid-1"}]),
        MockAPIResponse(data=[{"content_parts": [
            {"id": "act-123", "type": "action", "data": {"status": "staged", "action": "create_todo", "data": {"title": "Test"}}}
        ]}]),
        MockAPIResponse(data=[{"id": "mid-1"}]),
    ]
    mock_async_service = make_async_client_with_side_effects(responses)
    monkeypatch.setattr(chat, "get_async_service_role_client", AsyncMock(return_value=mock_async_service))
    # Mock _execute_action so we don't hit real services
    monkeypatch.setattr(chat, "_execute_action", AsyncMock(return_value=None))

    # Use a valid UUID for message_id to pass route validation
    message_id = "11111111-1111-1111-1111-111111111111"
    resp = client.patch(f"/api/chat/messages/{message_id}/actions/act-123/execute")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("success") is True
    assert body.get("status") == "executed"


@pytest.mark.asyncio
async def test_execute_action_dispatches_create_project_issue(monkeypatch):
    from api.routers import chat
    import api.services.projects as project_service

    captured = {}

    async def fake_create_issue(*, user_id, user_jwt, board_id, state_id, title, description=None, priority=0, due_at=None):
        captured.update(
            {
                "user_id": user_id,
                "user_jwt": user_jwt,
                "board_id": board_id,
                "state_id": state_id,
                "title": title,
                "description": description,
                "priority": priority,
                "due_at": due_at,
            }
        )
        return {
            "id": "issue-1",
            "board_id": board_id,
            "workspace_id": "ws-1",
            "number": 12,
            "title": title,
        }

    monkeypatch.setattr(project_service, "create_issue", fake_create_issue)

    result = await chat._execute_action(
        "create_project_issue",
        {
            "board_id": "board-1",
            "state_id": "state-1",
            "title": "Ship project writes",
            "description": "Add staged card creation to chat",
            "priority": "2",
            "due_at": "2026-04-04T09:30:00Z",
        },
        TEST_USER_ID,
        TEST_USER_JWT,
    )

    assert captured["user_id"] == TEST_USER_ID
    assert captured["user_jwt"] == TEST_USER_JWT
    assert captured["board_id"] == "board-1"
    assert captured["state_id"] == "state-1"
    assert captured["title"] == "Ship project writes"
    assert captured["description"] == "Add staged card creation to chat"
    assert captured["priority"] == 2
    assert captured["due_at"] == datetime(2026, 4, 4, 9, 30, tzinfo=timezone.utc)

    assert result == {
        "issue_id": "issue-1",
        "board_id": "board-1",
        "workspace_id": "ws-1",
        "number": 12,
        "title": "Ship project writes",
    }
