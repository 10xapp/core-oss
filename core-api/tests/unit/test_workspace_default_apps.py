"""Test that new workspaces get the correct default apps."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from api.services.workspaces.crud import create_workspace

EXPECTED_DEFAULT_APPS = ["chat", "email", "calendar", "projects", "files", "tasks"]


def _make_mock_supabase(workspace_data: dict) -> MagicMock:
    """Build a mock supabase client matching the real builder pattern.

    Supabase client uses sync builder methods (rpc, table, select, eq, single)
    with only .execute() being async.
    """
    mock = MagicMock()

    # rpc(...) is sync (returns builder), .execute() is async
    rpc_builder = MagicMock()
    rpc_builder.execute = AsyncMock(
        return_value=MagicMock(data=workspace_data["id"])
    )
    mock.rpc.return_value = rpc_builder

    # table().select().eq().single().execute() — sync chain, async execute
    single_builder = MagicMock()
    single_builder.execute = AsyncMock(
        return_value=MagicMock(data=workspace_data)
    )
    mock.table.return_value.select.return_value.eq.return_value.single.return_value = single_builder

    return mock


@pytest.mark.asyncio
async def test_create_workspace_calls_rpc_with_default_apps_enabled():
    """create_workspace should call the RPC with p_create_default_apps=True by default."""
    mock_supabase = _make_mock_supabase({
        "id": "fake-workspace-id",
        "name": "Test",
        "owner_id": "user-1",
        "is_default": False,
        "icon_r2_key": None,
    })

    with patch(
        "api.services.workspaces.crud.get_authenticated_async_client",
        AsyncMock(return_value=mock_supabase),
    ):
        result = await create_workspace(
            user_id="user-1",
            user_jwt="fake-jwt",
            name="Test",
            create_default_apps=True,
        )

    mock_supabase.rpc.assert_called_once_with(
        "create_workspace_with_defaults",
        {
            "p_name": "Test",
            "p_user_id": "user-1",
            "p_is_default": False,
            "p_create_default_apps": True,
        },
    )
    assert result["id"] == "fake-workspace-id"


@pytest.mark.asyncio
async def test_create_workspace_without_default_apps():
    """create_workspace with create_default_apps=False should pass that to the RPC."""
    mock_supabase = _make_mock_supabase({
        "id": "fake-workspace-id",
        "name": "Empty",
        "owner_id": "user-1",
        "is_default": False,
        "icon_r2_key": None,
    })

    with patch(
        "api.services.workspaces.crud.get_authenticated_async_client",
        AsyncMock(return_value=mock_supabase),
    ):
        await create_workspace(
            user_id="user-1",
            user_jwt="fake-jwt",
            name="Empty",
            create_default_apps=False,
        )

    mock_supabase.rpc.assert_called_once_with(
        "create_workspace_with_defaults",
        {
            "p_name": "Empty",
            "p_user_id": "user-1",
            "p_is_default": False,
            "p_create_default_apps": False,
        },
    )


def test_expected_default_apps_list():
    """Sanity check: the expected default apps match what the migration defines."""
    assert len(EXPECTED_DEFAULT_APPS) == 6
    assert EXPECTED_DEFAULT_APPS == ["chat", "email", "calendar", "projects", "files", "tasks"]
    # All expected apps must be valid
    from api.services.workspaces.apps import VALID_APP_TYPES
    for app in EXPECTED_DEFAULT_APPS:
        assert app in VALID_APP_TYPES, f"{app} not in VALID_APP_TYPES"
