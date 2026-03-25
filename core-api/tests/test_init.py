from unittest.mock import AsyncMock

from tests.conftest import TEST_USER_ID, TEST_USER_JWT


def test_get_init_data_success_returns_batched_payload(client, monkeypatch):
    from api.routers import init as init_router

    workspaces = [
        {
            "id": "ws-1",
            "name": "Workspace 1",
            "is_default": True,
        },
        {
            "id": "ws-2",
            "name": "Workspace 2",
            "is_default": False,
        },
    ]

    apps_by_workspace = {
        "ws-1": [
            {
                "id": "msg-app-1",
                "workspace_id": "ws-1",
                "app_type": "messages",
                "is_public": True,
                "position": 0,
                "config": {},
                "created_at": "2026-01-01T00:00:00Z",
            },
            {
                "id": "tasks-app-1",
                "workspace_id": "ws-1",
                "app_type": "tasks",
                "is_public": True,
                "position": 1,
                "config": {},
                "created_at": "2026-01-01T00:00:00Z",
            },
        ],
        "ws-2": [
            {
                "id": "msg-app-2",
                "workspace_id": "ws-2",
                "app_type": "messages",
                "is_public": True,
                "position": 0,
                "config": {},
                "created_at": "2026-01-01T00:00:00Z",
            },
        ],
    }

    channels_by_app = {
        "msg-app-1": [{"id": "ch-1", "workspace_app_id": "msg-app-1"}],
        "msg-app-2": [{"id": "ch-2", "workspace_app_id": "msg-app-2"}],
    }
    dms_by_app = {
        "msg-app-1": [{"id": "dm-1", "workspace_app_id": "msg-app-1", "is_dm": True}],
        "msg-app-2": [{"id": "dm-2", "workspace_app_id": "msg-app-2", "is_dm": True}],
    }
    unread_by_app = {
        "msg-app-1": {"ch-1": 3},
        "msg-app-2": {"ch-2": 5},
    }

    monkeypatch.setattr(
        init_router,
        "get_workspaces",
        AsyncMock(return_value=workspaces),
    )
    monkeypatch.setattr(
        init_router,
        "get_default_workspace",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        init_router,
        "get_workspace_apps",
        AsyncMock(side_effect=lambda ws_id, _jwt: apps_by_workspace[ws_id]),
    )
    monkeypatch.setattr(
        init_router,
        "get_channels",
        AsyncMock(side_effect=lambda app_id, _jwt: channels_by_app[app_id]),
    )
    monkeypatch.setattr(
        init_router,
        "get_user_dms",
        AsyncMock(side_effect=lambda app_id, _uid, _jwt: dms_by_app[app_id]),
    )
    monkeypatch.setattr(
        init_router,
        "get_unread_counts",
        AsyncMock(side_effect=lambda app_id, _jwt: unread_by_app[app_id]),
    )

    response = client.get("/api/me/init")

    assert response.status_code == 200
    data = response.json()
    assert len(data["workspaces"]) == 2
    assert len(data["workspaces"][0]["apps"]) == 2
    assert len(data["workspaces"][1]["apps"]) == 1
    assert data["channels_by_app"]["msg-app-1"][0]["id"] == "ch-1"
    assert data["dms_by_app"]["msg-app-2"][0]["id"] == "dm-2"
    assert data["unread_counts"] == {"ch-1": 3, "ch-2": 5}


def test_get_init_data_recovers_default_workspace(client, monkeypatch):
    from api.routers import init as init_router

    default_workspace = {
        "id": "ws-default",
        "name": "Personal",
        "is_default": True,
    }

    get_workspaces_mock = AsyncMock(return_value=[])
    get_default_workspace_mock = AsyncMock(return_value=default_workspace)
    get_workspace_apps_mock = AsyncMock(return_value=[])
    get_channels_mock = AsyncMock(return_value=[])
    get_user_dms_mock = AsyncMock(return_value=[])
    get_unread_counts_mock = AsyncMock(return_value={})

    monkeypatch.setattr(init_router, "get_workspaces", get_workspaces_mock)
    monkeypatch.setattr(init_router, "get_default_workspace", get_default_workspace_mock)
    monkeypatch.setattr(init_router, "get_workspace_apps", get_workspace_apps_mock)
    monkeypatch.setattr(init_router, "get_channels", get_channels_mock)
    monkeypatch.setattr(init_router, "get_user_dms", get_user_dms_mock)
    monkeypatch.setattr(init_router, "get_unread_counts", get_unread_counts_mock)

    response = client.get("/api/me/init")

    assert response.status_code == 200
    data = response.json()
    assert [ws["id"] for ws in data["workspaces"]] == ["ws-default"]
    assert data["workspaces"][0]["apps"] == []
    assert data["channels_by_app"] == {}
    assert data["dms_by_app"] == {}
    assert data["unread_counts"] == {}

    get_workspaces_mock.assert_awaited_once_with(TEST_USER_ID, TEST_USER_JWT)
    get_default_workspace_mock.assert_awaited_once_with(TEST_USER_ID, TEST_USER_JWT)


def test_get_init_data_returns_empty_when_no_workspaces_exist(client, monkeypatch):
    from api.routers import init as init_router

    get_workspaces_mock = AsyncMock(return_value=[])
    get_default_workspace_mock = AsyncMock(return_value=None)
    get_workspace_apps_mock = AsyncMock(return_value=[])

    monkeypatch.setattr(init_router, "get_workspaces", get_workspaces_mock)
    monkeypatch.setattr(init_router, "get_default_workspace", get_default_workspace_mock)
    monkeypatch.setattr(init_router, "get_workspace_apps", get_workspace_apps_mock)
    monkeypatch.setattr(init_router, "get_channels", AsyncMock(return_value=[]))
    monkeypatch.setattr(init_router, "get_user_dms", AsyncMock(return_value=[]))
    monkeypatch.setattr(init_router, "get_unread_counts", AsyncMock(return_value={}))

    response = client.get("/api/me/init")

    assert response.status_code == 200
    assert response.json() == {
        "workspaces": [],
        "channels_by_app": {},
        "dms_by_app": {},
        "unread_counts": {},
    }
    get_workspace_apps_mock.assert_not_awaited()


def test_get_init_data_tolerates_workspace_app_fetch_failure(client, monkeypatch):
    from api.routers import init as init_router

    workspaces = [
        {"id": "ws-bad", "name": "Bad WS", "is_default": False},
        {"id": "ws-good", "name": "Good WS", "is_default": True},
    ]

    async def get_apps_side_effect(workspace_id: str, _jwt: str):
        if workspace_id == "ws-bad":
            raise RuntimeError("apps query failed")
        return [
            {
                "id": "msg-app-good",
                "workspace_id": "ws-good",
                "app_type": "messages",
                "is_public": True,
                "position": 0,
                "config": {},
                "created_at": "2026-01-01T00:00:00Z",
            }
        ]

    monkeypatch.setattr(init_router, "get_workspaces", AsyncMock(return_value=workspaces))
    monkeypatch.setattr(init_router, "get_default_workspace", AsyncMock(return_value=None))
    monkeypatch.setattr(init_router, "get_workspace_apps", AsyncMock(side_effect=get_apps_side_effect))
    monkeypatch.setattr(init_router, "get_channels", AsyncMock(return_value=[]))
    monkeypatch.setattr(init_router, "get_user_dms", AsyncMock(return_value=[]))
    monkeypatch.setattr(init_router, "get_unread_counts", AsyncMock(return_value={}))

    response = client.get("/api/me/init")

    assert response.status_code == 200
    data = response.json()
    bad_ws = next(ws for ws in data["workspaces"] if ws["id"] == "ws-bad")
    good_ws = next(ws for ws in data["workspaces"] if ws["id"] == "ws-good")
    assert bad_ws["apps"] == []
    assert len(good_ws["apps"]) == 1


def test_get_init_data_tolerates_message_fetch_failures(client, monkeypatch):
    from api.routers import init as init_router

    workspaces = [{"id": "ws-1", "name": "Workspace", "is_default": True}]
    apps = [
        {
            "id": "msg-app-bad",
            "workspace_id": "ws-1",
            "app_type": "messages",
            "is_public": True,
            "position": 0,
            "config": {},
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "id": "msg-app-good",
            "workspace_id": "ws-1",
            "app_type": "messages",
            "is_public": True,
            "position": 1,
            "config": {},
            "created_at": "2026-01-01T00:00:00Z",
        },
    ]

    async def channels_side_effect(app_id: str, _jwt: str):
        if app_id == "msg-app-bad":
            raise RuntimeError("channels failed")
        return [{"id": "ch-good", "workspace_app_id": app_id}]

    async def dms_side_effect(app_id: str, _uid: str, _jwt: str):
        if app_id == "msg-app-bad":
            raise RuntimeError("dms failed")
        return [{"id": "dm-good", "workspace_app_id": app_id, "is_dm": True}]

    async def unread_side_effect(app_id: str, _jwt: str):
        if app_id == "msg-app-bad":
            raise RuntimeError("unreads failed")
        return {"ch-good": 7}

    monkeypatch.setattr(init_router, "get_workspaces", AsyncMock(return_value=workspaces))
    monkeypatch.setattr(init_router, "get_default_workspace", AsyncMock(return_value=None))
    monkeypatch.setattr(init_router, "get_workspace_apps", AsyncMock(return_value=apps))
    monkeypatch.setattr(init_router, "get_channels", AsyncMock(side_effect=channels_side_effect))
    monkeypatch.setattr(init_router, "get_user_dms", AsyncMock(side_effect=dms_side_effect))
    monkeypatch.setattr(init_router, "get_unread_counts", AsyncMock(side_effect=unread_side_effect))

    response = client.get("/api/me/init")

    assert response.status_code == 200
    data = response.json()
    assert data["channels_by_app"]["msg-app-bad"] == []
    assert data["dms_by_app"]["msg-app-bad"] == []
    assert data["channels_by_app"]["msg-app-good"][0]["id"] == "ch-good"
    assert data["dms_by_app"]["msg-app-good"][0]["id"] == "dm-good"
    assert data["unread_counts"] == {"ch-good": 7}
