from unittest.mock import AsyncMock

from fastapi import HTTPException

from tests.conftest import TEST_USER_ID, TEST_USER_JWT


def test_subscribe_document_requires_access(client, monkeypatch):
    from api.routers import notifications as notifications_router

    subscribe_mock = AsyncMock()

    monkeypatch.setattr(notifications_router, "normalize_resource_type", lambda value: value)
    monkeypatch.setattr(
        notifications_router,
        "assert_document_access",
        AsyncMock(
            side_effect=HTTPException(
                status_code=403,
                detail="You don't have access to this document",
            )
        ),
    )
    monkeypatch.setattr(notifications_router, "subscribe", subscribe_mock)

    response = client.post("/api/notifications/subscribe/document/doc-1")

    assert response.status_code == 403
    assert response.json()["detail"] == "You don't have access to this document"
    notifications_router.assert_document_access.assert_awaited_once_with(
        user_id=TEST_USER_ID,
        user_jwt=TEST_USER_JWT,
        document_id="doc-1",
    )
    subscribe_mock.assert_not_awaited()


def test_subscribe_document_subscribes_after_access_check(client, monkeypatch):
    from api.routers import notifications as notifications_router

    subscribe_mock = AsyncMock()
    access_mock = AsyncMock()

    monkeypatch.setattr(notifications_router, "normalize_resource_type", lambda value: value)
    monkeypatch.setattr(notifications_router, "assert_document_access", access_mock)
    monkeypatch.setattr(notifications_router, "subscribe", subscribe_mock)

    response = client.post("/api/notifications/subscribe/document/doc-1")

    assert response.status_code == 200
    assert response.json() == {"status": "subscribed"}
    access_mock.assert_awaited_once_with(
        user_id=TEST_USER_ID,
        user_jwt=TEST_USER_JWT,
        document_id="doc-1",
    )
    subscribe_mock.assert_awaited_once_with(
        user_id=TEST_USER_ID,
        resource_type="document",
        resource_id="doc-1",
        reason="manual",
    )
