import pytest
from fastapi import HTTPException
from unittest.mock import MagicMock, AsyncMock

from tests.conftest import MockAPIResponse, _create_async_query_builder


@pytest.mark.asyncio
async def test_get_documents_size_sort_desc(monkeypatch):
    import copy
    import importlib
    mod = importlib.import_module("api.services.documents.get_documents")

    client = MagicMock()
    docs = [
        {"id": "f1", "is_folder": True, "title": "Folder"},
        {"id": "d1", "is_folder": False, "title": "Doc A", "file": {"file_size": 10}},
        {"id": "d2", "is_folder": False, "title": "Doc B", "file": {"file_size": 100}},
        {"id": "d3", "is_folder": False, "title": "Doc C", "file": {"file_size": 50}},
    ]
    qb = _create_async_query_builder(MockAPIResponse(data=copy.deepcopy(docs)))
    client.table.return_value = qb
    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))

    res = await mod.get_documents(
        user_id="u1", user_jwt="jwt", sort_by="size", sort_direction="desc"
    )
    # Size sort orders all documents by file_size desc (folders have size 0, so they go last)
    sizes = [d.get("file", {}).get("file_size", 0) if d.get("file") else 0 for d in res]
    assert sizes == [100, 50, 10, 0]


@pytest.mark.asyncio
async def test_get_documents_root_filters_parent_null(monkeypatch):
    import importlib
    mod = importlib.import_module("api.services.documents.get_documents")

    client = MagicMock()
    qb = _create_async_query_builder(MockAPIResponse(data=[]))
    client.table.return_value = qb
    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))

    await mod.get_documents(user_id="u1", user_jwt="jwt")

    qb.is_.assert_called_once_with("parent_id", "null")


@pytest.mark.asyncio
async def test_get_document_by_id_owner_updates_last_opened(monkeypatch):
    import importlib
    mod = importlib.import_module("api.services.documents.get_documents")

    client = MagicMock()
    qb = _create_async_query_builder(MockAPIResponse(data=[{"id": "d1", "user_id": "u1", "file": None}]))
    # After select, an update is called; let execute return something again
    qb.execute = AsyncMock(side_effect=[
        MockAPIResponse(data=[{"id": "d1", "user_id": "u1", "file": None}]),  # select
        MockAPIResponse(data=[{"id": "d1"}]),  # update last_opened_at
    ])
    # Track whether update() was invoked on the builder
    update_called = {"v": False}
    def _update_side_effect(*args, **kwargs):
        update_called["v"] = True
        return qb
    qb.update.side_effect = _update_side_effect
    client.table.return_value = qb
    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))

    doc = await mod.get_document_by_id("u1", "jwt", "d1")
    assert doc and doc["id"] == "d1"
    # Ensure update() was invoked to set last_opened_at for owner docs
    assert update_called["v"] is True


@pytest.mark.asyncio
async def test_assert_document_access_raises_forbidden_when_rls_blocks(monkeypatch):
    import importlib
    mod = importlib.import_module("api.services.documents.get_documents")

    auth_client = MagicMock()
    auth_qb = _create_async_query_builder(MockAPIResponse(data=[]))
    auth_client.table.return_value = auth_qb

    service_client = MagicMock()
    service_qb = _create_async_query_builder(MockAPIResponse(data={"id": "d1"}))
    service_client.table.return_value = service_qb

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=auth_client))
    monkeypatch.setattr(mod, "get_async_service_role_client", AsyncMock(return_value=service_client))

    with pytest.raises(HTTPException) as exc_info:
        await mod.assert_document_access("u1", "jwt", "d1")

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_assert_document_access_returns_when_document_visible(monkeypatch):
    import importlib
    mod = importlib.import_module("api.services.documents.get_documents")

    auth_client = MagicMock()
    auth_qb = _create_async_query_builder(MockAPIResponse(data=[{"id": "d1"}]))
    auth_client.table.return_value = auth_qb

    service_client = MagicMock()

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=auth_client))
    monkeypatch.setattr(mod, "get_async_service_role_client", AsyncMock(return_value=service_client))

    await mod.assert_document_access("u1", "jwt", "d1")

    service_client.table.assert_not_called()
