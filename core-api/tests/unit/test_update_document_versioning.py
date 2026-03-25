import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException

from tests.conftest import MockAPIResponse, _create_async_query_builder


@pytest.mark.asyncio
async def test_update_document_queues_snapshot_for_note_content_change(monkeypatch):
    """Snapshot task is queued when a note's content changes by >= MIN_DIFF_CHARS."""
    import importlib

    mod = importlib.import_module("api.services.documents.update_document")

    client = MagicMock()
    documents_qb = _create_async_query_builder()
    documents_qb.execute = AsyncMock(
        side_effect=[
            MockAPIResponse(data=[{
                "title": "Doc",
                "content": "old content",
                "type": "note",
                "updated_at": "2026-02-19T10:00:00+00:00",
            }]),
            MockAPIResponse(data=[{"id": "doc-1"}]),
            MockAPIResponse(data=[{"id": "doc-1", "content": "new content that is significantly longer in order to comfortably exceed the minimum 100 character diff threshold required for automatic versioning snapshots"}]),
        ]
    )
    client.table.side_effect = lambda table: documents_qb

    create_task_mock = MagicMock()

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(mod.asyncio, "create_task", create_task_mock)

    result = await mod.update_document(
        user_id="user-1",
        user_jwt="jwt",
        document_id="doc-1",
        content="new content that is significantly longer in order to comfortably exceed the minimum 100 character diff threshold required for automatic versioning snapshots",
        expected_updated_at="2026-02-19T10:00:00+00:00",
    )

    assert result["id"] == "doc-1"
    assert create_task_mock.call_count == 1

    eq_calls = [tuple(call.args) for call in documents_qb.eq.call_args_list]
    assert ("updated_at", "2026-02-19T10:00:00+00:00") in eq_calls


@pytest.mark.asyncio
async def test_update_document_skips_snapshot_for_small_diff(monkeypatch):
    """Snapshot task is NOT queued when content change is below MIN_DIFF_CHARS."""
    import importlib

    mod = importlib.import_module("api.services.documents.update_document")

    client = MagicMock()
    documents_qb = _create_async_query_builder()
    documents_qb.execute = AsyncMock(
        side_effect=[
            MockAPIResponse(data=[{
                "title": "Doc",
                "content": "old content here",
                "type": "note",
                "updated_at": "2026-02-19T10:00:00+00:00",
            }]),
            MockAPIResponse(data=[{"id": "doc-1b"}]),
            MockAPIResponse(data=[{"id": "doc-1b", "content": "old content here!"}]),
        ]
    )
    client.table.side_effect = lambda table: documents_qb

    create_task_mock = MagicMock()

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(mod.asyncio, "create_task", create_task_mock)

    await mod.update_document(
        user_id="user-1",
        user_jwt="jwt",
        document_id="doc-1b",
        content="old content here!",
    )

    # Diff is only 1 char — below the 100-char threshold
    assert create_task_mock.call_count == 0


@pytest.mark.asyncio
async def test_update_document_skips_snapshot_for_file_docs(monkeypatch):
    import importlib

    mod = importlib.import_module("api.services.documents.update_document")

    client = MagicMock()
    documents_qb = _create_async_query_builder()
    documents_qb.execute = AsyncMock(
        side_effect=[
            MockAPIResponse(data=[{
                "title": "File Wrapper",
                "content": "old",
                "type": "file",
                "updated_at": "2026-02-19T10:00:00+00:00",
            }]),
            MockAPIResponse(data=[{"id": "doc-2"}]),
            MockAPIResponse(data=[{"id": "doc-2", "content": "new"}]),
        ]
    )
    client.table.side_effect = lambda table: documents_qb

    create_task_mock = MagicMock()

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(mod.asyncio, "create_task", create_task_mock)

    await mod.update_document(
        user_id="user-1",
        user_jwt="jwt",
        document_id="doc-2",
        content="new",
    )

    assert create_task_mock.call_count == 0


@pytest.mark.asyncio
async def test_update_document_returns_conflict_on_stale_write(monkeypatch):
    import importlib

    mod = importlib.import_module("api.services.documents.update_document")

    client = MagicMock()
    documents_qb = _create_async_query_builder()
    documents_qb.execute = AsyncMock(
        side_effect=[
            MockAPIResponse(data=[{
                "title": "Doc",
                "content": "old content",
                "type": "note",
                "updated_at": "2026-02-19T10:00:00+00:00",
            }]),
            MockAPIResponse(data=[]),
            MockAPIResponse(data=[{"id": "doc-3"}]),
        ]
    )
    client.table.side_effect = lambda table: documents_qb

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))

    with pytest.raises(HTTPException) as exc_info:
        await mod.update_document(
            user_id="user-1",
            user_jwt="jwt",
            document_id="doc-3",
            content="new content",
            expected_updated_at="2026-02-19T10:00:00+00:00",
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_should_snapshot_allows_first_save():
    """First save (old_content empty) is now captured as Version 1."""
    import importlib

    mod = importlib.import_module("api.services.documents.update_document")

    # Old content is empty, new content is substantial
    long_content = "a" * 150
    assert mod._should_snapshot("note", "", long_content, force=False) is True

    # Old content is empty, new content is SHORT (under 100 chars) — still captured
    assert mod._should_snapshot("note", "", "hello world", force=False) is True

    # Old content is whitespace-only, new content is substantial
    assert mod._should_snapshot("note", "   ", long_content, force=False) is True


@pytest.mark.asyncio
async def test_should_snapshot_force_allows_none_content():
    """force=True bypasses the new_content None gate but still requires note type."""
    import importlib

    mod = importlib.import_module("api.services.documents.update_document")

    assert mod._should_snapshot("note", "hello", None, force=True) is True
    assert mod._should_snapshot("file", "hello", None, force=True) is False


@pytest.mark.asyncio
async def test_update_document_force_snapshot_queues_task(monkeypatch):
    """force_snapshot=True on update_document always queues snapshot task."""
    import importlib

    mod = importlib.import_module("api.services.documents.update_document")

    client = MagicMock()
    documents_qb = _create_async_query_builder()
    documents_qb.execute = AsyncMock(
        side_effect=[
            MockAPIResponse(data=[{
                "title": "Doc",
                "content": "original content",
                "type": "note",
                "updated_at": "2026-02-19T10:00:00+00:00",
            }]),
            MockAPIResponse(data=[{"id": "doc-force"}]),
            MockAPIResponse(data=[{"id": "doc-force", "content": "restored content"}]),
        ]
    )
    client.table.side_effect = lambda table: documents_qb

    create_task_mock = MagicMock()

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(mod.asyncio, "create_task", create_task_mock)

    await mod.update_document(
        user_id="user-1",
        user_jwt="jwt",
        document_id="doc-force",
        content="restored content",
        force_snapshot=True,
    )

    # force_snapshot bypasses diff check even though diff may be small
    assert create_task_mock.call_count == 1


@pytest.mark.asyncio
async def test_update_document_queues_file_edit_task_for_note_title_change(monkeypatch):
    """Title-only note edits still queue the async file-edit side effects."""
    import importlib

    mod = importlib.import_module("api.services.documents.update_document")

    client = MagicMock()
    documents_qb = _create_async_query_builder()
    documents_qb.execute = AsyncMock(
        side_effect=[
            MockAPIResponse(data=[{
                "title": "Old title",
                "content": "unchanged content",
                "type": "note",
                "updated_at": "2026-02-19T10:00:00+00:00",
                "workspace_id": "ws-1",
                "user_id": "owner-1",
                "file_id": None,
            }]),
            MockAPIResponse(data=[{"id": "doc-title"}]),
            MockAPIResponse(data=[{"id": "doc-title", "title": "New title", "content": "unchanged content"}]),
        ]
    )
    client.table.side_effect = lambda table: documents_qb

    create_task_mock = MagicMock()

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(mod.asyncio, "create_task", create_task_mock)

    await mod.update_document(
        user_id="user-1",
        user_jwt="jwt",
        document_id="doc-title",
        title="New title",
    )

    assert create_task_mock.call_count == 1
