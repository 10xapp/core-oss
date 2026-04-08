import pytest
from unittest.mock import AsyncMock, MagicMock

from tests.conftest import MockAPIResponse, _create_async_query_builder


@pytest.mark.asyncio
async def test_legacy_enrichment_skips_text_only_messages(monkeypatch):
    from api.services.messages import messages as mod

    monkeypatch.setattr(
        mod,
        "get_async_service_role_client",
        AsyncMock(side_effect=AssertionError("service role client should not be used")),
    )
    monkeypatch.setattr(
        mod,
        "get_r2_client",
        MagicMock(side_effect=AssertionError("R2 client should not be initialized")),
    )

    messages = [{
        "id": "msg-1",
        "channel_id": "channel-1",
        "blocks": [{"type": "text", "data": {"content": "hello"}}],
    }]

    await mod._enrich_messages_with_file_urls_legacy(messages, "jwt")


@pytest.mark.asyncio
async def test_get_message_skips_r2_for_text_only_messages(monkeypatch):
    from api.services.messages import messages as mod

    message = {
        "id": "msg-1",
        "channel_id": "channel-1",
        "user_id": "user-1",
        "blocks": [{"type": "text", "data": {"content": "hello"}}],
    }

    client = MagicMock()
    query_builder = _create_async_query_builder(MockAPIResponse(data=[message]))
    client.table.return_value = query_builder

    monkeypatch.setattr(mod, "get_authenticated_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(mod, "attach_public_profiles", AsyncMock())
    monkeypatch.setattr(mod.settings, "image_proxy_url", "")
    monkeypatch.setattr(mod.settings, "image_proxy_secret", "")
    monkeypatch.setattr(
        mod,
        "get_async_service_role_client",
        AsyncMock(side_effect=AssertionError("service role client should not be used")),
    )
    monkeypatch.setattr(
        mod,
        "get_r2_client",
        MagicMock(side_effect=AssertionError("R2 client should not be initialized")),
    )

    result = await mod.get_message("msg-1", "jwt")

    assert result is not None
    assert result["id"] == "msg-1"
