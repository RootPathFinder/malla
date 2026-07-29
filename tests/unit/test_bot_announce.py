"""Tests for LongFast / response-channel bot announcements."""

from unittest.mock import MagicMock, patch

import pytest

from src.malla.services.bot_service import BotMessagePriority, BotService


@pytest.fixture
def bot_service() -> BotService:
    BotService._instance = None
    service = BotService()
    yield service
    BotService._instance = None


@pytest.mark.unit
def test_announce_requires_running_bot(client, bot_service: BotService):
    bot_service._running = False
    with patch("src.malla.routes.bot_routes.get_bot_service", return_value=bot_service):
        resp = client.post("/api/bot/announce", json={"text": "hello mesh"})
    assert resp.status_code == 400
    assert "not running" in resp.get_json()["error"].lower()


@pytest.mark.unit
def test_announce_queues_broadcast_on_respond_channel(client, bot_service: BotService):
    bot_service._running = True
    bot_service._respond_channel_index = 1
    bot_service.queue_message = MagicMock()

    with patch("src.malla.routes.bot_routes.get_bot_service", return_value=bot_service):
        resp = client.post(
            "/api/bot/announce",
            json={"text": "📡 MeshCore bridge is live"},
        )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert body["channel_index"] == 1
    bot_service.queue_message.assert_called_once()
    kwargs = bot_service.queue_message.call_args.kwargs
    assert kwargs["text"] == "📡 MeshCore bridge is live"
    assert kwargs["destination"] == 0xFFFFFFFF
    assert kwargs["channel_index"] == 1
    assert kwargs["priority"] == BotMessagePriority.HIGH


@pytest.mark.unit
def test_announce_allows_channel_override(client, bot_service: BotService):
    bot_service._running = True
    bot_service._respond_channel_index = 1
    bot_service.queue_message = MagicMock()

    with patch("src.malla.routes.bot_routes.get_bot_service", return_value=bot_service):
        resp = client.post(
            "/api/bot/announce",
            json={"text": "override test", "channel_index": 0, "priority": "normal"},
        )

    assert resp.status_code == 200
    assert resp.get_json()["channel_index"] == 0
    kwargs = bot_service.queue_message.call_args.kwargs
    assert kwargs["channel_index"] == 0
    assert kwargs["priority"] == BotMessagePriority.NORMAL


@pytest.mark.unit
def test_announce_rejects_empty_and_too_long(client, bot_service: BotService):
    bot_service._running = True
    with patch("src.malla.routes.bot_routes.get_bot_service", return_value=bot_service):
        empty = client.post("/api/bot/announce", json={"text": "   "})
        long = client.post("/api/bot/announce", json={"text": "x" * 231})
    assert empty.status_code == 400
    assert long.status_code == 400
