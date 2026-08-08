"""Tests for MeshCore-bridge bot command handling via /api/bot/send."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    BotService._instance = None
    service = BotService()
    service._enabled = True
    yield service
    BotService._instance = None


class TestUnwrapBridgeEnvelope:
    def test_plain_text_unchanged(self, bot_service: BotService):
        body, sender, tag = bot_service.unwrap_bridge_envelope("!ping")
        assert body == "!ping"
        assert sender is None
        assert tag is None

    def test_mc_envelope(self, bot_service: BotService):
        body, sender, tag = bot_service.unwrap_bridge_envelope("[MC] alice: !ping")
        assert body == "!ping"
        assert sender == "alice"
        assert tag == "MC"

    def test_mt_envelope(self, bot_service: BotService):
        body, sender, tag = bot_service.unwrap_bridge_envelope("[MT] AB12: hello there")
        assert body == "hello there"
        assert sender == "AB12"
        assert tag == "MT"

    def test_envelope_with_args(self, bot_service: BotService):
        body, sender, tag = bot_service.unwrap_bridge_envelope(
            "[MC] bob: !wx 90210 tonight"
        )
        assert body == "!wx 90210 tonight"
        assert sender == "bob"
        assert tag == "MC"


class TestParseCommandTextBridge:
    def test_prefixed_inside_mc_envelope(self, bot_service: BotService):
        assert bot_service._parse_command_text("[MC] alice: !ping") == ("ping", [])

    def test_bare_inside_mc_envelope(self, bot_service: BotService):
        assert bot_service._parse_command_text("[MC] alice: ping") == ("ping", [])

    def test_chat_inside_mc_envelope_ignored(self, bot_service: BotService):
        assert bot_service._parse_command_text("[MC] alice: hello mesh") is None


class TestHandleBridgedSendText:
    def test_mc_command_queues_reply(self, bot_service: BotService):
        queued: list[str] = []

        def _capture(text, **kwargs):
            queued.append(text)

        bot_service.queue_message = _capture  # type: ignore[method-assign]
        response = bot_service.handle_bridged_send_text(
            "[MC] alice: !ping", channel_index=4
        )
        assert response is not None
        assert "Pong!" in response
        assert "MeshCore bridge" in response
        assert len(queued) == 1
        assert queued[0] == response

    def test_mc_non_command_ignored(self, bot_service: BotService):
        bot_service.queue_message = MagicMock()  # type: ignore[method-assign]
        assert (
            bot_service.handle_bridged_send_text("[MC] alice: just chatting") is None
        )
        bot_service.queue_message.assert_not_called()

    def test_mt_envelope_ignored(self, bot_service: BotService):
        bot_service.queue_message = MagicMock()  # type: ignore[method-assign]
        assert bot_service.handle_bridged_send_text("[MT] AB12: !ping") is None
        bot_service.queue_message.assert_not_called()

    def test_disabled_bot_ignored(self, bot_service: BotService):
        bot_service._enabled = False
        bot_service.queue_message = MagicMock()  # type: ignore[method-assign]
        assert bot_service.handle_bridged_send_text("[MC] alice: !ping") is None
        bot_service.queue_message.assert_not_called()

    def test_disabled_command_ignored(self, bot_service: BotService):
        bot_service._disabled_commands.add("ping")
        bot_service.queue_message = MagicMock()  # type: ignore[method-assign]
        assert bot_service.handle_bridged_send_text("[MC] alice: !ping") is None
        bot_service.queue_message.assert_not_called()


class TestApiBotSendBridgeCommands:
    @pytest.fixture
    def app(self):
        from malla.web_ui import create_app

        application = create_app()
        application.config["TESTING"] = True
        return application

    def test_send_mc_command_handles_and_replies(self, app, bot_service: BotService):
        bot_service._enabled = True
        queued: list[tuple[str, dict]] = []

        def _capture(text, **kwargs):
            queued.append((text, kwargs))

        bot_service.queue_message = _capture  # type: ignore[method-assign]

        with app.test_client() as client:
            with patch(
                "malla.routes.bot_routes.get_bot_service", return_value=bot_service
            ):
                resp = client.post(
                    "/api/bot/send",
                    json={
                        "text": "[MC] alice: !ping",
                        "destination": "broadcast",
                        "channel_index": 4,
                    },
                )

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["success"] is True
        assert data["command_handled"] is True
        assert "Pong!" in data["command_response"]
        # Chat line + command reply
        assert len(queued) == 2
        assert queued[0][0] == "[MC] alice: !ping"
        assert queued[0][1]["channel_index"] == 4
        assert "Pong!" in queued[1][0]
        assert queued[1][1]["channel_index"] == 4

    def test_send_plain_chat_no_command(self, app, bot_service: BotService):
        bot_service._enabled = True
        queued: list[str] = []
        bot_service.queue_message = lambda text, **kwargs: queued.append(text)  # type: ignore[method-assign]

        with app.test_client() as client:
            with patch(
                "malla.routes.bot_routes.get_bot_service", return_value=bot_service
            ):
                resp = client.post(
                    "/api/bot/send",
                    json={"text": "[MC] alice: hi everyone", "channel_index": 4},
                )

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["command_handled"] is False
        assert data["command_response"] is None
        assert queued == ["[MC] alice: hi everyone"]
