"""Tests for mesh bot command text parsing (prefix and bare single-word)."""

import pytest

from src.malla.services.bot_service import BotService


@pytest.fixture
def bot_service() -> BotService:
    BotService._instance = None
    service = BotService()
    yield service
    BotService._instance = None


class TestParseCommandText:
    def test_prefixed_command_no_args(self, bot_service: BotService):
        assert bot_service._parse_command_text("!ping") == ("ping", [])

    def test_prefixed_command_with_args(self, bot_service: BotService):
        assert bot_service._parse_command_text("!wx 90210") == ("wx", ["90210"])

    def test_prefixed_command_case_insensitive(self, bot_service: BotService):
        assert bot_service._parse_command_text("!PING") == ("ping", [])
        assert bot_service._parse_command_text("!Help") == ("help", [])

    def test_prefixed_strips_whitespace(self, bot_service: BotService):
        assert bot_service._parse_command_text("  !ping  ") == ("ping", [])
        assert bot_service._parse_command_text("!wx   90210  extra") == (
            "wx",
            ["90210", "extra"],
        )

    def test_bare_single_word_command(self, bot_service: BotService):
        assert bot_service._parse_command_text("ping") == ("ping", [])
        assert bot_service._parse_command_text("PING") == ("ping", [])
        assert bot_service._parse_command_text("  help  ") == ("help", [])

    def test_bare_multi_word_rejected(self, bot_service: BotService):
        assert bot_service._parse_command_text("wx 90210") is None
        assert bot_service._parse_command_text("ping please") is None

    def test_bare_unknown_word_rejected(self, bot_service: BotService):
        assert bot_service._parse_command_text("hello") is None
        assert bot_service._parse_command_text("notacommand") is None

    def test_empty_and_prefix_only_rejected(self, bot_service: BotService):
        assert bot_service._parse_command_text("") is None
        assert bot_service._parse_command_text("   ") is None
        assert bot_service._parse_command_text("!") is None
        assert bot_service._parse_command_text("!   ") is None

    def test_custom_prefix(self, bot_service: BotService):
        bot_service._command_prefix = "?"
        assert bot_service._parse_command_text("?ping") == ("ping", [])
        assert bot_service._parse_command_text("!ping") is None
        # Bare form still works regardless of prefix
        assert bot_service._parse_command_text("ping") == ("ping", [])

    def test_bare_matches_registered_even_if_disabled(self, bot_service: BotService):
        # Parsing succeeds; disable check happens later in the receive handler.
        assert "uptime" in bot_service._commands
        bot_service._disabled_commands.add("uptime")
        assert bot_service._parse_command_text("uptime") == ("uptime", [])
