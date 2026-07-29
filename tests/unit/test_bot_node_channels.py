"""Tests for GET /api/bot/node-channels."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from malla.routes.bot_routes import api_bot_node_channels


@pytest.fixture
def app():
    from malla.web_ui import create_app

    application = create_app()
    application.config["TESTING"] = True
    return application


def test_node_channels_lists_named_slots(app):
    ch0 = SimpleNamespace(
        index=0,
        settings=SimpleNamespace(name="LongFast"),
    )
    ch4 = SimpleNamespace(
        index=4,
        settings=SimpleNamespace(name="MeshCore"),
    )
    publisher = MagicMock()
    publisher.is_connected = True
    publisher._interface = MagicMock()
    publisher._interface.localNode = SimpleNamespace(channels=[ch0, ch4])

    bot = MagicMock()
    bot._get_publisher.return_value = publisher

    with app.app_context():
        with patch("malla.routes.bot_routes.get_bot_service", return_value=bot):
            resp = api_bot_node_channels()

    data = resp.get_json()
    assert data["count"] == 2
    assert data["channels"] == [
        {"index": 0, "name": "LongFast"},
        {"index": 4, "name": "MeshCore"},
    ]
