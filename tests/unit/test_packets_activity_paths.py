"""Unit tests for live packet activity path enrichment."""

import base64
import time
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask
from meshtastic import mesh_pb2

from src.malla.routes.api_routes import api_bp, api_packets_activity


def _traceroute_payload(route: list[int], snr_towards: list[float]) -> bytes:
    rd = mesh_pb2.RouteDiscovery()
    rd.route.extend(route)
    rd.snr_towards.extend(int(s * 4) for s in snr_towards)
    return rd.SerializeToString()


@pytest.fixture()
def app():
    app = Flask(__name__)
    app.register_blueprint(api_bp, url_prefix="/api")
    return app


@pytest.mark.unit
def test_packets_activity_includes_traceroute_path_nodes(app):
    source = 0x11111111
    dest = 0x22222222
    hop = 0x33333333
    payload = _traceroute_payload([hop], [-5.0, -8.0])

    row = {
        "from_node_id": dest,
        "to_node_id": source,
        "timestamp": time.time(),
        "portnum_name": "TRACEROUTE_APP",
        "hop_limit": 3,
        "hop_start": 5,
        "raw_payload": payload,
    }

    cursor = MagicMock()
    cursor.fetchall.return_value = [row]
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with app.test_request_context("/api/packets/activity?seconds=10"):
        with patch("src.malla.routes.api_routes.get_db_connection", return_value=conn):
            response = api_packets_activity()

    data = response.get_json()
    assert data["count"] == 1
    activity = data["activities"][0]
    assert activity["path_kind"] == "traceroute"
    assert isinstance(activity["path_nodes"], list)
    assert len(activity["path_nodes"]) >= 2
    assert hop in activity["path_nodes"]


@pytest.mark.unit
def test_packets_activity_plain_packets_have_no_path_nodes(app):
    row = {
        "from_node_id": 1,
        "to_node_id": 2,
        "timestamp": time.time(),
        "portnum_name": "TEXT_MESSAGE_APP",
        "hop_limit": 3,
        "hop_start": 5,
        "raw_payload": base64.b64decode("AQID"),
    }

    cursor = MagicMock()
    cursor.fetchall.return_value = [row]
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with app.test_request_context("/api/packets/activity?seconds=10"):
        with patch("src.malla.routes.api_routes.get_db_connection", return_value=conn):
            response = api_packets_activity()

    data = response.get_json()
    assert data["count"] == 1
    assert "path_nodes" not in data["activities"][0]
