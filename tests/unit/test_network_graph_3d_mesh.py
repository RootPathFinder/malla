"""Tests for the MeshCore-style 3D network graph front-end assets."""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
JS_PATH = ROOT / "src" / "malla" / "static" / "js" / "network-graph-3d.js"
TEMPLATE_PATH = ROOT / "src" / "malla" / "templates" / "traceroute_graph.html"


@pytest.mark.unit
def test_network_graph_3d_exposes_mesh_controls_api():
    source = JS_PATH.read_text(encoding="utf-8")

    for symbol in (
        "setLetEmDrift",
        "setRepulsion",
        "setParticleSpeed",
        "shuffleLayout",
        "expandContract",
        "emitPath",
        "packetColor",
        "PACKET_COLORS",
        "PACKET_LEGEND",
        "NODE_LEGEND",
    ):
        assert symbol in source, f"Expected {symbol} in network-graph-3d.js"

    # Orbital ring layout should be gone (MeshCore-style force layout)
    assert "applyOrbitalLayout" not in source
    assert "orbitTier" not in source
    assert "seedForcePositions" in source
    assert "d3AlphaTarget" in source


@pytest.mark.unit
def test_packet_color_map_covers_core_meshtastic_portnums():
    source = JS_PATH.read_text(encoding="utf-8")
    for portnum in (
        "NODEINFO_APP",
        "POSITION_APP",
        "TEXT_MESSAGE_APP",
        "ROUTING_APP",
        "TRACEROUTE_APP",
        "TELEMETRY_APP",
        "NEIGHBORINFO_APP",
        "ADMIN_APP",
    ):
        assert f"{portnum}:" in source or f"{portnum} :" in source


@pytest.mark.unit
def test_traceroute_graph_template_has_mesh_visualizer_controls():
    html = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "3D Mesh" in html
    assert 'id="graphLetEmDrift"' in html
    assert 'id="graphRepulsion"' in html
    assert 'id="graphPacketSpeed"' in html
    assert 'id="graphShuffleLayout"' in html
    assert 'id="graphExpandContract"' in html
    assert 'id="meshVizLegend"' in html
    assert "NetworkGraph3D.emitPath(pathNodes, packetOpts)" in html
    assert "3D Orbital" not in html
    assert "orbital rings by role" not in html
