"""Regression checks for Mesh Bot admin UI config wiring."""

from pathlib import Path

import pytest

MESH_ADMIN = Path(__file__).resolve().parents[2] / "src/malla/templates/mesh_admin.html"


@pytest.mark.unit
def test_traceroute_format_is_marked_dirty_on_change():
    """Status polling must not wipe an unsaved traceroute format selection."""
    html = MESH_ADMIN.read_text(encoding="utf-8")
    assert 'id="botTracerouteFormat"' in html
    assert "markBotConfigDirty('botTracerouteFormat')" in html
    assert "setIfBlurred('botTracerouteFormat'" in html


@pytest.mark.unit
def test_bot_config_fields_mark_dirty_before_save():
    html = MESH_ADMIN.read_text(encoding="utf-8")
    for field_id in (
        "botCommandPrefix",
        "botMinSendInterval",
        "botListenChannels",
        "botRespondChannelIndex",
        "botWaitForJobs",
        "botTracerouteFormat",
        "botWelcomeNewNodes",
        "botDailyDigestEnabled",
        "botDailyDigestHours",
        "botDailyDigestTimezone",
        "botDailyWxEnabled",
        "botDailyWxHours",
        "botDailyWxTimezone",
        "botDailyWxZip",
        "botChannelBroadcastEnabled",
        "botBroadcastIntervalHours",
        "botNwsAlertEnabled",
        "botNwsAlertZip",
        "botNwsAlertInterval",
    ):
        assert f"markBotConfigDirty('{field_id}')" in html, field_id


@pytest.mark.unit
def test_bot_config_uses_accordion_and_run_notices():
    html = MESH_ADMIN.read_text(encoding="utf-8")
    assert 'id="botConfigAccordion"' in html
    assert "Run Notices Now" in html
    assert "runBotNotice('daily_digest'" in html
    assert "runBotNotice('daily_wx'" in html
    assert "runBotNotice('channel_directory'" in html
    assert "runBotNotice('nws_alerts'" in html
    assert 'id="botDailyDigestHours"' in html
    assert 'id="botDailyWxHours"' in html
    assert "/api/bot/notices/" in html
    assert "daily_digest_hours" in html
    assert "daily_wx_hours" in html


@pytest.mark.unit
def test_save_bot_config_applies_traceroute_format_from_response():
    html = MESH_ADMIN.read_text(encoding="utf-8")
    assert "botTracerouteFormat" in html
    assert "traceroute_format" in html
    # Saved format is written back from the PUT response config payload.
    assert "data.config.traceroute_format" in html
