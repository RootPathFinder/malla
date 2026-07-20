"""Unit tests for opt-in solar weather forecasting."""

import json
import sqlite3
import tempfile
import time
from unittest.mock import patch

import pytest

from malla.solar_weather import (
    build_solar_charge_forecast,
    classify_charge_window,
    get_node_solar_weather_forecast,
    set_solar_forecast_settings,
)


@pytest.fixture
def db_conn():
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()
    conn = sqlite3.connect(temp_db.name)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE node_info (
            node_id INTEGER PRIMARY KEY,
            hex_id TEXT,
            long_name TEXT,
            short_name TEXT,
            power_type TEXT DEFAULT 'solar',
            archived INTEGER DEFAULT 0,
            solar_forecast_enabled INTEGER DEFAULT 0,
            solar_forecast_lat REAL,
            solar_forecast_lon REAL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE solar_weather_cache (
            cache_key TEXT PRIMARY KEY,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            fetched_at REAL NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO node_info (node_id, hex_id, long_name, power_type)
        VALUES (168935425, '!0a11c001', 'Solar Ridge Repeater', 'solar')
        """
    )
    conn.commit()
    yield conn
    conn.close()
    import os

    try:
        os.unlink(temp_db.name)
    except FileNotFoundError:
        pass


def _sample_hourly_payload(
    *,
    day1_rad: float,
    day1_cloud: float,
    day2_rad: float,
    day2_cloud: float,
) -> dict:
    times = []
    radiation = []
    clouds = []
    codes = []
    for day, rad, cloud in [
        ("2026-07-20", day1_rad, day1_cloud),
        ("2026-07-21", day2_rad, day2_cloud),
    ]:
        for hour in range(0, 24):
            times.append(f"{day}T{hour:02d}:00")
            if 8 <= hour <= 17:
                radiation.append(rad)
                clouds.append(cloud)
                codes.append(0 if cloud < 40 else 3)
            else:
                radiation.append(0)
                clouds.append(cloud)
                codes.append(0)
    return {
        "timezone": "America/Los_Angeles",
        "hourly": {
            "time": times,
            "shortwave_radiation": radiation,
            "cloud_cover": clouds,
            "weather_code": codes,
        },
    }


@pytest.mark.unit
def test_classify_charge_window_thresholds():
    assert classify_charge_window(400, 20) == "good"
    assert classify_charge_window(200, 50) == "fair"
    assert classify_charge_window(80, 90) == "poor"
    assert classify_charge_window(None, 20) == "good"
    assert classify_charge_window(None, None) == "unknown"


@pytest.mark.unit
def test_build_solar_charge_forecast_today_tomorrow():
    raw = _sample_hourly_payload(
        day1_rad=400, day1_cloud=20, day2_rad=90, day2_cloud=85
    )
    forecast = build_solar_charge_forecast(raw)
    assert forecast["today"]["condition"] == "good"
    assert forecast["tomorrow"]["condition"] == "poor"
    assert forecast["overall_condition"] == "poor"
    assert "Today:" in forecast["outlook"]
    assert "Tomorrow:" in forecast["outlook"]


@pytest.mark.unit
def test_forecast_requires_opt_in(db_conn):
    result = get_node_solar_weather_forecast(168935425, db_conn)
    assert result["enabled"] is False
    assert result["available"] is False


@pytest.mark.unit
def test_set_settings_and_fetch_with_override(db_conn):
    payload = _sample_hourly_payload(
        day1_rad=120, day1_cloud=80, day2_rad=100, day2_cloud=75
    )

    with patch(
        "malla.solar_weather.fetch_open_meteo_solar_forecast",
        return_value=payload,
    ) as mock_fetch:
        result = set_solar_forecast_settings(
            168935425,
            db_conn,
            enabled=True,
            latitude=37.77,
            longitude=-122.42,
        )
        assert mock_fetch.called
        assert result["enabled"] is True
        assert result["available"] is True
        assert result["location"]["source"] == "override"
        assert result["overall_condition"] == "poor"

        # Second call should hit cache (no new fetch if TTL valid)
        mock_fetch.reset_mock()
        cached = get_node_solar_weather_forecast(168935425, db_conn)
        assert cached["available"] is True
        assert cached["cached"] is True
        assert not mock_fetch.called


@pytest.mark.unit
def test_enabled_without_location_reports_reason(db_conn):
    result = set_solar_forecast_settings(168935425, db_conn, enabled=True)
    assert result["enabled"] is True
    assert result["available"] is False
    assert "location" in (result.get("reason") or "").lower()


@pytest.mark.unit
def test_digest_includes_solar_wx():
    from src.malla.services.bot_service import BotService

    BotService._instance = None
    bot = BotService()
    when = time.strptime("2026-07-20 08:00", "%Y-%m-%d %H:%M")
    message = bot._format_daily_digest(
        vitals={"active_nodes_24h": 10, "packets_24h": 100, "avg_snr": -5.0},
        nodes_delta=0,
        offline_routers=[],
        lowbat_count=0,
        top_names=[],
        when=when,
        solar_at_risk_count=0,
        solar_wx_poor_count=2,
    )
    assert "SolarWx: 2" in message
