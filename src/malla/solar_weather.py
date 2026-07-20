"""
Opt-in solar charge forecasting via Open-Meteo.

Only nodes with solar_forecast_enabled are queried. Location preference:
1. Operator lat/lon override on the node
2. Latest GPS position from POSITION packets
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 3600  # 1 hour
FORECAST_DAYS = 2
DAYTIME_START_HOUR = 8
DAYTIME_END_HOUR = 17
USER_AGENT = "Malla/1.0 (Meshtastic Mesh Health Monitor)"

# Rough daytime shortwave radiation thresholds (W/m² average over daytime hours)
RADIATION_GOOD = 350.0
RADIATION_FAIR = 180.0
# Cloud cover daytime averages (%)
CLOUD_GOOD = 35.0
CLOUD_FAIR = 65.0


def _http_get_json(url: str, timeout: float = 5.0) -> dict[str, Any] | None:
    """Fetch JSON from a URL with a short timeout."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        logger.debug("Solar weather HTTP fetch failed for %s: %s", url, e)
        return None


def ensure_solar_weather_schema(db_connection: Any) -> None:
    """Add opt-in forecast columns and location cache table if missing."""
    cursor = db_connection.cursor()
    columns = [
        ("solar_forecast_enabled", "INTEGER DEFAULT 0"),
        ("solar_forecast_lat", "REAL"),
        ("solar_forecast_lon", "REAL"),
    ]
    for name, col_type in columns:
        try:
            cursor.execute(f"ALTER TABLE node_info ADD COLUMN {name} {col_type}")
        except Exception as e:
            if "duplicate column name" not in str(e).lower():
                logger.debug("Could not add %s: %s", name, e)

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS solar_weather_cache (
            cache_key TEXT PRIMARY KEY,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            fetched_at REAL NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    try:
        db_connection.commit()
    except Exception:
        pass


def _cache_key(latitude: float, longitude: float) -> str:
    # ~1km grid — nearby nodes share one Open-Meteo request
    return f"{round(latitude, 2):.2f},{round(longitude, 2):.2f}"


def resolve_node_forecast_location(
    node_id: int, db_connection: Any
) -> dict[str, Any] | None:
    """
    Resolve forecast coordinates for a node.

    Returns:
        {latitude, longitude, source: 'override'|'gps'} or None
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(
            """
            SELECT solar_forecast_lat, solar_forecast_lon
            FROM node_info WHERE node_id = ?
            """,
            (node_id,),
        )
        row = cursor.fetchone()
    except Exception:
        row = None

    if row:
        lat = row["solar_forecast_lat"] if hasattr(row, "keys") else row[0]
        lon = row["solar_forecast_lon"] if hasattr(row, "keys") else row[1]
        if lat is not None and lon is not None:
            try:
                lat_f, lon_f = float(lat), float(lon)
                if -90 <= lat_f <= 90 and -180 <= lon_f <= 180 and lat_f != 0 and lon_f != 0:
                    return {
                        "latitude": lat_f,
                        "longitude": lon_f,
                        "source": "override",
                    }
            except (TypeError, ValueError):
                pass

    # Fall back to latest GPS from position packets (reuse caller connection)
    try:
        from meshtastic import mesh_pb2

        cursor.execute(
            """
            SELECT timestamp, raw_payload
            FROM packet_history
            WHERE from_node_id = ?
              AND portnum = 3
              AND raw_payload IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (node_id,),
        )
        pos_row = cursor.fetchone()
        if pos_row:
            payload = (
                pos_row["raw_payload"] if hasattr(pos_row, "keys") else pos_row[1]
            )
            position = mesh_pb2.Position()
            position.ParseFromString(payload)
            latitude = position.latitude_i / 1e7 if position.latitude_i else None
            longitude = position.longitude_i / 1e7 if position.longitude_i else None
            if (
                latitude
                and longitude
                and latitude != 0
                and longitude != 0
            ):
                return {
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                    "source": "gps",
                }
    except Exception as e:
        logger.debug("GPS lookup failed for node %s: %s", node_id, e)

    return None


def is_solar_forecast_enabled(node_id: int, db_connection: Any) -> bool:
    try:
        cursor = db_connection.cursor()
        cursor.execute(
            "SELECT COALESCE(solar_forecast_enabled, 0) FROM node_info WHERE node_id = ?",
            (node_id,),
        )
        row = cursor.fetchone()
        if not row:
            return False
        return bool(row[0] if not hasattr(row, "keys") else row[0])
    except Exception:
        return False


def set_solar_forecast_settings(
    node_id: int,
    db_connection: Any,
    *,
    enabled: bool,
    latitude: float | None = None,
    longitude: float | None = None,
    clear_override: bool = False,
) -> dict[str, Any]:
    """Persist opt-in flag and optional location override."""
    ensure_solar_weather_schema(db_connection)
    cursor = db_connection.cursor()

    if clear_override:
        lat_val, lon_val = None, None
    else:
        lat_val = latitude
        lon_val = longitude
        if lat_val is not None or lon_val is not None:
            if lat_val is None or lon_val is None:
                raise ValueError("Both latitude and longitude are required for override")
            lat_val = float(lat_val)
            lon_val = float(lon_val)
            if not (-90 <= lat_val <= 90 and -180 <= lon_val <= 180):
                raise ValueError("Latitude/longitude out of range")

    # If not clearing and both None, leave existing override untouched
    if clear_override:
        cursor.execute(
            """
            UPDATE node_info
            SET solar_forecast_enabled = ?,
                solar_forecast_lat = NULL,
                solar_forecast_lon = NULL
            WHERE node_id = ?
            """,
            (1 if enabled else 0, node_id),
        )
    elif lat_val is not None and lon_val is not None:
        cursor.execute(
            """
            UPDATE node_info
            SET solar_forecast_enabled = ?,
                solar_forecast_lat = ?,
                solar_forecast_lon = ?
            WHERE node_id = ?
            """,
            (1 if enabled else 0, lat_val, lon_val, node_id),
        )
    else:
        cursor.execute(
            """
            UPDATE node_info
            SET solar_forecast_enabled = ?
            WHERE node_id = ?
            """,
            (1 if enabled else 0, node_id),
        )

    if cursor.rowcount == 0:
        raise ValueError(f"Node {node_id} not found")
    db_connection.commit()

    return get_node_solar_weather_forecast(
        node_id, db_connection, force_refresh=bool(enabled)
    ) or {
        "enabled": enabled,
        "available": False,
        "reason": "Forecast enabled but location or weather data unavailable",
    }


def fetch_open_meteo_solar_forecast(
    latitude: float, longitude: float
) -> dict[str, Any] | None:
    """Fetch hourly shortwave radiation + cloud cover from Open-Meteo."""
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}&longitude={longitude}"
        "&hourly=shortwave_radiation,cloud_cover,weather_code"
        f"&forecast_days={FORECAST_DAYS}"
        "&timezone=auto"
    )
    data = _http_get_json(url)
    if not data or "hourly" not in data:
        return None
    return data


def _day_key_from_iso(iso_ts: str) -> str:
    # "2026-07-20T14:00" -> "2026-07-20"
    return iso_ts[:10]


def _hour_from_iso(iso_ts: str) -> int | None:
    try:
        # Local timezone already applied by Open-Meteo (timezone=auto)
        return int(iso_ts[11:13])
    except Exception:
        return None


def classify_charge_window(
    avg_radiation: float | None, avg_cloud: float | None
) -> str:
    """Classify a daytime charge window as good / fair / poor / unknown."""
    if avg_radiation is None and avg_cloud is None:
        return "unknown"
    if avg_radiation is not None:
        if avg_radiation >= RADIATION_GOOD and (
            avg_cloud is None or avg_cloud <= CLOUD_FAIR
        ):
            return "good"
        if avg_radiation >= RADIATION_FAIR:
            return "fair"
        return "poor"
    # Cloud-only fallback
    assert avg_cloud is not None
    if avg_cloud <= CLOUD_GOOD:
        return "good"
    if avg_cloud <= CLOUD_FAIR:
        return "fair"
    return "poor"


def build_solar_charge_forecast(raw: dict[str, Any]) -> dict[str, Any]:
    """Turn Open-Meteo hourly payload into today/tomorrow charge outlook."""
    hourly = raw.get("hourly") or {}
    times = hourly.get("time") or []
    radiation = hourly.get("shortwave_radiation") or []
    clouds = hourly.get("cloud_cover") or []
    codes = hourly.get("weather_code") or []

    by_day: dict[str, dict[str, list[float]]] = {}
    for idx, ts in enumerate(times):
        hour = _hour_from_iso(ts)
        if hour is None or hour < DAYTIME_START_HOUR or hour > DAYTIME_END_HOUR:
            continue
        day = _day_key_from_iso(ts)
        bucket = by_day.setdefault(day, {"radiation": [], "cloud": [], "codes": []})
        if idx < len(radiation) and radiation[idx] is not None:
            bucket["radiation"].append(float(radiation[idx]))
        if idx < len(clouds) and clouds[idx] is not None:
            bucket["cloud"].append(float(clouds[idx]))
        if idx < len(codes) and codes[idx] is not None:
            bucket["codes"].append(float(codes[idx]))

    # Determine "today" in the forecast timezone from first timestamp date if possible
    today_key = None
    if times:
        today_key = _day_key_from_iso(times[0])
    # Prefer calendar today in UTC as secondary — Open-Meteo local "today" is first day
    days_sorted = sorted(by_day.keys())
    day_summaries: list[dict[str, Any]] = []
    for day in days_sorted:
        bucket = by_day[day]
        avg_rad = (
            sum(bucket["radiation"]) / len(bucket["radiation"])
            if bucket["radiation"]
            else None
        )
        avg_cloud = (
            sum(bucket["cloud"]) / len(bucket["cloud"]) if bucket["cloud"] else None
        )
        condition = classify_charge_window(avg_rad, avg_cloud)
        day_summaries.append(
            {
                "date": day,
                "condition": condition,
                "condition_label": {
                    "good": "Good",
                    "fair": "Fair",
                    "poor": "Poor",
                    "unknown": "Unknown",
                }.get(condition, condition),
                "avg_shortwave_radiation": round(avg_rad, 1) if avg_rad is not None else None,
                "avg_cloud_cover": round(avg_cloud, 1) if avg_cloud is not None else None,
                "daytime_samples": max(len(bucket["radiation"]), len(bucket["cloud"])),
            }
        )

    today = day_summaries[0] if day_summaries else None
    tomorrow = day_summaries[1] if len(day_summaries) > 1 else None

    def _outlook_line(label: str, day: dict[str, Any] | None) -> str | None:
        if not day:
            return None
        cloud = day.get("avg_cloud_cover")
        rad = day.get("avg_shortwave_radiation")
        parts = [f"{label}: {day['condition_label']} charge window"]
        detail = []
        if cloud is not None:
            detail.append(f"cloud {cloud:.0f}%")
        if rad is not None:
            detail.append(f"irradiance {rad:.0f} W/m²")
        if detail:
            parts.append(f"({', '.join(detail)})")
        return " ".join(parts)

    outlook_parts = [
        p
        for p in (
            _outlook_line("Today", today),
            _outlook_line("Tomorrow", tomorrow),
        )
        if p
    ]
    outlook = " · ".join(outlook_parts) if outlook_parts else "No daytime forecast available"

    # Overall advisory for digests: worst of today/tomorrow among known
    severity_order = {"poor": 0, "fair": 1, "good": 2, "unknown": 3}
    overall = "unknown"
    known = [d["condition"] for d in day_summaries if d["condition"] != "unknown"]
    if known:
        overall = sorted(known, key=lambda c: severity_order.get(c, 9))[0]

    return {
        "timezone": raw.get("timezone"),
        "today": today,
        "tomorrow": tomorrow,
        "days": day_summaries,
        "overall_condition": overall,
        "overall_label": {
            "good": "Good",
            "fair": "Fair",
            "poor": "Poor",
            "unknown": "Unknown",
        }.get(overall, overall),
        "outlook": outlook,
        "today_key": today_key,
    }


def _read_cache(
    db_connection: Any, key: str
) -> dict[str, Any] | None:
    try:
        cursor = db_connection.cursor()
        cursor.execute(
            """
            SELECT fetched_at, payload_json FROM solar_weather_cache
            WHERE cache_key = ?
            """,
            (key,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        fetched_at = row["fetched_at"] if hasattr(row, "keys") else row[0]
        payload_json = row["payload_json"] if hasattr(row, "keys") else row[1]
        if time.time() - float(fetched_at) > CACHE_TTL_SECONDS:
            return None
        return json.loads(payload_json)
    except Exception as e:
        logger.debug("Solar weather cache read failed: %s", e)
        return None


def _write_cache(
    db_connection: Any,
    key: str,
    latitude: float,
    longitude: float,
    payload: dict[str, Any],
) -> None:
    try:
        cursor = db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO solar_weather_cache (cache_key, latitude, longitude, fetched_at, payload_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                latitude=excluded.latitude,
                longitude=excluded.longitude,
                fetched_at=excluded.fetched_at,
                payload_json=excluded.payload_json
            """,
            (key, latitude, longitude, time.time(), json.dumps(payload)),
        )
        db_connection.commit()
    except Exception as e:
        logger.debug("Solar weather cache write failed: %s", e)


def get_node_solar_weather_forecast(
    node_id: int,
    db_connection: Any,
    *,
    force_refresh: bool = False,
) -> dict[str, Any] | None:
    """
    Return solar weather forecast for an opted-in node, or a status dict explaining why not.
    """
    ensure_solar_weather_schema(db_connection)

    enabled = is_solar_forecast_enabled(node_id, db_connection)
    location = resolve_node_forecast_location(node_id, db_connection)

    base = {
        "enabled": enabled,
        "node_id": node_id,
        "location": location,
        "available": False,
        "fetched_at": None,
        "cached": False,
    }

    if not enabled:
        return {
            **base,
            "reason": "Solar weather forecast not enabled for this node",
        }

    if not location:
        return {
            **base,
            "reason": "No location — set a lat/lon override or wait for a GPS position",
        }

    lat = location["latitude"]
    lon = location["longitude"]
    key = _cache_key(lat, lon)

    raw = None if force_refresh else _read_cache(db_connection, key)
    cached = raw is not None
    if raw is None:
        raw = fetch_open_meteo_solar_forecast(lat, lon)
        if raw is None:
            return {
                **base,
                "reason": "Open-Meteo forecast unavailable",
            }
        _write_cache(db_connection, key, lat, lon, raw)
        cached = False

    forecast = build_solar_charge_forecast(raw)
    return {
        **base,
        "available": True,
        "cached": cached,
        "fetched_at": time.time() if not cached else None,
        "forecast": forecast,
        "outlook": forecast.get("outlook"),
        "overall_condition": forecast.get("overall_condition"),
        "overall_label": forecast.get("overall_label"),
        "reason": None,
    }


def get_opted_in_solar_forecasts(db_connection: Any) -> list[dict[str, Any]]:
    """Forecasts for all non-archived nodes with solar_forecast_enabled=1."""
    ensure_solar_weather_schema(db_connection)
    cursor = db_connection.cursor()
    try:
        cursor.execute(
            """
            SELECT node_id, hex_id, long_name, short_name, power_type
            FROM node_info
            WHERE COALESCE(solar_forecast_enabled, 0) = 1
              AND COALESCE(archived, 0) = 0
            ORDER BY long_name COLLATE NOCASE, hex_id
            """
        )
        rows = cursor.fetchall()
    except Exception as e:
        logger.warning("Failed listing opted-in solar forecast nodes: %s", e)
        return []

    results = []
    for row in rows:
        node_id = row["node_id"] if hasattr(row, "keys") else row[0]
        hex_id = row["hex_id"] if hasattr(row, "keys") else row[1]
        long_name = row["long_name"] if hasattr(row, "keys") else row[2]
        short_name = row["short_name"] if hasattr(row, "keys") else row[3]
        power_type = row["power_type"] if hasattr(row, "keys") else row[4]
        name = long_name or short_name or hex_id or f"!{int(node_id):08x}"
        wx = get_node_solar_weather_forecast(node_id, db_connection) or {}
        results.append(
            {
                "node_id": node_id,
                "hex_id": hex_id,
                "name": name,
                "short_name": short_name,
                "power_type": power_type,
                "solar_weather": wx,
                "outlook": wx.get("outlook"),
                "overall_condition": wx.get("overall_condition"),
                "available": bool(wx.get("available")),
            }
        )
    return results
