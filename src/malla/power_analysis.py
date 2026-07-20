"""
Unified power analysis for mesh nodes.

Single source of truth for:
- Power source classification (solar / battery / mains)
- Charge state and runtime outlook
- Solar battery/charger degradation signals
- Persisted node_info updates
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

CRITICAL_VOLTAGE = 3.2
WARNING_VOLTAGE = 3.4
FULL_CHARGE_VOLTAGE = 4.10
NEAR_FULL_BATTERY_PCT = 95
DAY_HOURS = range(6, 18)
MORNING_HOURS = range(6, 11)
AFTERNOON_HOURS = range(14, 19)

# Health/monitoring only includes nodes with telemetry newer than this.
# 48h excludes abandoned/stale nodes while still covering sparse solar reporters.
RECENT_TELEMETRY_MAX_AGE_HOURS = 48
RECENT_TELEMETRY_MAX_AGE_SECONDS = RECENT_TELEMETRY_MAX_AGE_HOURS * 3600


def recent_telemetry_cutoff(now: float | None = None) -> float:
    """Unix timestamp cutoff for "recent enough" telemetry in health/monitoring."""
    return (now if now is not None else time.time()) - RECENT_TELEMETRY_MAX_AGE_SECONDS


def get_latest_telemetry_timestamp(
    db_connection: Any, node_id: int
) -> float | None:
    """Return the newest battery/voltage telemetry timestamp for a node, if any."""
    cursor = db_connection.cursor()
    cursor.execute(
        """
        SELECT MAX(timestamp) as last_ts
        FROM telemetry_data
        WHERE node_id = ?
          AND (voltage IS NOT NULL OR battery_level IS NOT NULL)
        """,
        (node_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    last_ts = row["last_ts"] if hasattr(row, "keys") else row[0]
    return float(last_ts) if last_ts is not None else None


def has_recent_telemetry(
    db_connection: Any,
    node_id: int,
    *,
    max_age_seconds: int = RECENT_TELEMETRY_MAX_AGE_SECONDS,
    now: float | None = None,
) -> bool:
    """True when the node has battery/voltage telemetry within max_age_seconds."""
    last_ts = get_latest_telemetry_timestamp(db_connection, node_id)
    if last_ts is None:
        return False
    cutoff = (now if now is not None else time.time()) - max_age_seconds
    return last_ts >= cutoff


def normalize_voltage(voltage: float | None) -> float | None:
    """Normalize telemetry voltage to volts (handles mV-as-fraction mistakes)."""
    if voltage is None:
        return None
    try:
        value = float(voltage)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    # Common bad encoding: millivolts stored as 0.0039..0.0043
    if value < 1.0:
        value *= 1000.0
    # Occasional raw millivolts
    if value > 20:
        value /= 1000.0
    if value < 2.0 or value > 5.5:
        return None
    return value


def _utc_hour(ts: float) -> int:
    return datetime.fromtimestamp(ts, tz=UTC).hour


def _utc_day_key(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")


def _fetch_telemetry_rows(
    db_connection: Any, node_id: int, days: int = 7
) -> list[dict[str, Any]]:
    cursor = db_connection.cursor()
    cutoff = time.time() - (days * 24 * 3600)
    cursor.execute(
        """
        SELECT timestamp, voltage, battery_level
        FROM telemetry_data
        WHERE node_id = ?
          AND timestamp > ?
          AND (voltage IS NOT NULL OR battery_level IS NOT NULL)
        ORDER BY timestamp ASC
        """,
        (node_id, cutoff),
    )
    rows = cursor.fetchall()
    # Row factory may be sqlite3.Row or tuple
    result: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "keys"):
            result.append(
                {
                    "timestamp": float(row["timestamp"]),
                    "voltage": row["voltage"],
                    "battery_level": row["battery_level"],
                }
            )
        else:
            result.append(
                {
                    "timestamp": float(row[0]),
                    "voltage": row[1],
                    "battery_level": row[2],
                }
            )
    return result


def _prepare_series(
    rows: list[dict[str, Any]],
) -> tuple[list[float], list[float | None], list[int | None]]:
    timestamps: list[float] = []
    voltages: list[float | None] = []
    batteries: list[int | None] = []
    for row in rows:
        timestamps.append(float(row["timestamp"]))
        voltages.append(normalize_voltage(row.get("voltage")))
        level = row.get("battery_level")
        try:
            batteries.append(int(level) if level is not None else None)
        except (TypeError, ValueError):
            batteries.append(None)
    return timestamps, voltages, batteries


def classify_power_source(
    timestamps: list[float],
    voltages: list[float | None],
    battery_levels: list[int | None],
) -> tuple[str, str, float]:
    """
    Classify power source.

    Returns:
        (power_type, reason, confidence 0..1)
    """
    valid_levels = [b for b in battery_levels if b is not None]
    valid_voltages = [v for v in voltages if v is not None]

    if len(timestamps) < 5 and len(valid_voltages) < 5:
        return (
            "unknown",
            f"Insufficient telemetry data ({len(timestamps)} samples)",
            0.2,
        )

    # Fast path: Meshtastic USB/mains marker
    if valid_levels:
        count_101 = sum(1 for b in valid_levels if b == 101)
        if count_101 / len(valid_levels) >= 0.5:
            return (
                "mains",
                f"USB/mains marker (battery_level=101 in {count_101}/{len(valid_levels)} readings)",
                0.95,
            )
        if sum(1 for b in valid_levels if b == 100) / len(valid_levels) > 0.95:
            return "mains", "Constant 100% battery (powered)", 0.9

    # Battery % cycling strongly indicates solar charging
    if len(valid_levels) >= 5:
        bat_min = min(valid_levels)
        bat_max = max(valid_levels)
        bat_range = bat_max - bat_min
        if bat_max >= 98 and bat_range > 10:
            return (
                "solar",
                f"Battery cycling {bat_min}-{bat_max}% indicates solar charging",
                0.9,
            )

        # Daytime charging curve even when never hitting 98%
        hourly: dict[int, list[int]] = {}
        for ts, level in zip(timestamps, battery_levels, strict=False):
            if level is None or level >= 101:
                continue
            hourly.setdefault(_utc_hour(ts), []).append(level)
        hourly_avg = {h: sum(v) / len(v) for h, v in hourly.items()}
        morning = [hourly_avg[h] for h in MORNING_HOURS if h in hourly_avg]
        afternoon = [hourly_avg[h] for h in AFTERNOON_HOURS if h in hourly_avg]
        if len(morning) >= 2 and len(afternoon) >= 2:
            morning_avg = sum(morning) / len(morning)
            afternoon_avg = sum(afternoon) / len(afternoon)
            if afternoon_avg > morning_avg + 1.0:
                return (
                    "solar",
                    f"Daytime charging pattern ({morning_avg:.0f}% AM → {afternoon_avg:.0f}% PM UTC)",
                    0.8,
                )

    # Voltage day/night pattern (UTC)
    if len(valid_voltages) >= 10:
        hourly_v: dict[int, list[float]] = {}
        for ts, voltage in zip(timestamps, voltages, strict=False):
            if voltage is None:
                continue
            hourly_v.setdefault(_utc_hour(ts), []).append(voltage)
        if len(hourly_v) >= 8:
            day = [sum(hourly_v[h]) / len(hourly_v[h]) for h in DAY_HOURS if h in hourly_v]
            night = [
                sum(hourly_v[h]) / len(hourly_v[h])
                for h in list(range(0, 6)) + list(range(18, 24))
                if h in hourly_v
            ]
            if day and night:
                day_avg = sum(day) / len(day)
                night_avg = sum(night) / len(night)
                if day_avg > night_avg + 0.12:
                    return (
                        "solar",
                        f"Daytime voltage ({day_avg:.2f}V) higher than nighttime ({night_avg:.2f}V)",
                        0.75,
                    )

        v_min = min(valid_voltages)
        v_max = max(valid_voltages)
        v_avg = sum(valid_voltages) / len(valid_voltages)
        v_range = v_max - v_min

        if v_range < 0.1 and v_avg >= 4.0:
            return (
                "mains",
                f"Voltage stable (avg {v_avg:.2f}V, range {v_range:.2f}V)",
                0.85,
            )

        # Linear trend for battery-only decline
        # Align voltages with their timestamps
        paired = [
            (ts, v)
            for ts, v in zip(timestamps, voltages, strict=False)
            if v is not None
        ]
        paired.sort(key=lambda item: item[0])
        ys = [v for _, v in paired]
        xs = list(range(len(ys)))
        if len(ys) >= 5:
            x_mean = sum(xs) / len(xs)
            y_mean = sum(ys) / len(ys)
            denom = sum((x - x_mean) ** 2 for x in xs)
            slope = (
                sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=False))
                / denom
                if denom
                else 0.0
            )
            # Negative slope across samples + meaningful range → battery
            if slope < -0.002 and v_range > 0.15:
                return (
                    "battery",
                    f"Declining voltage trend ({v_max:.2f}→{v_min:.2f}V)",
                    0.7,
                )

        if valid_levels and max(valid_levels) < 98 and v_avg < 4.05:
            return (
                "battery",
                f"No recharge observed (max battery {max(valid_levels)}%, avg {v_avg:.2f}V)",
                0.55,
            )

        if v_min >= 4.0 and v_range < 0.2:
            return "mains", f"High stable voltage ({v_min:.2f}V+)", 0.65

    if len(valid_levels) >= 8 and max(valid_levels) < 98:
        return (
            "battery",
            f"Battery discharging without full recharge (max {max(valid_levels)}%)",
            0.5,
        )

    return "unknown", "No clear solar/battery/mains pattern yet", 0.3


def _recent_discharge_rate_vph(
    timestamps: list[float], voltages: list[float | None], hours: float = 48.0
) -> float | None:
    cutoff = time.time() - hours * 3600
    paired = [
        (ts, v)
        for ts, v in zip(timestamps, voltages, strict=False)
        if v is not None and ts >= cutoff
    ]
    if len(paired) < 5:
        return None
    paired.sort(key=lambda item: item[0])
    span_h = (paired[-1][0] - paired[0][0]) / 3600.0
    if span_h < 1.0:
        return None
    drop = paired[0][1] - paired[-1][1]
    if drop <= 0:
        return None
    return drop / span_h


def predict_hours_to_critical(
    timestamps: list[float],
    voltages: list[float | None],
    *,
    critical_voltage: float = CRITICAL_VOLTAGE,
) -> float | None:
    """Estimate hours until voltage hits critical, or None if not discharging."""
    valid = [v for v in voltages if v is not None]
    if not valid:
        return None
    current = valid[-1]
    if current > 4.15:
        return None
    if current <= critical_voltage:
        return 0.0
    rate = _recent_discharge_rate_vph(timestamps, voltages)
    if rate is None or rate <= 0:
        return None
    hours = (current - critical_voltage) / rate
    return min(max(hours, 0.0), 168.0)


def _infer_charge_state(
    timestamps: list[float],
    voltages: list[float | None],
    battery_levels: list[int | None],
    power_type: str,
) -> str:
    if power_type == "mains":
        return "powered"

    # Look at last ~6 hours of trend
    cutoff = time.time() - 6 * 3600
    recent_b = [
        (ts, b)
        for ts, b in zip(timestamps, battery_levels, strict=False)
        if b is not None and b < 101 and ts >= cutoff
    ]
    recent_v = [
        (ts, v)
        for ts, v in zip(timestamps, voltages, strict=False)
        if v is not None and ts >= cutoff
    ]

    def _trend(pairs: list[tuple[float, float]]) -> float | None:
        if len(pairs) < 3:
            return None
        pairs = sorted(pairs, key=lambda item: item[0])
        span = pairs[-1][0] - pairs[0][0]
        if span <= 0:
            return None
        return (pairs[-1][1] - pairs[0][1]) / (span / 3600.0)

    bat_trend = _trend([(ts, float(b)) for ts, b in recent_b])
    volt_trend = _trend(recent_v)

    if bat_trend is not None:
        if bat_trend > 0.5:
            return "charging"
        if bat_trend < -0.5:
            return "discharging"
    if volt_trend is not None:
        if volt_trend > 0.01:
            return "charging"
        if volt_trend < -0.01:
            return "discharging"
    if power_type in ("solar", "battery"):
        return "stable"
    return "unknown"


def analyze_solar_degradation(
    timestamps: list[float],
    voltages: list[float | None],
    battery_levels: list[int | None],
) -> dict[str, Any]:
    """
    Detect early battery/charger problems on solar nodes.

    Signals:
    - days since near-full charge
    - consecutive days without daytime charge gain
    - declining daily peak charge
    - deeper overnight lows vs prior baseline
    """
    issues: list[str] = []
    now = time.time()

    # Group by UTC day
    by_day: dict[str, dict[str, Any]] = {}
    for ts, voltage, level in zip(timestamps, voltages, battery_levels, strict=False):
        day = _utc_day_key(ts)
        bucket = by_day.setdefault(
            day,
            {
                "levels": [],
                "voltages": [],
                "morning": [],
                "afternoon": [],
                "night": [],
            },
        )
        hour = _utc_hour(ts)
        if level is not None and level < 101:
            bucket["levels"].append(level)
            if hour in MORNING_HOURS:
                bucket["morning"].append(level)
            if hour in AFTERNOON_HOURS:
                bucket["afternoon"].append(level)
            if hour < 6 or hour >= 18:
                bucket["night"].append(level)
        if voltage is not None:
            bucket["voltages"].append(voltage)

    days_sorted = sorted(by_day.keys())
    days_since_full = None
    last_full_ts = None
    for ts, voltage, level in zip(timestamps, voltages, battery_levels, strict=False):
        near_full = (level is not None and level >= NEAR_FULL_BATTERY_PCT) or (
            voltage is not None and voltage >= FULL_CHARGE_VOLTAGE
        )
        if near_full:
            last_full_ts = ts
    if last_full_ts is not None:
        days_since_full = max(0, int((now - last_full_ts) / 86400))
    elif days_sorted:
        days_since_full = len(days_sorted)

    # Daytime gain: afternoon avg - morning avg
    no_gain_streak = 0
    daily_peaks: list[tuple[str, float]] = []
    overnight_mins: list[tuple[str, float]] = []
    for day in days_sorted:
        bucket = by_day[day]
        if bucket["levels"]:
            daily_peaks.append((day, float(max(bucket["levels"]))))
        elif bucket["voltages"]:
            # Map voltage peak into a pseudo-% scale for trend only
            daily_peaks.append((day, float(max(bucket["voltages"]) * 20)))

        if len(bucket["morning"]) >= 1 and len(bucket["afternoon"]) >= 1:
            gain = (sum(bucket["afternoon"]) / len(bucket["afternoon"])) - (
                sum(bucket["morning"]) / len(bucket["morning"])
            )
            if gain < 1.0:
                no_gain_streak += 1
            else:
                no_gain_streak = 0
        if bucket["night"]:
            overnight_mins.append((day, float(min(bucket["night"]))))

    peak_trend = "unknown"
    if len(daily_peaks) >= 5:
        recent = [p for _, p in daily_peaks[-3:]]
        prior = [p for _, p in daily_peaks[:-3]]
        if prior:
            recent_avg = sum(recent) / len(recent)
            prior_avg = sum(prior) / len(prior)
            if recent_avg < prior_avg - 5:
                peak_trend = "declining"
            elif recent_avg > prior_avg + 5:
                peak_trend = "improving"
            else:
                peak_trend = "stable"

    overnight_min = overnight_mins[-1][1] if overnight_mins else None
    baseline_overnight = None
    if len(overnight_mins) >= 4:
        baseline_overnight = sum(m for _, m in overnight_mins[:-2]) / max(
            1, len(overnight_mins) - 2
        )

    if days_since_full is not None and days_since_full >= 3:
        issues.append(f"No near-full charge in {days_since_full} day(s)")
    if no_gain_streak >= 2:
        issues.append(
            f"Weak/no daytime charge gain for {no_gain_streak} consecutive day(s)"
        )
    if peak_trend == "declining":
        issues.append("Daily peak charge is declining versus prior days")
    if (
        overnight_min is not None
        and baseline_overnight is not None
        and overnight_min < baseline_overnight - 8
    ):
        issues.append(
            f"Deeper overnight low ({overnight_min:.0f}% vs typical {baseline_overnight:.0f}%)"
        )

    # Condition severity
    condition = "healthy"
    if (
        (days_since_full is not None and days_since_full >= 5)
        or no_gain_streak >= 3
        or (peak_trend == "declining" and no_gain_streak >= 2)
    ):
        condition = "at_risk"
    elif issues:
        condition = "watching"

    return {
        "days_since_full_charge": days_since_full,
        "days_without_daytime_gain": no_gain_streak,
        "peak_charge_trend": peak_trend,
        "overnight_min_pct": overnight_min,
        "baseline_overnight_min_pct": baseline_overnight,
        "issues": issues,
        "condition": condition,
    }


def _score_from_issues(
    power_type: str,
    voltages: list[float | None],
    solar_info: dict[str, Any] | None,
    hours_to_critical: float | None,
) -> int | None:
    valid = [v for v in voltages if v is not None]
    if len(valid) < 5 and power_type != "mains":
        return None
    if power_type == "mains":
        return 100

    score = 100
    if valid:
        min_v = min(valid)
        max_v = max(valid)
        avg_v = sum(valid) / len(valid)
        v_range = max_v - min_v
        if min_v < 3.0:
            score -= 45
        elif min_v <= CRITICAL_VOLTAGE:
            score -= 30
        elif min_v < WARNING_VOLTAGE:
            score -= 18
        if avg_v < 3.5:
            score -= 12
        # Large swing often means an unhealthy pack (or untracked solar)
        if v_range > 0.7:
            score -= 15
        elif v_range > 0.4:
            score -= 8

    if hours_to_critical is not None:
        if hours_to_critical <= 6:
            score -= 35
        elif hours_to_critical <= 24:
            score -= 20
        elif hours_to_critical <= 48:
            score -= 10

    if solar_info:
        for _ in solar_info.get("issues") or []:
            score -= 12
        if solar_info.get("condition") == "at_risk":
            score -= 10

    return max(0, min(100, score))


def build_power_status(
    timestamps: list[float],
    voltages: list[float | None],
    battery_levels: list[int | None],
    *,
    stored_power_type: str | None = None,
    force_power_type: bool = False,
    power_type_locked: bool = False,
) -> dict[str, Any]:
    """Build explained power status for UI/API from prepared series."""
    power_type, reason, confidence = classify_power_source(
        timestamps, voltages, battery_levels
    )
    # Manual override / lock always wins over auto-detection
    if (
        force_power_type
        and stored_power_type in ("solar", "battery", "mains", "unknown")
    ):
        power_type = stored_power_type
        reason = "Manual power-type override"
        confidence = 1.0
    # Prefer a stored high-confidence type if current window is thin
    elif (
        stored_power_type in ("solar", "battery", "mains")
        and power_type == "unknown"
        and len(timestamps) < 10
    ):
        power_type = stored_power_type
        reason = reason or "Using previously detected power type"
        confidence = min(confidence, 0.5)

    state = _infer_charge_state(timestamps, voltages, battery_levels, power_type)
    hours_to_critical = None
    if power_type in ("solar", "battery") and state in ("discharging", "stable"):
        hours_to_critical = predict_hours_to_critical(timestamps, voltages)

    solar_info = None
    issues: list[str] = []
    if power_type == "solar":
        solar_info = analyze_solar_degradation(timestamps, voltages, battery_levels)
        issues.extend(solar_info.get("issues") or [])

    valid_v = [v for v in voltages if v is not None]
    if valid_v:
        latest_v = valid_v[-1]
        if latest_v < CRITICAL_VOLTAGE:
            issues.append(f"Voltage critically low ({latest_v:.2f}V)")
        elif latest_v < WARNING_VOLTAGE:
            issues.append(f"Voltage low ({latest_v:.2f}V)")

    if hours_to_critical is not None and hours_to_critical <= 24:
        issues.append(f"Estimated {hours_to_critical:.0f}h until critical voltage")

    health_score = _score_from_issues(
        power_type, voltages, solar_info, hours_to_critical
    )

    if power_type == "mains":
        condition = "powered"
    elif not issues:
        condition = "healthy"
    elif solar_info and solar_info.get("condition") == "at_risk":
        condition = "at_risk"
    elif any("critical" in i.lower() for i in issues) or (
        hours_to_critical is not None and hours_to_critical <= 12
    ):
        condition = "at_risk"
    else:
        condition = "watching"

    outlook = _format_outlook(power_type, state, hours_to_critical, solar_info)

    labels = {
        "solar": "Solar",
        "battery": "Battery only",
        "mains": "Mains / USB",
        "unknown": "Unknown",
    }
    condition_labels = {
        "healthy": "Healthy",
        "watching": "Watching",
        "at_risk": "At risk",
        "powered": "Powered",
        "unknown": "Unknown",
    }
    state_labels = {
        "charging": "Charging",
        "discharging": "Discharging",
        "stable": "Stable",
        "powered": "Powered",
        "unknown": "Unknown",
    }

    return {
        "power_type": power_type,
        "power_type_label": labels.get(power_type, power_type),
        "power_type_locked": bool(power_type_locked or force_power_type),
        "confidence": round(confidence, 2),
        "state": state,
        "state_label": state_labels.get(state, state),
        "reason": reason,
        "health_score": health_score,
        "condition": condition,
        "condition_label": condition_labels.get(condition, condition),
        "outlook": outlook,
        "hours_to_critical": (
            round(hours_to_critical, 1) if hours_to_critical is not None else None
        ),
        "issues": issues,
        "solar": solar_info,
        "analyzed_at": time.time(),
    }


def _format_outlook(
    power_type: str,
    state: str,
    hours_to_critical: float | None,
    solar_info: dict[str, Any] | None,
) -> str:
    if power_type == "mains":
        return "Externally powered — battery not the limiting factor"
    if power_type == "unknown":
        return "Need more telemetry to forecast power"

    if hours_to_critical is not None:
        if hours_to_critical <= 0:
            return "At or below critical voltage now"
        if hours_to_critical < 24:
            return f"About {hours_to_critical:.0f} hours to critical at current drain"
        days = hours_to_critical / 24.0
        return f"About {days:.1f} days to critical at current drain"

    if power_type == "solar":
        if state == "charging":
            return "Charging from solar — watching for full recovery"
        if solar_info and solar_info.get("days_since_full_charge") is not None:
            days = solar_info["days_since_full_charge"]
            if days == 0:
                return "Reached near-full charge recently"
            return f"Last near-full charge ~{days} day(s) ago"
        return "Solar node — no clear discharge slope for ETA yet"

    if state == "discharging":
        return "Discharging — not enough history for a reliable ETA"
    return "No active discharge trend detected"


def _read_stored_power_meta(
    db_connection: Any, node_id: int
) -> tuple[str | None, bool]:
    """Return (power_type, locked) from node_info, tolerating missing columns."""
    stored_type = None
    locked = False
    try:
        cursor = db_connection.cursor()
        try:
            cursor.execute(
                """
                SELECT power_type, COALESCE(power_type_locked, 0) as power_type_locked
                FROM node_info WHERE node_id = ?
                """,
                (node_id,),
            )
            row = cursor.fetchone()
            if row:
                if hasattr(row, "keys"):
                    stored_type = row["power_type"]
                    locked = bool(row["power_type_locked"])
                else:
                    stored_type = row[0]
                    locked = bool(row[1]) if len(row) > 1 else False
        except Exception:
            cursor.execute(
                "SELECT power_type FROM node_info WHERE node_id = ?",
                (node_id,),
            )
            row = cursor.fetchone()
            if row:
                stored_type = row["power_type"] if hasattr(row, "keys") else row[0]
    except Exception:
        pass
    return stored_type, locked


def analyze_node_power(node_id: int, db_connection: Any) -> dict[str, Any]:
    """Full power analysis for a node from telemetry_data (+ stored type hint)."""
    rows = _fetch_telemetry_rows(db_connection, node_id, days=7)
    if len(rows) < 3:
        rows = _fetch_telemetry_rows(db_connection, node_id, days=30)

    stored_type, locked = _read_stored_power_meta(db_connection, node_id)

    timestamps, voltages, batteries = _prepare_series(rows)
    if not timestamps:
        labels = {
            "solar": "Solar",
            "battery": "Battery only",
            "mains": "Mains / USB",
            "unknown": "Unknown",
        }
        ptype = stored_type or "unknown"
        return {
            "power_type": ptype,
            "power_type_label": labels.get(ptype, ptype),
            "power_type_locked": locked,
            "confidence": 0.0,
            "state": "unknown",
            "state_label": "Unknown",
            "reason": "No telemetry samples available",
            "health_score": None,
            "condition": "unknown",
            "condition_label": "Unknown",
            "outlook": "No telemetry samples available",
            "hours_to_critical": None,
            "issues": ["No telemetry samples available"],
            "solar": None,
            "analyzed_at": time.time(),
        }

    return build_power_status(
        timestamps,
        voltages,
        batteries,
        stored_power_type=stored_type,
        force_power_type=locked,
        power_type_locked=locked,
    )


# ---------------------------------------------------------------------------
# Backward-compatible wrappers (used by existing tests / MQTT worker)
# ---------------------------------------------------------------------------


def detect_power_type(node_id: int, db_connection: Any) -> tuple[str, str]:
    """Analyze voltage/battery patterns to determine power type."""
    status = analyze_node_power(node_id, db_connection)
    return status["power_type"], status.get("reason") or "No reason"


def calculate_battery_health_score(node_id: int, db_connection: Any) -> int | None:
    """Calculate battery/solar health score (0-100)."""
    status = analyze_node_power(node_id, db_connection)
    return status.get("health_score")


def predict_battery_runtime(node_id: int, db_connection: Any) -> float | None:
    """Predict hours until battery depleted based on discharge rate."""
    rows = _fetch_telemetry_rows(db_connection, node_id, days=2)
    timestamps, voltages, _batteries = _prepare_series(rows)
    return predict_hours_to_critical(timestamps, voltages)


def predict_solar_availability(
    node_id: int, db_connection: Any
) -> list[tuple[datetime, datetime]]:
    """Placeholder solar availability windows (historical pattern later)."""
    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT power_type FROM node_info WHERE node_id = ?",
        (node_id,),
    )
    row = cursor.fetchone()
    power_type = None
    if row:
        power_type = row["power_type"] if hasattr(row, "keys") else row[0]
    if power_type != "solar":
        return []

    now = datetime.now(tz=UTC)
    predictions = []
    for day_offset in range(7):
        start_time = (now + timedelta(days=day_offset)).replace(
            hour=8, minute=0, second=0, microsecond=0
        )
        end_time = start_time + timedelta(hours=12)
        predictions.append((start_time, end_time))
    return predictions


def update_power_analysis_for_node(node_id: int, db_connection: Any) -> dict[str, Any]:
    """
    Persist unified power analysis for a single node.

    When power_type_locked is set, auto-detection will not overwrite power_type.
    Returns the status dict that was stored / computed.
    """
    cursor = db_connection.cursor()
    status = analyze_node_power(node_id, db_connection)
    locked = bool(status.get("power_type_locked"))
    try:
        if locked:
            cursor.execute(
                """
                UPDATE node_info
                SET battery_health_score = ?,
                    power_type_reason = ?,
                    power_analysis_timestamp = ?
                WHERE node_id = ?
                """,
                (
                    status.get("health_score"),
                    status.get("reason"),
                    status.get("analyzed_at") or time.time(),
                    node_id,
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE node_info
                SET power_type = ?,
                    battery_health_score = ?,
                    power_type_reason = ?,
                    power_analysis_timestamp = ?
                WHERE node_id = ?
                """,
                (
                    status["power_type"],
                    status.get("health_score"),
                    status.get("reason"),
                    status.get("analyzed_at") or time.time(),
                    node_id,
                ),
            )
        db_connection.commit()
        logger.debug(
            "Updated power analysis for node %s: type=%s condition=%s score=%s locked=%s",
            node_id,
            status["power_type"],
            status.get("condition"),
            status.get("health_score"),
            locked,
        )
    except Exception as e:
        logger.error("Error updating power analysis for node %s: %s", node_id, e)
        try:
            db_connection.rollback()
        except Exception:
            pass
    return status


def set_power_type_override(
    node_id: int,
    power_type: str,
    db_connection: Any,
    *,
    locked: bool = True,
) -> dict[str, Any]:
    """
    Manually set a node's power type and optionally lock it against auto-detect.

    Returns the refreshed power status.
    """
    allowed = {"solar", "battery", "mains", "unknown"}
    if power_type not in allowed:
        raise ValueError(f"Invalid power_type '{power_type}'. Allowed: {sorted(allowed)}")

    cursor = db_connection.cursor()
    reason = "Manual power-type override" if locked else "Manual power-type set (unlocked)"
    try:
        cursor.execute(
            """
            UPDATE node_info
            SET power_type = ?,
                power_type_locked = ?,
                power_type_reason = ?,
                power_analysis_timestamp = ?
            WHERE node_id = ?
            """,
            (power_type, 1 if locked else 0, reason, time.time(), node_id),
        )
        if cursor.rowcount == 0:
            raise ValueError(f"Node {node_id} not found")
        db_connection.commit()
    except ValueError:
        raise
    except Exception as e:
        try:
            db_connection.rollback()
        except Exception:
            pass
        raise RuntimeError(f"Failed to set power type override: {e}") from e

    return analyze_node_power(node_id, db_connection)


def get_solar_power_conditions(db_connection: Any) -> dict[str, Any]:
    """
    Analyze non-archived solar nodes with recent telemetry and group by condition.

    Nodes without battery/voltage telemetry within
    RECENT_TELEMETRY_MAX_AGE_HOURS are excluded from health/monitoring lists.

    Returns:
        {
          "at_risk": [...],
          "watching": [...],
          "healthy": [...],
          "unknown": [...],
          "counts": {...},
          "recent_max_age_hours": int,
        }
    """
    cursor = db_connection.cursor()
    cutoff = recent_telemetry_cutoff()
    cursor.execute(
        """
        SELECT ni.node_id, ni.hex_id, ni.long_name, ni.short_name, ni.power_type,
               COALESCE(ni.power_type_locked, 0) as power_type_locked,
               MAX(td.timestamp) as last_telemetry
        FROM node_info ni
        INNER JOIN telemetry_data td ON td.node_id = ni.node_id
        WHERE ni.power_type = 'solar'
          AND COALESCE(ni.archived, 0) = 0
          AND td.timestamp > ?
          AND (td.voltage IS NOT NULL OR td.battery_level IS NOT NULL)
        GROUP BY ni.node_id
        ORDER BY ni.long_name COLLATE NOCASE, ni.hex_id
        """,
        (cutoff,),
    )
    try:
        rows = cursor.fetchall()
    except Exception:
        # Older DBs without power_type_locked
        cursor.execute(
            """
            SELECT ni.node_id, ni.hex_id, ni.long_name, ni.short_name, ni.power_type,
                   MAX(td.timestamp) as last_telemetry
            FROM node_info ni
            INNER JOIN telemetry_data td ON td.node_id = ni.node_id
            WHERE ni.power_type = 'solar'
              AND COALESCE(ni.archived, 0) = 0
              AND td.timestamp > ?
              AND (td.voltage IS NOT NULL OR td.battery_level IS NOT NULL)
            GROUP BY ni.node_id
            ORDER BY ni.long_name COLLATE NOCASE, ni.hex_id
            """,
            (cutoff,),
        )
        rows = cursor.fetchall()

    groups: dict[str, list[dict[str, Any]]] = {
        "at_risk": [],
        "watching": [],
        "healthy": [],
        "unknown": [],
    }

    for row in rows:
        node_id = row["node_id"] if hasattr(row, "keys") else row[0]
        hex_id = row["hex_id"] if hasattr(row, "keys") else row[1]
        long_name = row["long_name"] if hasattr(row, "keys") else row[2]
        short_name = row["short_name"] if hasattr(row, "keys") else row[3]
        locked = False
        last_telemetry = None
        if hasattr(row, "keys"):
            try:
                locked = bool(row["power_type_locked"])
            except (KeyError, IndexError):
                locked = False
            try:
                last_telemetry = row["last_telemetry"]
            except (KeyError, IndexError):
                last_telemetry = None

        status = analyze_node_power(node_id, db_connection)
        condition = status.get("condition") or "unknown"
        if condition == "powered":
            condition = "healthy"
        if condition not in groups:
            condition = "unknown"

        name = long_name or short_name or hex_id or f"!{int(node_id):08x}"
        entry = {
            "node_id": node_id,
            "hex_id": hex_id or f"!{int(node_id):08x}",
            "name": name,
            "short_name": short_name,
            "power_type": status.get("power_type") or "solar",
            "power_type_locked": locked or bool(status.get("power_type_locked")),
            "condition": condition,
            "condition_label": status.get("condition_label") or condition,
            "state": status.get("state"),
            "state_label": status.get("state_label"),
            "outlook": status.get("outlook"),
            "issues": status.get("issues") or [],
            "health_score": status.get("health_score"),
            "hours_to_critical": status.get("hours_to_critical"),
            "solar": status.get("solar"),
            "reason": status.get("reason"),
            "last_telemetry": last_telemetry,
        }
        groups[condition].append(entry)

    return {
        **groups,
        "counts": {k: len(v) for k, v in groups.items()},
        "total": sum(len(v) for v in groups.values()),
        "recent_max_age_hours": RECENT_TELEMETRY_MAX_AGE_HOURS,
    }


def update_power_analysis_batch(
    db_connection: Any, *, force_update: bool = False
) -> dict[str, int]:
    """
    Re-analyze nodes needing updates. Used by MQTT worker and power monitor.
    """
    cursor = db_connection.cursor()
    current_time = time.time()
    reanalysis_threshold = current_time - (24 * 3600)

    if force_update:
        cursor.execute(
            """
            SELECT DISTINCT ni.node_id
            FROM node_info ni
            INNER JOIN telemetry_data td ON ni.node_id = td.node_id
            WHERE td.voltage IS NOT NULL OR td.battery_level IS NOT NULL
            """
        )
    else:
        cursor.execute(
            """
            SELECT DISTINCT ni.node_id
            FROM node_info ni
            INNER JOIN telemetry_data td ON ni.node_id = td.node_id
            WHERE (td.voltage IS NOT NULL OR td.battery_level IS NOT NULL)
              AND (
                ni.power_type IS NULL
                OR ni.power_type = 'unknown'
                OR ni.power_analysis_timestamp IS NULL
                OR ni.power_analysis_timestamp < ?
              )
            """,
            (reanalysis_threshold,),
        )

    node_ids = [row["node_id"] if hasattr(row, "keys") else row[0] for row in cursor.fetchall()]
    counts = {"solar": 0, "battery": 0, "mains": 0, "unknown": 0, "updated": 0}
    for node_id in node_ids:
        status = update_power_analysis_for_node(node_id, db_connection)
        ptype = status.get("power_type") or "unknown"
        counts[ptype] = counts.get(ptype, 0) + 1
        counts["updated"] += 1
    return counts


def check_battery_alerts(
    db_connection: Any, critical_voltage: float = 3.2, warning_voltage: float = 3.4
) -> list[dict[str, Any]]:
    """Check for nodes with low battery and return alert information."""
    cursor = db_connection.cursor()
    alerts = []
    one_hour_ago = time.time() - 3600

    cursor.execute(
        """
        SELECT DISTINCT
            ni.node_id,
            ni.hex_id,
            ni.long_name,
            ni.short_name,
            td.voltage,
            td.timestamp
        FROM node_info ni
        JOIN telemetry_data td ON ni.node_id = td.node_id
        WHERE td.voltage IS NOT NULL
          AND td.timestamp > ?
          AND ni.power_type IN ('solar', 'battery')
          AND COALESCE(ni.archived, 0) = 0
        ORDER BY td.timestamp DESC
        """,
        (one_hour_ago,),
    )

    seen: set[int] = set()
    for row in cursor.fetchall():
        node_id = row["node_id"]
        if node_id in seen:
            continue
        seen.add(node_id)
        voltage = normalize_voltage(row["voltage"])
        if voltage is None or voltage >= warning_voltage:
            continue
        alert_type = "critical" if voltage < critical_voltage else "warning"
        node_name = row["long_name"] or row["short_name"] or row["hex_id"]
        alerts.append(
            {
                "node_id": node_id,
                "hex_id": row["hex_id"],
                "name": node_name,
                "voltage": voltage,
                "alert_type": alert_type,
                "timestamp": row["timestamp"],
            }
        )

    return alerts
