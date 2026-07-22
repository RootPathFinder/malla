"""
Unit tests for power analysis module
"""

import sqlite3
import tempfile
import time
from datetime import UTC, datetime, timedelta

import pytest

from malla.power_analysis import (
    analyze_node_power,
    analyze_solar_degradation,
    build_power_status,
    calculate_battery_health_score,
    check_battery_alerts,
    classify_power_source,
    detect_power_type,
    infer_sunlight_from_temperature,
    normalize_voltage,
    predict_battery_runtime,
)


@pytest.fixture
def db_with_telemetry():
    """Create a database with telemetry test data."""
    # Create a temporary database
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()

    conn = sqlite3.connect(temp_db.name)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Create telemetry table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            node_id INTEGER NOT NULL,
            battery_level INTEGER,
            voltage REAL,
            channel_utilization REAL,
            air_util_tx REAL,
            uptime_seconds INTEGER
        )
    """)

    # Create node_info table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS node_info (
            node_id INTEGER PRIMARY KEY,
            hex_id TEXT,
            long_name TEXT,
            short_name TEXT,
            power_type TEXT DEFAULT 'unknown',
            power_type_reason TEXT,
            power_type_locked INTEGER DEFAULT 0,
            battery_health_score INTEGER,
            last_battery_voltage REAL,
            power_analysis_timestamp REAL,
            archived INTEGER DEFAULT 0
        )
    """)

    conn.commit()
    yield conn
    conn.close()

    # Clean up
    import os

    try:
        os.unlink(temp_db.name)
    except FileNotFoundError:
        pass


def test_detect_solar_power_type(db_with_telemetry):
    """Test solar node detection with charging pattern."""
    cursor = db_with_telemetry.cursor()
    node_id = 1001

    # Insert node
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name) VALUES (?, ?, ?)",
        (node_id, "!000003e9", "Solar Node"),
    )

    # Create solar charging pattern: higher voltage during day (6am-6pm)
    # Align to start of day UTC to ensure hour logic matches
    current_dt = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    current_time = current_dt.timestamp()

    for day in range(7):
        base_time = current_time - ((6 - day) * 24 * 3600)

        # Nighttime (low voltage - 0am-6am)
        for hour in range(0, 6):
            timestamp = base_time + (hour * 3600)
            voltage = 3.5  # Steady low voltage at night
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
            """,
                (timestamp, node_id, voltage, 50),
            )

        # Daytime (charging - higher voltage 6am-6pm)
        for hour in range(6, 18):
            timestamp = base_time + (hour * 3600)
            voltage = 4.2  # High voltage during day (solar charging)
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
            """,
                (timestamp, node_id, voltage, 85),
            )

        # Evening (declining back to night levels 6pm-midnight)
        for hour in range(18, 24):
            timestamp = base_time + (hour * 3600)
            voltage = 3.5  # Back to low voltage in evening
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
            """,
                (timestamp, node_id, voltage, 50),
            )

    db_with_telemetry.commit()

    # Test detection
    power_type, _ = detect_power_type(node_id, db_with_telemetry)
    assert power_type == "solar", f"Expected 'solar' but got '{power_type}'"


def test_detect_battery_power_type(db_with_telemetry):
    """Test battery-only node detection with declining voltage."""
    cursor = db_with_telemetry.cursor()
    node_id = 1002

    # Insert node
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name) VALUES (?, ?, ?)",
        (node_id, "!000003ea", "Battery Node"),
    )

    # Midnight-aligned UTC base so loop hour == UTC hour. Using wall-clock
    # relative timestamps can fake an AM→PM rise and misclassify as solar.
    now = datetime.now(tz=UTC)
    base = (
        (now - timedelta(days=6))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .timestamp()
    )
    for day in range(7):
        for hour in range(24):
            timestamp = base + (day * 24 * 3600) + (hour * 3600)
            # Steadily declining voltage/battery within each day (no recharge)
            voltage = 4.0 - (day * 0.08) - (hour * 0.002)
            battery = max(10, 85 - (day * 10) - (hour * 0.4))
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
            """,
                (timestamp, node_id, voltage, int(battery)),
            )

    db_with_telemetry.commit()

    # Test detection
    power_type, reason = detect_power_type(node_id, db_with_telemetry)
    assert power_type == "battery", (
        f"Expected 'battery' but got '{power_type}' ({reason})"
    )


def test_detect_mains_power_type(db_with_telemetry):
    """Test mains-powered node detection with stable voltage."""
    cursor = db_with_telemetry.cursor()
    node_id = 1003

    # Insert node
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name) VALUES (?, ?, ?)",
        (node_id, "!000003eb", "Mains Node"),
    )

    # Create stable voltage pattern (mains power)
    current_time = time.time()
    for day in range(7):
        for hour in range(0, 24, 2):  # Every 2 hours
            timestamp = current_time - ((6 - day) * 24 * 3600) + (hour * 3600)
            # Very stable voltage around 4.2V
            voltage = 4.18 + ((hour % 4) * 0.01)  # Minor fluctuation
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
            """,
                (timestamp, node_id, voltage, 100),
            )

    db_with_telemetry.commit()

    # Test detection
    power_type, _ = detect_power_type(node_id, db_with_telemetry)
    assert power_type == "mains", f"Expected 'mains' but got '{power_type}'"


def test_detect_unknown_power_type(db_with_telemetry):
    """Test unknown power type with insufficient data."""
    cursor = db_with_telemetry.cursor()
    node_id = 1004

    # Insert node
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name) VALUES (?, ?, ?)",
        (node_id, "!000003ec", "Unknown Node"),
    )

    # Insert only a few data points
    current_time = time.time()
    for i in range(3):
        cursor.execute(
            """
            INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
            VALUES (?, ?, ?, ?)
        """,
            (current_time - (i * 3600), node_id, 3.8, 70),
        )

    db_with_telemetry.commit()

    # Test detection
    power_type, _ = detect_power_type(node_id, db_with_telemetry)
    assert power_type == "unknown", f"Expected 'unknown' but got '{power_type}'"


def test_calculate_battery_health_score_healthy(db_with_telemetry):
    """Test battery health calculation for a healthy battery."""
    cursor = db_with_telemetry.cursor()
    node_id = 2001

    # Insert node
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name) VALUES (?, ?, ?)",
        (node_id, "!00007d1", "Healthy Battery"),
    )

    # Create healthy battery pattern
    current_time = time.time()
    for day in range(30):
        for hour in range(0, 24, 3):  # Every 3 hours
            timestamp = current_time - ((29 - day) * 24 * 3600) + (hour * 3600)
            # Good voltage range (3.7-4.1V)
            voltage = 3.85 + ((hour % 12) * 0.02)
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage)
                VALUES (?, ?, ?)
            """,
                (timestamp, node_id, voltage),
            )

    db_with_telemetry.commit()

    # Test health score
    health_score = calculate_battery_health_score(node_id, db_with_telemetry)
    assert health_score is not None
    assert health_score >= 80, f"Expected healthy score >= 80 but got {health_score}"


def test_calculate_battery_health_score_degraded(db_with_telemetry):
    """Test battery health calculation for a degraded battery."""
    cursor = db_with_telemetry.cursor()
    node_id = 2002

    # Insert node
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name) VALUES (?, ?, ?)",
        (node_id, "!00007d2", "Degraded Battery"),
    )

    # Create degraded battery pattern (low voltage, high variation)
    current_time = time.time()
    for day in range(30):
        for hour in range(0, 24, 3):
            timestamp = current_time - ((29 - day) * 24 * 3600) + (hour * 3600)
            # Low voltage with high variation
            voltage = 3.2 + ((hour % 6) * 0.15)  # 3.2-4.1V with large swings
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage)
                VALUES (?, ?, ?)
            """,
                (timestamp, node_id, voltage),
            )

    db_with_telemetry.commit()

    # Test health score
    health_score = calculate_battery_health_score(node_id, db_with_telemetry)
    assert health_score is not None
    assert health_score <= 70, f"Expected degraded score <= 70 but got {health_score}"


def test_predict_battery_runtime(db_with_telemetry):
    """Test battery runtime prediction."""
    cursor = db_with_telemetry.cursor()
    node_id = 3001

    # Insert node
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name) VALUES (?, ?, ?)",
        (node_id, "!00000bb9", "Runtime Test"),
    )

    # Create declining voltage pattern
    current_time = time.time()
    start_voltage = 3.8
    discharge_rate = 0.05  # 0.05V per hour
    for hour in range(48):
        timestamp = current_time - ((47 - hour) * 3600)
        voltage = start_voltage - (hour * discharge_rate)
        cursor.execute(
            """
            INSERT INTO telemetry_data (timestamp, node_id, voltage)
            VALUES (?, ?, ?)
        """,
            (timestamp, node_id, max(3.0, voltage)),
        )

    db_with_telemetry.commit()

    # Test runtime prediction
    runtime = predict_battery_runtime(node_id, db_with_telemetry)
    assert runtime is not None, "Runtime prediction should not be None"
    # Current voltage should be around 3.8 - (47 * 0.05) = 1.45V but capped at 3.0
    # Actually should be around 3.8 - 2.35 = 1.45, but we max at 3.0
    # So current is 3.0V, critical is 3.2V, so runtime should be 0 or very small
    assert runtime >= 0, f"Runtime should be non-negative, got {runtime}"


def test_check_battery_alerts(db_with_telemetry):
    """Test battery alert detection."""
    cursor = db_with_telemetry.cursor()

    # Insert nodes with different voltage levels
    nodes = [
        (4001, "!00000fa1", "Critical Node", "battery", 3.1),
        (4002, "!00000fa2", "Warning Node", "battery", 3.3),
        (4003, "!00000fa3", "OK Node", "solar", 3.8),
    ]

    for node_id, hex_id, name, power_type, voltage in nodes:
        cursor.execute(
            """
            INSERT INTO node_info (node_id, hex_id, long_name, power_type)
            VALUES (?, ?, ?, ?)
        """,
            (node_id, hex_id, name, power_type),
        )

        # Add recent telemetry
        cursor.execute(
            """
            INSERT INTO telemetry_data (timestamp, node_id, voltage)
            VALUES (?, ?, ?)
        """,
            (time.time() - 300, node_id, voltage),  # 5 minutes ago
        )

    db_with_telemetry.commit()

    # Test alert detection
    alerts = check_battery_alerts(
        db_with_telemetry, critical_voltage=3.2, warning_voltage=3.4
    )

    assert len(alerts) >= 2, f"Expected at least 2 alerts, got {len(alerts)}"

    # Check critical alert
    critical_alerts = [a for a in alerts if a["alert_type"] == "critical"]
    assert len(critical_alerts) >= 1, "Should have at least 1 critical alert"
    assert critical_alerts[0]["voltage"] < 3.2, "Critical alert voltage should be < 3.2"

    # Check warning alert
    warning_alerts = [a for a in alerts if a["alert_type"] == "warning"]
    assert len(warning_alerts) >= 1, "Should have at least 1 warning alert"


def test_no_alerts_for_healthy_nodes(db_with_telemetry):
    """Test that no alerts are generated for healthy nodes."""
    cursor = db_with_telemetry.cursor()

    # Insert healthy node
    cursor.execute(
        """
        INSERT INTO node_info (node_id, hex_id, long_name, power_type)
        VALUES (?, ?, ?, ?)
    """,
        (5001, "!00001389", "Healthy Node", "solar"),
    )

    # Add recent telemetry with good voltage
    cursor.execute(
        """
        INSERT INTO telemetry_data (timestamp, node_id, voltage)
        VALUES (?, ?, ?)
    """,
        (time.time() - 300, 5001, 4.0),
    )

    db_with_telemetry.commit()

    # Test alert detection
    alerts = check_battery_alerts(
        db_with_telemetry, critical_voltage=3.2, warning_voltage=3.4
    )

    assert len(alerts) == 0, f"Expected no alerts for healthy node, got {len(alerts)}"


def test_normalize_voltage_handles_encodings():
    assert normalize_voltage(4.12) == pytest.approx(4.12)
    assert normalize_voltage(0.00412) == pytest.approx(4.12)
    assert normalize_voltage(4120) == pytest.approx(4.12)
    assert normalize_voltage(None) is None
    assert normalize_voltage(0) is None


def test_classify_usb_mains_marker():
    now = time.time()
    timestamps = [now - i * 3600 for i in range(10)][::-1]
    voltages = [4.2] * 10
    batteries = [101] * 10
    power_type, reason, confidence = classify_power_source(
        timestamps, voltages, batteries
    )
    assert power_type == "mains"
    assert "101" in reason
    assert confidence >= 0.9


def test_solar_degradation_detects_no_full_charge_and_weak_days():
    """Solar node that stops reaching full charge and loses daytime gain."""
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    timestamps: list[float] = []
    voltages: list[float | None] = []
    batteries: list[int | None] = []

    # 6 days: morning low, afternoon barely moves (<1% gain), never near-full
    for day in range(6):
        day_base = now - timedelta(days=5 - day)
        for hour, level, voltage in [
            (7, 40, 3.55),
            (9, 41, 3.56),
            (15, 41, 3.57),  # essentially no daytime gain
            (17, 41, 3.57),
            (21, 38, 3.50),
        ]:
            ts = day_base.replace(hour=hour).timestamp()
            timestamps.append(ts)
            voltages.append(voltage)
            batteries.append(level)

    solar = analyze_solar_degradation(timestamps, voltages, batteries)
    assert solar["days_since_full_charge"] is not None
    assert solar["days_since_full_charge"] >= 3
    assert solar["days_without_daytime_gain"] >= 2
    assert solar["condition"] in ("watching", "at_risk")
    assert solar["issues"]
    assert any(
        "full charge" in i.lower() or "daytime" in i.lower() for i in solar["issues"]
    )


def test_analyze_node_power_returns_explained_status(db_with_telemetry):
    cursor = db_with_telemetry.cursor()
    node_id = 6001
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name) VALUES (?, ?, ?)",
        (node_id, "!00001771", "Explained Solar"),
    )

    current_dt = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    current_time = current_dt.timestamp()
    for day in range(5):
        base_time = current_time - ((4 - day) * 24 * 3600)
        for hour in range(0, 24, 2):
            timestamp = base_time + hour * 3600
            if 6 <= hour < 18:
                voltage, battery = 4.15, 96
            else:
                voltage, battery = 3.70, 55
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
                """,
                (timestamp, node_id, voltage, battery),
            )
    db_with_telemetry.commit()

    status = analyze_node_power(node_id, db_with_telemetry)
    assert status["power_type"] == "solar"
    assert status["condition"] in ("healthy", "watching", "at_risk", "powered")
    assert status["outlook"]
    assert "issues" in status
    assert status["solar"] is not None


def test_power_type_override_locks_auto_detect(db_with_telemetry):
    """Manual override must persist and block auto-detect overwrite."""
    from malla.power_analysis import (
        set_power_type_override,
        update_power_analysis_for_node,
    )

    cursor = db_with_telemetry.cursor()
    node_id = 7001
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name, power_type) VALUES (?, ?, ?, ?)",
        (node_id, "!00001b59", "Override Node", "unknown"),
    )

    # Strong solar pattern that would normally auto-detect as solar
    current_time = time.time()
    for day in range(5):
        base_time = current_time - ((4 - day) * 24 * 3600)
        for hour in range(0, 24, 2):
            timestamp = base_time + hour * 3600
            if 6 <= hour < 18:
                voltage, battery = 4.2, 98
            else:
                voltage, battery = 3.65, 50
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
                """,
                (timestamp, node_id, voltage, battery),
            )
    db_with_telemetry.commit()

    status = set_power_type_override(node_id, "battery", db_with_telemetry, locked=True)
    assert status["power_type"] == "battery"
    assert status["power_type_locked"] is True

    # Re-run auto analysis — type must stay battery
    updated = update_power_analysis_for_node(node_id, db_with_telemetry)
    assert updated["power_type"] == "battery"
    assert updated["power_type_locked"] is True

    cursor.execute(
        "SELECT power_type, power_type_locked FROM node_info WHERE node_id = ?",
        (node_id,),
    )
    row = cursor.fetchone()
    assert row["power_type"] == "battery"
    assert row["power_type_locked"] == 1


def test_get_solar_power_conditions_groups_nodes(db_with_telemetry):
    from malla.power_analysis import get_solar_power_conditions

    cursor = db_with_telemetry.cursor()
    healthy_id = 8001
    at_risk_id = 8002

    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name, power_type) VALUES (?, ?, ?, ?)",
        (healthy_id, "!00001f41", "Healthy Solar", "solar"),
    )
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name, power_type) VALUES (?, ?, ?, ?)",
        (at_risk_id, "!00001f42", "At Risk Solar", "solar"),
    )

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    # Healthy: daily full charge
    for day in range(6):
        day_base = now - timedelta(days=5 - day)
        for hour, level, voltage in [
            (7, 55, 3.7),
            (12, 90, 4.1),
            (15, 95, 4.15),
            (21, 70, 3.85),
        ]:
            ts = day_base.replace(hour=hour).timestamp()
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
                """,
                (ts, healthy_id, voltage, level),
            )

    # At risk: no full charge, no daytime gain for many days
    for day in range(7):
        day_base = now - timedelta(days=6 - day)
        for hour, level, voltage in [
            (7, 35, 3.45),
            (9, 36, 3.46),
            (15, 36, 3.47),
            (21, 30, 3.35),
        ]:
            ts = day_base.replace(hour=hour).timestamp()
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
                """,
                (ts, at_risk_id, voltage, level),
            )
    db_with_telemetry.commit()

    conditions = get_solar_power_conditions(db_with_telemetry)
    assert conditions["total"] == 2
    assert conditions["recent_max_age_hours"] == 48
    assert conditions["counts"]["at_risk"] + conditions["counts"]["watching"] >= 1
    at_risk_ids = {n["node_id"] for n in conditions["at_risk"]}
    watching_ids = {n["node_id"] for n in conditions["watching"]}
    # Degraded node should be watching or at_risk
    assert at_risk_id in at_risk_ids or at_risk_id in watching_ids
    # Healthy node should not be at_risk
    assert healthy_id not in at_risk_ids


def test_get_solar_power_conditions_excludes_stale_telemetry(db_with_telemetry):
    """Nodes whose newest telemetry is older than 48h are excluded from monitoring."""
    from malla.power_analysis import get_solar_power_conditions

    cursor = db_with_telemetry.cursor()
    stale_id = 8101
    fresh_id = 8102
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name, power_type) VALUES (?, ?, ?, ?)",
        (stale_id, "!00001fa5", "Stale Solar", "solar"),
    )
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name, power_type) VALUES (?, ?, ?, ?)",
        (fresh_id, "!00001fa6", "Fresh Solar", "solar"),
    )

    now = time.time()
    # Stale: last sample ~5 days ago
    for i in range(10):
        cursor.execute(
            """
            INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
            VALUES (?, ?, ?, ?)
            """,
            (now - (5 * 86400) - i * 3600, stale_id, 3.5, 40),
        )
    # Fresh: samples within last day
    for i in range(10):
        cursor.execute(
            """
            INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
            VALUES (?, ?, ?, ?)
            """,
            (now - i * 3600, fresh_id, 3.9, 80),
        )
    db_with_telemetry.commit()

    conditions = get_solar_power_conditions(db_with_telemetry)
    ids = {
        n["node_id"]
        for bucket in ("at_risk", "watching", "healthy", "unknown")
        for n in conditions[bucket]
    }
    assert fresh_id in ids
    assert stale_id not in ids
    assert conditions["total"] == 1


def test_infer_sunlight_unavailable_without_temperature():
    status = infer_sunlight_from_temperature([], [])
    assert status["available"] is False
    assert status["state"] == "unknown"


def test_infer_sunlight_in_sun_from_daytime_warming():
    # Anchor around a known midday UTC instant for stable day/night classification,
    # and pass it as `now` so sample age stays fresh.
    now = datetime(2026, 7, 22, 14, 0, tzinfo=UTC).timestamp()
    timestamps = [now - 7200, now - 3600, now - 1800, now]
    temperatures = [18.0, 20.5, 23.0, 25.5]

    status = infer_sunlight_from_temperature(
        timestamps,
        temperatures,
        charge_state="charging",
        now=now,
    )
    assert status["available"] is True
    assert status["state"] == "in_sun"
    assert status["confidence"] >= 0.7
    assert status["temperature_c"] == 25.5
    assert (
        "rising" in status["reason"].lower() or "charging" in status["reason"].lower()
    )


def test_infer_sunlight_night_when_cool_overnight():
    now = datetime(2026, 7, 22, 2, 0, tzinfo=UTC).timestamp()
    timestamps = [now - 7200, now - 3600, now]
    temperatures = [16.0, 15.5, 15.0]

    status = infer_sunlight_from_temperature(
        timestamps, temperatures, charge_state="discharging", now=now
    )
    assert status["available"] is True
    assert status["state"] == "night"


def test_build_power_status_includes_charge_and_sunlight(monkeypatch):
    day_anchor = datetime(2026, 7, 22, 15, 0, tzinfo=UTC).timestamp()
    monkeypatch.setattr(time, "time", lambda: day_anchor)

    # Rising battery over last hours => charging
    timestamps = [day_anchor - i * 1800 for i in range(8, -1, -1)]
    batteries = [60 + i * 2 for i in range(9)]
    voltages = [3.7 + i * 0.02 for i in range(9)]
    temp_ts = [day_anchor - 7200, day_anchor - 3600, day_anchor]
    temps = [19.0, 22.0, 25.0]

    status = build_power_status(
        timestamps,
        voltages,
        batteries,
        stored_power_type="solar",
        force_power_type=True,
        temperature_timestamps=temp_ts,
        temperatures=temps,
    )
    assert status["charge"]["state"] == "charging"
    assert status["charge"]["active"] is True
    assert status["charge"]["mode_label"] == "Active charge"
    assert status["sunlight"]["available"] is True
    assert status["sunlight"]["state"] in ("in_sun", "warming")
    assert status["state_label"] == "Charging"


def test_analyze_node_power_sunlight_from_packet_history(
    db_with_telemetry, monkeypatch
):
    """Temperature packets in packet_history feed sunlight inference."""
    from meshtastic import telemetry_pb2

    cursor = db_with_telemetry.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS packet_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL,
            topic TEXT,
            from_node_id INTEGER,
            to_node_id INTEGER,
            portnum INTEGER,
            portnum_name TEXT,
            gateway_id TEXT,
            processed_successfully INTEGER,
            raw_payload BLOB,
            payload_length INTEGER
        )
        """
    )
    node_id = 9001
    cursor.execute(
        "INSERT INTO node_info (node_id, hex_id, long_name, power_type) VALUES (?, ?, ?, ?)",
        (node_id, "!00002329", "Sun Node", "solar"),
    )

    day_anchor = datetime(2026, 7, 22, 15, 0, tzinfo=UTC).timestamp()
    monkeypatch.setattr(time, "time", lambda: day_anchor)

    for i in range(6):
        cursor.execute(
            """
            INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
            VALUES (?, ?, ?, ?)
            """,
            (day_anchor - (5 - i) * 1800, node_id, 3.8 + i * 0.05, 70 + i * 3),
        )
        tel = telemetry_pb2.Telemetry()
        tel.environment_metrics.temperature = 18.0 + i * 1.5
        payload = tel.SerializeToString()
        cursor.execute(
            """
            INSERT INTO packet_history
            (timestamp, topic, from_node_id, to_node_id, portnum, portnum_name,
             gateway_id, processed_successfully, raw_payload, payload_length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                day_anchor - (5 - i) * 1800,
                "test/topic",
                node_id,
                node_id,
                3,
                "TELEMETRY_APP",
                "!00002329",
                1,
                payload,
                len(payload),
            ),
        )
    db_with_telemetry.commit()

    status = analyze_node_power(node_id, db_with_telemetry)
    assert "charge" in status
    assert status["sunlight"]["available"] is True
    assert status["sunlight"]["temperature_c"] is not None
    assert status["sunlight"]["state"] in ("in_sun", "warming", "shade", "cooling")
