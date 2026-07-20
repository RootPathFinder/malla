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
    calculate_battery_health_score,
    check_battery_alerts,
    classify_power_source,
    detect_power_type,
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
            battery_health_score INTEGER,
            last_battery_voltage REAL,
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

    # Create declining voltage pattern (battery discharge)
    current_time = time.time()
    for day in range(7):
        for hour in range(24):
            timestamp = current_time - ((6 - day) * 24 * 3600) + (hour * 3600)
            # Steadily declining voltage
            voltage = 4.0 - (day * 0.08) - (hour * 0.002)
            battery = max(10, 90 - (day * 10) - (hour * 0.3))
            cursor.execute(
                """
                INSERT INTO telemetry_data (timestamp, node_id, voltage, battery_level)
                VALUES (?, ?, ?, ?)
            """,
                (timestamp, node_id, voltage, int(battery)),
            )

    db_with_telemetry.commit()

    # Test detection
    power_type, _ = detect_power_type(node_id, db_with_telemetry)
    assert power_type == "battery", f"Expected 'battery' but got '{power_type}'"


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
    assert any("full charge" in i.lower() or "daytime" in i.lower() for i in solar["issues"])


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
