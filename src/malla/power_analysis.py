"""
Power analysis module for solar/battery monitoring.

This module provides algorithms for:
- Detecting power source types (solar, battery, mains)
- Calculating battery health scores
- Predicting battery runtime
- Predicting solar availability windows
"""

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


def detect_power_type(node_id: int, db_connection: Any) -> tuple[str, str]:
    """
    Analyze voltage patterns over last 7 days to determine power type.

    Detection logic:
    - Solar: Shows charging pattern during daylight hours (voltage increases)
    - Battery: Voltage only decreases over time
    - Mains: Voltage stays constant (4.1-4.2V typically)
    - Unknown: Insufficient data

    Args:
        node_id: The numeric node ID to analyze
        db_connection: SQLite database connection

    Returns:
        Tuple of (Power type, Reason)
        Power type: 'solar', 'battery', 'mains', or 'unknown'
        Reason: Explanation of the detection logic
    """
    cursor = db_connection.cursor()

    # Get voltage data from last 7 days
    seven_days_ago = time.time() - (7 * 24 * 3600)

    cursor.execute(
        """
        SELECT timestamp, voltage
        FROM telemetry_data
        WHERE node_id = ? AND voltage IS NOT NULL AND timestamp > ?
        ORDER BY timestamp ASC
    """,
        (node_id, seven_days_ago),
    )

    voltage_data = cursor.fetchall()

    if len(voltage_data) < 10:
        # Insufficient data
        return (
            "unknown",
            f"Insufficient telemetry data ({len(voltage_data)} packets) in last 7 days",
        )

    voltages = [row["voltage"] for row in voltage_data]
    timestamps = [row["timestamp"] for row in voltage_data]

    # Calculate voltage statistics
    min_voltage = min(voltages)
    max_voltage = max(voltages)
    avg_voltage = sum(voltages) / len(voltages)
    voltage_range = max_voltage - min_voltage

    # Check if voltage is very stable (mains power characteristic)
    # Mains-powered devices typically stay at 4.1-4.2V with minimal variation
    if voltage_range < 0.1 and avg_voltage > 4.0:
        return (
            "mains",
            f"Voltage is stable (avg {avg_voltage:.2f}V, range {voltage_range:.2f}V)",
        )

    # Analyze daily patterns for solar detection
    # Group by hour of day and check for charging patterns
    hourly_averages = {}
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts, tz=UTC)
        hour = dt.hour
        if hour not in hourly_averages:
            hourly_averages[hour] = []
        hourly_averages[hour].append(voltages[i])

    # Calculate average voltage for each hour
    hourly_avg_voltage = {
        hour: sum(vals) / len(vals) for hour, vals in hourly_averages.items()
    }

    # Check for daylight charging pattern (6am-6pm higher voltage)
    if len(hourly_avg_voltage) >= 8:  # Need reasonable coverage
        daytime_hours = [h for h in range(6, 18) if h in hourly_avg_voltage]
        nighttime_hours = [
            h
            for h in list(range(0, 6)) + list(range(18, 24))
            if h in hourly_avg_voltage
        ]

        if daytime_hours and nighttime_hours:
            daytime_avg = sum(hourly_avg_voltage[h] for h in daytime_hours) / len(
                daytime_hours
            )
            nighttime_avg = sum(hourly_avg_voltage[h] for h in nighttime_hours) / len(
                nighttime_hours
            )

            # Solar nodes show higher voltage during day (charging)
            if daytime_avg > nighttime_avg + 0.15:
                return (
                    "solar",
                    f"Daytime voltage ({daytime_avg:.2f}V) significantly higher than nighttime ({nighttime_avg:.2f}V)",
                )

    # Check overall trend: if voltage only decreases, it's battery
    # Calculate linear trend
    n = len(voltages)
    x_mean = sum(range(n)) / n
    y_mean = sum(voltages) / n

    numerator = sum((i - x_mean) * (voltages[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    if denominator > 0:
        slope = numerator / denominator

        # Negative slope indicates declining voltage (battery discharge)
        # Positive slope might indicate charging or mains
        if slope < -0.0001 and voltage_range > 0.2:
            return (
                "battery",
                f"Voltage shows downward trend (discharge pattern, slope {slope:.5f})",
            )

    return "unknown", "No distinct power pattern detected"


def calculate_battery_health_score(node_id: int, db_connection: Any) -> int | None:
    """
    Calculate battery health score (0-100) based on various metrics.

    Factors considered:
    - Voltage stability (less variation = healthier)
    - Discharge rate consistency
    - Minimum voltage observed (below 3.3V indicates degradation)
    - Age of battery (time since first seen)

    Args:
        node_id: The numeric node ID to analyze
        db_connection: SQLite database connection

    Returns:
        Health score (0-100) or None if insufficient data
    """
    cursor = db_connection.cursor()

    # Get voltage data from last 30 days
    thirty_days_ago = time.time() - (30 * 24 * 3600)

    cursor.execute(
        """
        SELECT timestamp, voltage
        FROM telemetry_data
        WHERE node_id = ? AND voltage IS NOT NULL AND timestamp > ?
        ORDER BY timestamp ASC
    """,
        (node_id, thirty_days_ago),
    )

    voltage_data = cursor.fetchall()

    if len(voltage_data) < 5:
        return None

    voltages = [row["voltage"] for row in voltage_data]

    # Start with 100 points
    score = 100

    # Factor 1: Minimum voltage (critical indicator)
    min_voltage = min(voltages)
    if min_voltage < 3.0:
        score -= 50  # Critical low voltage
    elif min_voltage < 3.2:
        score -= 30
    elif min_voltage < 3.4:
        score -= 15

    # Factor 2: Voltage range (stability)
    voltage_range = max(voltages) - min(voltages)
    if voltage_range > 1.0:
        score -= 20  # High variation indicates issues
    elif voltage_range > 0.7:
        score -= 10

    # Factor 3: Average voltage level
    avg_voltage = sum(voltages) / len(voltages)
    if avg_voltage < 3.5:
        score -= 15
    elif avg_voltage > 4.0:
        score += 5  # Bonus for healthy voltage

    # Factor 4: Check for rapid discharge events
    rapid_drops = 0
    for i in range(1, len(voltages)):
        voltage_drop = voltages[i - 1] - voltages[i]
        time_diff = voltage_data[i]["timestamp"] - voltage_data[i - 1]["timestamp"]
        if time_diff > 0 and voltage_drop > 0.2 and time_diff < 3600:
            rapid_drops += 1

    score -= min(rapid_drops * 5, 20)  # Penalize rapid drops

    # Ensure score stays in valid range
    return max(0, min(100, score))


def predict_battery_runtime(node_id: int, db_connection: Any) -> float | None:
    """
    Predict hours until battery depleted based on discharge rate.

    Args:
        node_id: The numeric node ID to analyze
        db_connection: SQLite database connection

    Returns:
        Estimated hours remaining (float) or None if insufficient data
    """
    cursor = db_connection.cursor()

    # Get recent voltage data (last 48 hours)
    forty_eight_hours_ago = time.time() - (48 * 3600)

    cursor.execute(
        """
        SELECT timestamp, voltage
        FROM telemetry_data
        WHERE node_id = ? AND voltage IS NOT NULL AND timestamp > ?
        ORDER BY timestamp ASC
    """,
        (node_id, forty_eight_hours_ago),
    )

    voltage_data = cursor.fetchall()

    if len(voltage_data) < 5:
        return None

    voltages = [row["voltage"] for row in voltage_data]
    timestamps = [row["timestamp"] for row in voltage_data]

    current_voltage = voltages[-1]

    # Don't predict for mains-powered or fully charged
    if current_voltage > 4.15:
        return None

    # Calculate discharge rate (volts per hour)
    time_span_hours = (timestamps[-1] - timestamps[0]) / 3600
    if time_span_hours < 1:
        return None

    voltage_drop = voltages[0] - current_voltage

    # Only predict if voltage is actually dropping
    if voltage_drop <= 0:
        return None

    discharge_rate_per_hour = voltage_drop / time_span_hours

    # Critical voltage threshold (node will likely shut down)
    critical_voltage = 3.2

    if current_voltage <= critical_voltage:
        return 0.0

    voltage_remaining = current_voltage - critical_voltage

    if discharge_rate_per_hour > 0:
        hours_remaining = voltage_remaining / discharge_rate_per_hour
        # Cap prediction at reasonable maximum (7 days)
        return min(hours_remaining, 168.0)

    return None


def predict_solar_availability(
    node_id: int, db_connection: Any
) -> list[tuple[datetime, datetime]]:
    """
    For solar nodes, predict availability windows for next 7 days.

    This is a simplified prediction based on historical charging patterns.
    Future versions could integrate actual sunrise/sunset times and weather.

    Args:
        node_id: The numeric node ID to analyze
        db_connection: SQLite database connection

    Returns:
        List of (datetime_start, datetime_end) tuples for predicted availability
    """
    cursor = db_connection.cursor()

    # Check if this is a solar node
    cursor.execute(
        "SELECT power_type FROM node_info WHERE node_id = ?",
        (node_id,),
    )
    row = cursor.fetchone()

    if not row or row["power_type"] != "solar":
        return []

    # Get historical charging patterns
    seven_days_ago = time.time() - (7 * 24 * 3600)

    cursor.execute(
        """
        SELECT timestamp, voltage
        FROM telemetry_data
        WHERE node_id = ? AND voltage IS NOT NULL AND timestamp > ?
        ORDER BY timestamp ASC
    """,
        (node_id, seven_days_ago),
    )

    voltage_data = cursor.fetchall()

    if len(voltage_data) < 20:
        return []

    # Simple prediction: assume similar pattern for next 7 days
    # This is a placeholder - real implementation would use sunrise/sunset
    # and analyze actual voltage patterns for availability windows
    now = datetime.now(tz=UTC)
    predictions = []

    for day_offset in range(7):
        # Predict availability from 8am to 8pm (typical solar charging window)
        start_time = now + timedelta(days=day_offset, hours=8 - now.hour)
        end_time = start_time + timedelta(hours=12)

        predictions.append((start_time, end_time))

    return predictions


def update_power_analysis_for_node(node_id: int, db_connection: Any) -> None:
    """
    Update power analysis for a single node.

    This function:
    1. Detects power type
    2. Calculates battery health score
    3. Updates node_info table

    Args:
        node_id: The numeric node ID to analyze
        db_connection: SQLite database connection
    """
    cursor = db_connection.cursor()

    try:
        # Detect power type
        power_type, reason = detect_power_type(node_id, db_connection)

        # Calculate health score if battery-powered
        health_score = None
        if power_type in ("solar", "battery"):
            health_score = calculate_battery_health_score(node_id, db_connection)

        current_time = time.time()

        # Update node_info
        cursor.execute(
            """
            UPDATE node_info
            SET power_type = ?, battery_health_score = ?, power_type_reason = ?, power_analysis_timestamp = ?
            WHERE node_id = ?
        """,
            (power_type, health_score, reason, current_time, node_id),
        )

        db_connection.commit()

        logger.debug(
            f"Updated power analysis for node {node_id}: type={power_type}, health={health_score}, reason={reason}"
        )

    except Exception as e:
        logger.error(f"Error updating power analysis for node {node_id}: {e}")
        db_connection.rollback()


def check_battery_alerts(
    db_connection: Any, critical_voltage: float = 3.2, warning_voltage: float = 3.4
) -> list[dict[str, Any]]:
    """
    Check for nodes with low battery and return alert information.

    Args:
        db_connection: SQLite database connection
        critical_voltage: Voltage threshold for critical alerts
        warning_voltage: Voltage threshold for warning alerts

    Returns:
        List of alert dictionaries with node info and voltage
    """
    cursor = db_connection.cursor()

    alerts = []

    # Get nodes with recent low voltage
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
          AND td.voltage < ?
          AND ni.power_type IN ('solar', 'battery')
        ORDER BY td.voltage ASC
    """,
        (one_hour_ago, warning_voltage),
    )

    for row in cursor.fetchall():
        alert_type = "critical" if row["voltage"] < critical_voltage else "warning"
        node_name = row["long_name"] or row["short_name"] or row["hex_id"]

        alerts.append(
            {
                "node_id": row["node_id"],
                "hex_id": row["hex_id"],
                "name": node_name,
                "voltage": row["voltage"],
                "alert_type": alert_type,
                "timestamp": row["timestamp"],
            }
        )

    return alerts
