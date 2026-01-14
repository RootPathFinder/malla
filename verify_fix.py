import logging
import os
import sqlite3
import sys

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

# Mock config so imports work if needed
import malla.config

malla.config.get_config = lambda: {"database_file": "meshtastic_history.db"}

from malla.database.repositories import BatteryAnalyticsRepository

# Configure logging to see the debug outputs from the repository
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("malla.database.repositories")
logger.setLevel(logging.DEBUG)


def get_telemetry(cursor, node_id):
    query = """
    SELECT voltage, battery_level, timestamp
    FROM telemetry_data
    WHERE node_id = ? AND timestamp > strftime('%s', 'now') - (7 * 24 * 3600)
    ORDER BY timestamp ASC
    """
    cursor.execute(query, (node_id,))
    rows = cursor.fetchall()

    voltages = [r[0] for r in rows]
    battery_levels = [r[1] for r in rows]
    timestamps = [r[2] for r in rows]
    return voltages, battery_levels, timestamps


def get_node_db_id(cursor, hex_id_str):
    # hex_id in DB is usually without '!'
    clean_hex = hex_id_str.lstrip("!")
    query = "SELECT node_id FROM node_info WHERE hex_id = ? OR hex_id = ?"
    cursor.execute(query, (clean_hex, hex_id_str))
    row = cursor.fetchone()
    if row:
        return row[0]
    return None


def verify():
    db_path = "meshtastic_history.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    test_nodes = {
        "!da56b2f8": "MAINS_BOT (Expected: Mains/None)",
        "!cd76a16b": "SOLAR_GFT (Expected: Solar)",
        "!24e49e61": "BATT_D4V3 (Expected: Battery)",
    }

    print("--- Verification Run ---")

    for hex_id, description in test_nodes.items():
        print(f"\nTesting {description} [{hex_id}]")

        node_db_id = get_node_db_id(cursor, hex_id)
        if not node_db_id:
            print(f"  Node ID not found for {hex_id}")
            continue

        voltages, battery_levels, timestamps = get_telemetry(cursor, node_db_id)

        print(f"  Data Points: {len(voltages)}")
        if voltages:
            v_avg = sum(voltages) / len(voltages)
            print(f"  Avg Voltage: {v_avg:.2f}V")
        if battery_levels:
            valid_bat = [b for b in battery_levels if b is not None]
            if valid_bat:
                print(f"  Battery Range: {min(valid_bat)}% - {max(valid_bat)}%")
            else:
                print("  Battery Data: None")

        # Run classification
        try:
            # We call the static method on the CLASS
            result = BatteryAnalyticsRepository._classify_power_type_with_reason(
                voltages, timestamps, battery_levels
            )
            print(f"  RESULT: {result}")
        except Exception as e:
            print(f"  ERROR: {e}")


if __name__ == "__main__":
    verify()
