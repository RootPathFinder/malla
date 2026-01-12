#!/usr/bin/env python3
"""Quick script to check database state for power analysis."""

from src.malla.database.connection import get_db_connection

conn = get_db_connection()
cursor = conn.cursor()

# Check if we have telemetry data
cursor.execute("SELECT COUNT(*) as total FROM telemetry_data WHERE voltage IS NOT NULL")
voltage_count = cursor.fetchone()["total"]
print(f"Total voltage records: {voltage_count}")

# Check power types in node_info
cursor.execute(
    "SELECT power_type, COUNT(*) as count FROM node_info GROUP BY power_type"
)
print("\nPower type distribution:")
for row in cursor.fetchall():
    print(f"  {row['power_type']}: {row['count']} nodes")

# Check sample voltage data
cursor.execute(
    "SELECT node_id, voltage, timestamp FROM telemetry_data WHERE voltage IS NOT NULL ORDER BY timestamp DESC LIMIT 10"
)
print("\nRecent voltage data (sample):")
for row in cursor.fetchall():
    print(
        f"  node_id={row['node_id']}, voltage={row['voltage']:.3f}V, timestamp={row['timestamp']}"
    )

# Check voltage ranges per node
cursor.execute("""
    SELECT
        td.node_id,
        ni.long_name,
        COUNT(*) as readings,
        MIN(td.voltage) as min_v,
        MAX(td.voltage) as max_v,
        AVG(td.voltage) as avg_v,
        (MAX(td.voltage) - MIN(td.voltage)) as range_v
    FROM telemetry_data td
    JOIN node_info ni ON td.node_id = ni.node_id
    WHERE td.voltage IS NOT NULL
    GROUP BY td.node_id
    ORDER BY readings DESC
    LIMIT 10
""")
print("\nVoltage statistics by node (top 10):")
for row in cursor.fetchall():
    print(
        f"  {row['long_name'] or 'Unknown'} (ID={row['node_id']}): {row['readings']} readings, range={row['range_v']:.3f}V (min={row['min_v']:.3f}, max={row['max_v']:.3f}, avg={row['avg_v']:.3f})"
    )

conn.close()
