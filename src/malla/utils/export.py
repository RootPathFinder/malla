"""
Data export utilities for exporting mesh network data in various formats.

Provides functions to export packets, nodes, and analytics data to CSV, JSON,
and other formats for external analysis and reporting.
"""

import csv
import io
import json
import logging
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


def export_to_csv(
    data: list[dict],
    columns: Optional[list[str]] = None,
    filename: Optional[str] = None,
) -> tuple[str, str]:
    """
    Export data to CSV format.

    Args:
        data: List of dictionaries to export
        columns: Optional list of column names to include (None = all columns)
        filename: Optional filename for the export

    Returns:
        tuple[str, str]: (csv_content, suggested_filename)
    """
    if not data:
        return "", filename or "export_empty.csv"

    # Determine columns
    if columns is None:
        # Use all keys from first row
        columns = list(data[0].keys())

    # Generate filename if not provided
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"malla_export_{timestamp}.csv"

    # Create CSV content
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")

    writer.writeheader()
    for row in data:
        # Convert complex types to strings
        cleaned_row = {}
        for col in columns:
            value = row.get(col)
            if isinstance(value, (list, dict)):
                cleaned_row[col] = json.dumps(value)
            elif value is None:
                cleaned_row[col] = ""
            else:
                cleaned_row[col] = str(value)
        writer.writerow(cleaned_row)

    csv_content = output.getvalue()
    output.close()

    logger.info(f"Exported {len(data)} rows to CSV ({len(csv_content)} bytes)")
    return csv_content, filename


def export_to_json(
    data: Any,
    pretty: bool = True,
    filename: Optional[str] = None,
) -> tuple[str, str]:
    """
    Export data to JSON format.

    Args:
        data: Data to export (dict, list, or any JSON-serializable object)
        pretty: Whether to format JSON with indentation
        filename: Optional filename for the export

    Returns:
        tuple[str, str]: (json_content, suggested_filename)
    """
    # Generate filename if not provided
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"malla_export_{timestamp}.json"

    # Convert to JSON
    indent = 2 if pretty else None
    json_content = json.dumps(data, indent=indent, ensure_ascii=False, default=str)

    logger.info(f"Exported data to JSON ({len(json_content)} bytes)")
    return json_content, filename


def export_packets_to_csv(packets: list[dict]) -> tuple[str, str]:
    """
    Export packet data to CSV with relevant columns.

    Args:
        packets: List of packet dictionaries

    Returns:
        tuple[str, str]: (csv_content, filename)
    """
    columns = [
        "id",
        "timestamp",
        "from_node_id",
        "from_node_name",
        "to_node_id",
        "to_node_name",
        "gateway_id",
        "portnum_name",
        "hop_limit",
        "hop_start",
        "rssi",
        "snr",
        "channel_id",
        "decoded_text",
    ]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"malla_packets_{timestamp}.csv"

    return export_to_csv(packets, columns=columns, filename=filename)


def export_nodes_to_csv(nodes: list[dict]) -> tuple[str, str]:
    """
    Export node data to CSV with relevant columns.

    Args:
        nodes: List of node dictionaries

    Returns:
        tuple[str, str]: (csv_content, filename)
    """
    columns = [
        "node_id",
        "node_name",
        "short_name",
        "long_name",
        "hardware_model",
        "role",
        "latitude",
        "longitude",
        "altitude",
        "last_seen",
        "first_seen",
        "battery_level",
        "voltage",
        "primary_channel",
    ]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"malla_nodes_{timestamp}.csv"

    return export_to_csv(nodes, columns=columns, filename=filename)


def export_analytics_to_json(analytics: dict) -> tuple[str, str]:
    """
    Export analytics data to JSON.

    Args:
        analytics: Analytics dictionary

    Returns:
        tuple[str, str]: (json_content, filename)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"malla_analytics_{timestamp}.json"

    return export_to_json(analytics, pretty=True, filename=filename)


def export_network_graph_to_json(graph_data: dict) -> tuple[str, str]:
    """
    Export network graph data to JSON format suitable for graph visualization tools.

    Args:
        graph_data: Graph data with nodes and edges

    Returns:
        tuple[str, str]: (json_content, filename)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"malla_network_graph_{timestamp}.json"

    # Format for common graph tools (e.g., D3.js, Cytoscape)
    formatted_data = {
        "metadata": {
            "exported_at": datetime.now().isoformat(),
            "format": "network_graph",
            "tool": "Malla Meshtastic Monitor",
        },
        "graph": graph_data,
    }

    return export_to_json(formatted_data, pretty=True, filename=filename)


def create_geojson_from_nodes(nodes: list[dict]) -> dict:
    """
    Create GeoJSON from node location data.

    Args:
        nodes: List of node dictionaries with latitude/longitude

    Returns:
        dict: GeoJSON FeatureCollection
    """
    features = []

    for node in nodes:
        lat = node.get("latitude")
        lon = node.get("longitude")

        # Skip nodes without valid coordinates
        if lat is None or lon is None:
            continue

        try:
            lat_float = float(lat)
            lon_float = float(lon)

            # Basic validation
            if not (-90 <= lat_float <= 90 and -180 <= lon_float <= 180):
                continue

            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon_float, lat_float]},
                "properties": {
                    "node_id": node.get("node_id"),
                    "node_name": node.get("node_name"),
                    "hardware_model": node.get("hardware_model"),
                    "role": node.get("role"),
                    "altitude": node.get("altitude"),
                    "last_seen": node.get("last_seen"),
                },
            }
            features.append(feature)

        except (ValueError, TypeError):
            continue

    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "exported_at": datetime.now().isoformat(),
            "total_nodes": len(features),
        },
    }

    return geojson


def export_nodes_to_geojson(nodes: list[dict]) -> tuple[str, str]:
    """
    Export node locations to GeoJSON format.

    Args:
        nodes: List of node dictionaries

    Returns:
        tuple[str, str]: (geojson_content, filename)
    """
    geojson = create_geojson_from_nodes(nodes)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"malla_nodes_{timestamp}.geojson"

    geojson_content = json.dumps(geojson, indent=2, ensure_ascii=False)

    logger.info(
        f"Exported {len(geojson['features'])} nodes to GeoJSON ({len(geojson_content)} bytes)"
    )

    return geojson_content, filename


def get_export_formats() -> dict:
    """
    Get available export formats and their descriptions.

    Returns:
        dict: Export format information
    """
    return {
        "csv": {
            "name": "CSV (Comma-Separated Values)",
            "mime_type": "text/csv",
            "extension": ".csv",
            "description": "Spreadsheet-compatible format for Excel, Google Sheets, etc.",
            "supports": ["packets", "nodes", "analytics"],
        },
        "json": {
            "name": "JSON (JavaScript Object Notation)",
            "mime_type": "application/json",
            "extension": ".json",
            "description": "Structured data format for programmatic access",
            "supports": ["packets", "nodes", "analytics", "network_graph"],
        },
        "geojson": {
            "name": "GeoJSON",
            "mime_type": "application/geo+json",
            "extension": ".geojson",
            "description": "Geographic data format for mapping applications",
            "supports": ["nodes"],
        },
    }
