"""
Admin routes for remote node administration.

Provides REST API endpoints and page routes for the Mesh Admin functionality.
"""

import json
import logging
import time
from typing import Any

from flask import (
    Blueprint,
    Response,
    jsonify,
    render_template,
    request,
    stream_with_context,
)

from ..config import get_config
from ..database.admin_repository import AdminRepository
from ..services.admin_service import ConfigType, get_admin_service
from ..services.serial_publisher import discover_serial_ports, get_serial_publisher
from ..services.tcp_publisher import get_tcp_publisher
from ..utils.node_utils import convert_node_id

logger = logging.getLogger(__name__)

admin_bp = Blueprint("admin", __name__)


@admin_bp.before_request
def check_admin_enabled():
    """Check if admin features are enabled before processing any admin request."""
    config = get_config()
    if not config.admin_enabled:
        # Allow a special status endpoint to check if admin is enabled
        if request.endpoint == "admin.api_admin_enabled":
            return None
        # For the main page, render a disabled message
        if request.endpoint == "admin.admin_page":
            return render_template(
                "admin_disabled.html",
            ), 403
        # For API endpoints, return 403 Forbidden
        return jsonify(
            {
                "error": "Admin features are disabled",
                "admin_enabled": False,
                "message": "Remote administration is disabled in this installation. "
                "Set MALLA_ADMIN_ENABLED=true to enable.",
            }
        ), 403
    return None


# ============================================================================
# Page Routes
# ============================================================================


@admin_bp.route("/admin")
def admin_page():
    """Main mesh admin page."""
    logger.info("Admin page accessed")
    try:
        admin_service = get_admin_service()
        connection_status = admin_service.get_connection_status()
        administrable_nodes = admin_service.get_administrable_nodes()

        return render_template(
            "mesh_admin.html",
            connection_status=connection_status,
            administrable_nodes=administrable_nodes,
        )
    except Exception as e:
        logger.error(f"Error rendering admin page: {e}")
        return f"Admin page error: {e}", 500


# ============================================================================
# API Routes - Status
# ============================================================================


@admin_bp.route("/api/admin/enabled")
def api_admin_enabled():
    """Check if admin features are enabled."""
    config = get_config()
    return jsonify(
        {
            "admin_enabled": config.admin_enabled,
        }
    )


@admin_bp.route("/api/admin/status")
def api_admin_status():
    """Get admin connection status."""
    try:
        admin_service = get_admin_service()
        status = admin_service.get_connection_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting admin status: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/gateway", methods=["POST"])
def api_set_gateway():
    """Set the gateway node for admin operations."""
    try:
        data = request.get_json()
        if not data or "node_id" not in data:
            return jsonify({"error": "node_id is required"}), 400

        node_id = convert_node_id(data["node_id"])

        admin_service = get_admin_service()
        admin_service.set_gateway_node(node_id)

        return jsonify(
            {
                "success": True,
                "gateway_node_id": node_id,
                "gateway_node_hex": f"!{node_id:08x}",
            }
        )
    except Exception as e:
        logger.error(f"Error setting gateway: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/connection-type", methods=["POST"])
def api_set_connection_type():
    """Set the connection type for admin operations."""
    try:
        data = request.get_json()
        if not data or "connection_type" not in data:
            return jsonify({"error": "connection_type is required"}), 400

        conn_type = data["connection_type"]
        valid_types = ["mqtt", "tcp", "serial"]

        if conn_type not in valid_types:
            return jsonify(
                {"error": f"Invalid connection_type. Must be one of: {valid_types}"}
            ), 400

        admin_service = get_admin_service()
        if admin_service.set_connection_type(conn_type):
            return jsonify(
                {
                    "success": True,
                    "connection_type": conn_type,
                }
            )
        else:
            return jsonify(
                {"error": f"Failed to set connection type to {conn_type}"}
            ), 400

    except Exception as e:
        logger.error(f"Error setting connection type: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/tcp/connect", methods=["POST"])
def api_tcp_connect():
    """Connect to a Meshtastic node via TCP."""
    try:
        data = request.get_json() or {}

        # Get optional host/port overrides from request
        host = data.get("host")
        port = data.get("port")

        tcp_publisher = get_tcp_publisher()

        # Update host/port if provided
        if host or port:
            tcp_publisher.set_connection_params(
                host=host,
                port=int(port) if port else None,
            )

        if tcp_publisher.connect():
            local_node_id = tcp_publisher.get_local_node_id()
            local_node_name = tcp_publisher.get_local_node_name()

            # Set connection type to TCP
            admin_service = get_admin_service()
            admin_service.set_connection_type("tcp")

            return jsonify(
                {
                    "success": True,
                    "connected": True,
                    "host": tcp_publisher.tcp_host,
                    "port": tcp_publisher.tcp_port,
                    "local_node_id": local_node_id,
                    "local_node_hex": f"!{local_node_id:08x}"
                    if local_node_id
                    else None,
                    "local_node_name": local_node_name,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "connected": False,
                    "error": f"Failed to connect to {tcp_publisher.tcp_host}:{tcp_publisher.tcp_port}",
                }
            ), 500

    except Exception as e:
        logger.error(f"Error connecting via TCP: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/tcp/disconnect", methods=["POST"])
def api_tcp_disconnect():
    """Disconnect from the TCP-connected Meshtastic node."""
    try:
        tcp_publisher = get_tcp_publisher()
        tcp_publisher.disconnect()

        # Set connection type back to MQTT
        admin_service = get_admin_service()
        admin_service.set_connection_type("mqtt")

        return jsonify(
            {
                "success": True,
                "connected": False,
            }
        )

    except Exception as e:
        logger.error(f"Error disconnecting TCP: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/tcp/health")
def api_tcp_health():
    """Check the health of the TCP connection.

    Query parameters:
        thorough: If 'true', sends a heartbeat to verify connection is truly alive
    """
    try:
        tcp_publisher = get_tcp_publisher()
        # Use thorough check (with heartbeat) if requested
        send_heartbeat = request.args.get("thorough", "false").lower() == "true"
        health = tcp_publisher.check_connection_health(send_heartbeat=send_heartbeat)
        return jsonify(health)
    except Exception as e:
        logger.error(f"Error checking TCP health: {e}")
        return jsonify({"error": str(e), "healthy": False}), 500


@admin_bp.route("/api/admin/tcp/reconnect", methods=["POST"])
def api_tcp_reconnect():
    """Force reconnection to the TCP node.

    This is useful when the connection appears to be stale.
    """
    try:
        tcp_publisher = get_tcp_publisher()

        # Get current settings before reconnecting
        host = tcp_publisher.tcp_host
        port = tcp_publisher.tcp_port

        # Reconnect
        success = tcp_publisher.reconnect()

        if success:
            # Set connection type to TCP
            admin_service = get_admin_service()
            admin_service.set_connection_type("tcp")

            return jsonify(
                {
                    "success": True,
                    "connected": True,
                    "host": host,
                    "port": port,
                    "message": "Reconnected successfully",
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "connected": False,
                    "host": host,
                    "port": port,
                    "error": "Failed to reconnect",
                }
            ), 500

    except Exception as e:
        logger.error(f"Error reconnecting TCP: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Serial/USB Connection
# ============================================================================


@admin_bp.route("/api/admin/serial/ports")
def api_serial_ports():
    """Discover available serial ports.

    Query Parameters:
        probe: If "true", attempt to connect and identify Meshtastic devices (slower)
    """
    try:
        probe = request.args.get("probe", "false").lower() == "true"
        ports = discover_serial_ports(probe_devices=probe)
        return jsonify(
            {
                "success": True,
                "ports": ports,
                "count": len(ports),
                "probed": probe,
            }
        )
    except Exception as e:
        logger.error(f"Error discovering serial ports: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/serial/connect", methods=["POST"])
def api_serial_connect():
    """Connect to a Meshtastic node via USB/Serial."""
    try:
        data = request.get_json() or {}
        port = data.get("port")

        if not port:
            return jsonify({"error": "Serial port is required"}), 400

        serial_publisher = get_serial_publisher()

        # Disconnect TCP if connected
        tcp_publisher = get_tcp_publisher()
        if tcp_publisher.is_connected:
            tcp_publisher.disconnect()

        if serial_publisher.connect(port=port):
            local_node_id = serial_publisher.get_local_node_id()
            local_node_name = serial_publisher.get_local_node_name()

            # Set connection type to Serial
            admin_service = get_admin_service()
            admin_service.set_connection_type("serial")

            return jsonify(
                {
                    "success": True,
                    "connected": True,
                    "port": port,
                    "local_node_id": local_node_id,
                    "local_node_hex": f"!{local_node_id:08x}"
                    if local_node_id
                    else None,
                    "local_node_name": local_node_name,
                }
            )
        else:
            # Check if port exists
            import os

            if not os.path.exists(port):
                error_msg = (
                    f"Serial port {port} does not exist. "
                    "Make sure the device is connected and the port is accessible."
                )
            else:
                error_msg = (
                    f"Failed to connect to {port}. "
                    "Check if another application is using the port or if you have permission."
                )
            return jsonify(
                {
                    "success": False,
                    "connected": False,
                    "error": error_msg,
                }
            ), 400

    except Exception as e:
        logger.error(f"Error connecting via Serial: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/serial/disconnect", methods=["POST"])
def api_serial_disconnect():
    """Disconnect from the Serial-connected Meshtastic node."""
    try:
        serial_publisher = get_serial_publisher()
        serial_publisher.disconnect()

        # Set connection type back to MQTT
        admin_service = get_admin_service()
        admin_service.set_connection_type("mqtt")

        return jsonify(
            {
                "success": True,
                "connected": False,
            }
        )

    except Exception as e:
        logger.error(f"Error disconnecting Serial: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Administrable Nodes
# ============================================================================


@admin_bp.route("/api/admin/nodes")
def api_administrable_nodes():
    """Get list of administrable nodes."""
    try:
        admin_service = get_admin_service()
        nodes = admin_service.get_administrable_nodes()
        return jsonify(
            {
                "nodes": nodes,
                "count": len(nodes),
            }
        )
    except Exception as e:
        logger.error(f"Error getting administrable nodes: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/node/<node_id>/check")
def api_check_node_administrable(node_id):
    """Check if a specific node is administrable."""
    try:
        node_id_int = convert_node_id(node_id)
        is_admin = AdminRepository.is_node_administrable(node_id_int)
        details = AdminRepository.get_administrable_node_details(node_id_int)

        return jsonify(
            {
                "node_id": node_id_int,
                "hex_id": f"!{node_id_int:08x}",
                "administrable": is_admin,
                "details": details,
            }
        )
    except Exception as e:
        logger.error(f"Error checking node: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/node/<node_id>/status")
def api_node_admin_status(node_id):
    """
    Get admin session status for a specific node.

    Returns detailed information about whether the admin channel is ready
    to send commands to this node, including connection status, gateway
    configuration, and node administrability.
    """
    try:
        node_id_int = convert_node_id(node_id)

        admin_service = get_admin_service()
        status = admin_service.get_node_admin_status(node_id_int)

        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting node admin status: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/node/<node_id>/test", methods=["POST"])
def api_test_node_admin(node_id):
    """Test if a node is administrable by sending a device metadata request."""
    try:
        node_id_int = convert_node_id(node_id)

        admin_service = get_admin_service()
        result = admin_service.test_node_admin(node_id_int)

        if result.success:
            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "hex_id": f"!{node_id_int:08x}",
                    "administrable": True,
                    "response": result.response,
                    "log_id": result.log_id,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "node_id": node_id_int,
                    "hex_id": f"!{node_id_int:08x}",
                    "administrable": False,
                    "error": result.error,
                    "log_id": result.log_id,
                }
            ), 200  # Still 200 because the request was valid

    except Exception as e:
        logger.error(f"Error testing node admin: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Configuration
# ============================================================================


@admin_bp.route("/api/admin/node/<node_id>/config/<config_type>")
def api_get_node_config(node_id, config_type):
    """Get configuration from a remote node."""
    try:
        node_id_int = convert_node_id(node_id)

        # Map config type string to enum
        config_type_map = {
            "device": ConfigType.DEVICE,
            "position": ConfigType.POSITION,
            "power": ConfigType.POWER,
            "network": ConfigType.NETWORK,
            "display": ConfigType.DISPLAY,
            "lora": ConfigType.LORA,
            "bluetooth": ConfigType.BLUETOOTH,
            "security": ConfigType.SECURITY,
        }

        if config_type.lower() not in config_type_map:
            return jsonify(
                {
                    "error": f"Invalid config type. Valid types: {list(config_type_map.keys())}",
                }
            ), 400

        # Get retry parameters from query string
        max_retries = request.args.get("max_retries", 3, type=int)
        retry_delay = request.args.get("retry_delay", 2.0, type=float)
        timeout = request.args.get("timeout", 30.0, type=float)

        admin_service = get_admin_service()
        result = admin_service.get_config(
            target_node_id=node_id_int,
            config_type=config_type_map[config_type.lower()],
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
        )

        if result.success:
            # Include field schema for the UI
            schema = admin_service.get_config_schema(config_type.lower())
            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "config_type": config_type,
                    "config": result.response,
                    "schema": schema,
                    "log_id": result.log_id,
                    "attempts": result.attempts,
                    "retry_info": result.retry_info,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": result.error,
                    "log_id": result.log_id,
                    "attempts": result.attempts,
                    "retry_info": result.retry_info,
                }
            ), 200

    except Exception as e:
        logger.error(f"Error getting node config: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/node/<node_id>/moduleconfig/<module_type>")
def api_get_node_module_config(node_id, module_type):
    """Get module configuration from a remote node."""
    from ..services.admin_service import ModuleConfigType

    try:
        node_id_int = convert_node_id(node_id)

        # Map module type string to enum
        module_type_map = {
            "mqtt": ModuleConfigType.MQTT,
            "serial": ModuleConfigType.SERIAL,
            "extnotif": ModuleConfigType.EXTNOTIF,
            "storeforward": ModuleConfigType.STOREFORWARD,
            "rangetest": ModuleConfigType.RANGETEST,
            "telemetry": ModuleConfigType.TELEMETRY,
            "cannedmsg": ModuleConfigType.CANNEDMSG,
            "audio": ModuleConfigType.AUDIO,
            "remotehardware": ModuleConfigType.REMOTEHARDWARE,
            "neighborinfo": ModuleConfigType.NEIGHBORINFO,
            "ambientlighting": ModuleConfigType.AMBIENTLIGHTING,
            "detectionsensor": ModuleConfigType.DETECTIONSENSOR,
            "paxcounter": ModuleConfigType.PAXCOUNTER,
        }

        if module_type.lower() not in module_type_map:
            return jsonify(
                {
                    "error": f"Invalid module type. Valid types: {list(module_type_map.keys())}",
                }
            ), 400

        # Get retry parameters from query string
        max_retries = request.args.get("max_retries", 3, type=int)
        retry_delay = request.args.get("retry_delay", 2.0, type=float)
        timeout = request.args.get("timeout", 30.0, type=float)

        admin_service = get_admin_service()
        result = admin_service.get_module_config(
            target_node_id=node_id_int,
            module_config_type=module_type_map[module_type.lower()],
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
        )

        if result.success:
            from ..services.config_metadata import get_module_config_schema

            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "module_type": module_type,
                    "config": result.response,
                    "schema": get_module_config_schema(module_type),
                    "log_id": result.log_id,
                    "attempts": result.attempts,
                    "retry_info": result.retry_info,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": result.error,
                    "log_id": result.log_id,
                    "attempts": result.attempts,
                    "retry_info": result.retry_info,
                }
            ), 200

    except Exception as e:
        logger.error(f"Error getting node module config: {e}")
        return jsonify({"error": str(e)}), 500


# =========================
# Node Backup Endpoints
# =========================


@admin_bp.route("/api/admin/backups")
def api_get_backups():
    """Get all node backups, optionally filtered by node."""
    try:
        node_id = request.args.get("node_id")
        node_id_int = convert_node_id(node_id) if node_id else None
        limit = request.args.get("limit", 100, type=int)

        backups = AdminRepository.get_backups(node_id=node_id_int, limit=limit)

        # Get current node names from node_info table
        from ..database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT node_id, long_name, short_name, hex_id FROM node_info")
        node_info_rows = cursor.fetchall()
        conn.close()

        # Build lookup of current node names by node_id
        current_node_names = {}
        for row in node_info_rows:
            current_node_names[row["node_id"]] = {
                "current_long_name": row["long_name"],
                "current_short_name": row["short_name"],
                "current_hex_id": row["hex_id"],
            }

        # Parse backup_data JSON for summary info and add current node names
        for backup in backups:
            try:
                data = json.loads(backup.get("backup_data", "{}"))
                backup["config_summary"] = {
                    "core_configs": len(data.get("core_configs", {})),
                    "module_configs": len(data.get("module_configs", {})),
                    "channels": len(data.get("channels", {})),
                }
                # Remove large data from list response
                del backup["backup_data"]
            except (json.JSONDecodeError, KeyError):
                backup["config_summary"] = {"error": "Invalid backup data"}

            # Add current node name info
            node_id_key = backup.get("node_id")
            if node_id_key in current_node_names:
                backup.update(current_node_names[node_id_key])

        return jsonify({"backups": backups})
    except Exception as e:
        logger.error(f"Error getting backups: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/backups/<int:backup_id>")
def api_get_backup(backup_id):
    """Get a specific backup by ID with full data."""
    try:
        backup = AdminRepository.get_backup(backup_id)
        if not backup:
            return jsonify({"error": "Backup not found"}), 404

        # Parse backup_data JSON
        try:
            backup["backup_data"] = json.loads(backup.get("backup_data", "{}"))
        except json.JSONDecodeError:
            backup["backup_data"] = {"error": "Invalid backup data"}

        return jsonify(backup)
    except Exception as e:
        logger.error(f"Error getting backup: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/backups", methods=["POST"])
def api_create_backup():
    """Create a new backup from a remote node (streaming SSE)."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        node_id = data.get("node_id")
        backup_name = data.get("backup_name")

        if not node_id:
            return jsonify({"error": "node_id is required"}), 400
        if not backup_name:
            return jsonify({"error": "backup_name is required"}), 400

        node_id_int = convert_node_id(node_id)
        description = data.get("description", "")
        max_retries = data.get("max_retries", 3)
        retry_delay = data.get("retry_delay", 2.0)
        timeout = data.get("timeout", 30.0)

        admin_service = get_admin_service()
        result = admin_service.create_backup(
            target_node_id=node_id_int,
            backup_name=backup_name,
            description=description,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
        )

        if result.success:
            return jsonify(
                {
                    "success": True,
                    "backup_id": result.response.get("backup_id")
                    if result.response
                    else None,
                    "backup_name": backup_name,
                    "successful_configs": result.response.get("successful_configs", [])
                    if result.response
                    else [],
                    "failed_configs": result.response.get("failed_configs", [])
                    if result.response
                    else [],
                    "log_id": result.log_id,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": result.error,
                    "log_id": result.log_id,
                }
            ), 200

    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/backups/<int:backup_id>", methods=["DELETE"])
def api_delete_backup(backup_id):
    """Delete a backup."""
    try:
        deleted = AdminRepository.delete_backup(backup_id)
        if deleted:
            return jsonify({"success": True, "message": "Backup deleted"})
        else:
            return jsonify({"error": "Backup not found"}), 404
    except Exception as e:
        logger.error(f"Error deleting backup: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/backups/<int:backup_id>", methods=["PUT"])
def api_update_backup(backup_id):
    """Update backup metadata (name, description)."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        updated = AdminRepository.update_backup(
            backup_id=backup_id,
            backup_name=data.get("backup_name"),
            description=data.get("description"),
        )

        if updated:
            return jsonify({"success": True, "message": "Backup updated"})
        else:
            return jsonify({"error": "Backup not found or no changes"}), 404
    except Exception as e:
        logger.error(f"Error updating backup: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/backups/stream")
def api_create_backup_stream():
    """
    SSE endpoint to create a backup with real-time progress updates.

    Returns a Server-Sent Events stream with progress messages during backup.
    """
    from ..services.admin_service import ConfigType, ModuleConfigType

    node_id_str = request.args.get("node_id")
    backup_name = request.args.get("backup_name")
    description = request.args.get("description", "")

    def generate():
        def send_event(data: dict) -> str:
            return f"data: {json.dumps(data)}\n\n"

        # Validate inputs
        if not node_id_str:
            yield send_event(
                {"complete": True, "success": False, "error": "node_id is required"}
            )
            return
        if not backup_name:
            yield send_event(
                {"complete": True, "success": False, "error": "backup_name is required"}
            )
            return

        # Convert node ID
        try:
            node_id = convert_node_id(node_id_str)
        except (ValueError, TypeError):
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": f"Invalid node_id: {node_id_str}",
                }
            )
            return

        admin_service = get_admin_service()

        # Define all items to fetch
        core_configs = [
            ("DEVICE", ConfigType.DEVICE),
            ("POSITION", ConfigType.POSITION),
            ("POWER", ConfigType.POWER),
            ("NETWORK", ConfigType.NETWORK),
            ("DISPLAY", ConfigType.DISPLAY),
            ("LORA", ConfigType.LORA),
            ("BLUETOOTH", ConfigType.BLUETOOTH),
            ("SECURITY", ConfigType.SECURITY),
        ]

        module_configs = [
            ("MQTT", ModuleConfigType.MQTT),
            ("SERIAL", ModuleConfigType.SERIAL),
            ("EXTNOTIF", ModuleConfigType.EXTNOTIF),
            ("STOREFORWARD", ModuleConfigType.STOREFORWARD),
            ("RANGETEST", ModuleConfigType.RANGETEST),
            ("TELEMETRY", ModuleConfigType.TELEMETRY),
            ("CANNEDMSG", ModuleConfigType.CANNEDMSG),
            ("AUDIO", ModuleConfigType.AUDIO),
            ("REMOTEHARDWARE", ModuleConfigType.REMOTEHARDWARE),
            ("NEIGHBORINFO", ModuleConfigType.NEIGHBORINFO),
            ("AMBIENTLIGHTING", ModuleConfigType.AMBIENTLIGHTING),
            ("DETECTIONSENSOR", ModuleConfigType.DETECTIONSENSOR),
            ("PAXCOUNTER", ModuleConfigType.PAXCOUNTER),
        ]

        channels = list(range(8))

        total_items = len(core_configs) + len(module_configs) + len(channels)
        current_item = 0

        backup_data: dict = {
            "backup_version": 1,
            "target_node_id": node_id,
            "created_at": time.time(),
            "core_configs": {},
            "module_configs": {},
            "channels": {},
        }

        errors: list = []
        successful_configs: list = []

        try:
            # Fetch core configs
            yield send_event(
                {
                    "status": "Fetching core configurations...",
                    "progress": 0,
                    "phase": "core",
                    "current": 0,
                    "total": total_items,
                }
            )

            for name, config_type in core_configs:
                current_item += 1
                progress = int((current_item / total_items) * 100)

                yield send_event(
                    {
                        "status": f"Fetching {name} config...",
                        "progress": progress,
                        "phase": "core",
                        "current": current_item,
                        "total": total_items,
                        "config_name": name,
                    }
                )

                result = admin_service.get_config(
                    target_node_id=node_id,
                    config_type=config_type,
                    max_retries=3,
                    retry_delay=2.0,
                    timeout=30.0,
                )

                if result.success and result.response:
                    backup_data["core_configs"][name.lower()] = result.response
                    successful_configs.append(f"core:{name}")
                    yield send_event(
                        {
                            "status": f"✓ {name} config retrieved",
                            "progress": progress,
                            "phase": "core",
                            "current": current_item,
                            "total": total_items,
                            "config_name": name,
                            "config_success": True,
                        }
                    )
                else:
                    errors.append(f"core:{name}: {result.error or 'Unknown error'}")
                    yield send_event(
                        {
                            "status": f"✗ {name} config failed",
                            "progress": progress,
                            "phase": "core",
                            "current": current_item,
                            "total": total_items,
                            "config_name": name,
                            "config_success": False,
                            "config_error": result.error,
                        }
                    )

            # Fetch module configs
            yield send_event(
                {
                    "status": "Fetching module configurations...",
                    "progress": int((current_item / total_items) * 100),
                    "phase": "module",
                    "current": current_item,
                    "total": total_items,
                }
            )

            for name, module_type in module_configs:
                current_item += 1
                progress = int((current_item / total_items) * 100)

                yield send_event(
                    {
                        "status": f"Fetching {name} module...",
                        "progress": progress,
                        "phase": "module",
                        "current": current_item,
                        "total": total_items,
                        "config_name": name,
                    }
                )

                result = admin_service.get_module_config(
                    target_node_id=node_id,
                    module_config_type=module_type,
                    max_retries=3,
                    retry_delay=2.0,
                    timeout=30.0,
                )

                if result.success and result.response:
                    backup_data["module_configs"][name.lower()] = result.response
                    successful_configs.append(f"module:{name}")
                    yield send_event(
                        {
                            "status": f"✓ {name} module retrieved",
                            "progress": progress,
                            "phase": "module",
                            "current": current_item,
                            "total": total_items,
                            "config_name": name,
                            "config_success": True,
                        }
                    )
                else:
                    errors.append(f"module:{name}: {result.error or 'Unknown error'}")
                    yield send_event(
                        {
                            "status": f"✗ {name} module failed",
                            "progress": progress,
                            "phase": "module",
                            "current": current_item,
                            "total": total_items,
                            "config_name": name,
                            "config_success": False,
                            "config_error": result.error,
                        }
                    )

            # Fetch channels
            yield send_event(
                {
                    "status": "Fetching channel configurations...",
                    "progress": int((current_item / total_items) * 100),
                    "phase": "channels",
                    "current": current_item,
                    "total": total_items,
                }
            )

            for channel_idx in channels:
                current_item += 1
                progress = int((current_item / total_items) * 100)

                yield send_event(
                    {
                        "status": f"Fetching Channel {channel_idx}...",
                        "progress": progress,
                        "phase": "channels",
                        "current": current_item,
                        "total": total_items,
                        "config_name": f"Channel {channel_idx}",
                    }
                )

                result = admin_service.get_channel(
                    target_node_id=node_id,
                    channel_index=channel_idx,
                    max_retries=3,
                    retry_delay=2.0,
                    timeout=30.0,
                )

                if result.success and result.response:
                    backup_data["channels"][str(channel_idx)] = result.response
                    successful_configs.append(f"channel:{channel_idx}")
                    yield send_event(
                        {
                            "status": f"✓ Channel {channel_idx} retrieved",
                            "progress": progress,
                            "phase": "channels",
                            "current": current_item,
                            "total": total_items,
                            "config_name": f"Channel {channel_idx}",
                            "config_success": True,
                        }
                    )
                else:
                    errors.append(
                        f"channel:{channel_idx}: {result.error or 'Unknown error'}"
                    )
                    yield send_event(
                        {
                            "status": f"✗ Channel {channel_idx} failed",
                            "progress": progress,
                            "phase": "channels",
                            "current": current_item,
                            "total": total_items,
                            "config_name": f"Channel {channel_idx}",
                            "config_success": False,
                            "config_error": result.error,
                        }
                    )

            # Save backup if we got at least some configs
            if successful_configs:
                yield send_event(
                    {
                        "status": "Saving backup to database...",
                        "progress": 98,
                        "phase": "saving",
                    }
                )

                # Get node info for metadata
                node_long_name = None
                node_short_name = None
                node_hex_id = f"!{node_id:08x}"

                if "device" in backup_data["core_configs"]:
                    device_config = backup_data["core_configs"]["device"]
                    node_long_name = device_config.get("device", {}).get("owner", None)
                    node_short_name = device_config.get("device", {}).get(
                        "owner_short", None
                    )

                backup_id = AdminRepository.create_backup(
                    node_id=node_id,
                    backup_name=backup_name,
                    backup_data=json.dumps(backup_data),
                    description=description,
                    node_long_name=node_long_name,
                    node_short_name=node_short_name,
                    node_hex_id=node_hex_id,
                )

                yield send_event(
                    {
                        "complete": True,
                        "success": True,
                        "backup_id": backup_id,
                        "backup_name": backup_name,
                        "successful_configs": successful_configs,
                        "failed_configs": errors,
                        "total_configs": len(successful_configs) + len(errors),
                    }
                )
            else:
                yield send_event(
                    {
                        "complete": True,
                        "success": False,
                        "error": "Failed to retrieve any configuration from node",
                        "failed_configs": errors,
                    }
                )

        except Exception as e:
            logger.error(f"Error during backup stream: {e}")
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": str(e),
                }
            )

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@admin_bp.route("/api/admin/backups/job", methods=["POST"])
def api_create_backup_job():
    """
    Queue a backup job for background execution.

    This allows the user to start a backup and check back later for the result.
    The backup will continue even if the browser is closed.

    Request body (JSON):
        node_id: Node ID to backup (required)
        backup_name: Name for the backup (required)
        description: Optional description

    Returns:
        job_id: ID of the queued job
        queue_position: Position in the queue
        status: "queued"
    """
    from ..database.job_repository import JobType
    from ..services.job_service import get_job_service

    try:
        data = request.get_json() or {}

        node_id_str = data.get("node_id")
        backup_name = data.get("backup_name")
        description = data.get("description", "")
        # Optional: custom inter-request delay (auto-calculated if not provided)
        inter_request_delay = data.get("inter_request_delay")

        if not node_id_str:
            return jsonify({"error": "node_id is required"}), 400
        if not backup_name:
            return jsonify({"error": "backup_name is required"}), 400

        node_id = convert_node_id(node_id_str)

        job_data = {
            "backup_name": backup_name,
            "description": description,
        }

        # Only include delay if explicitly set (otherwise auto-calculated)
        if inter_request_delay is not None:
            try:
                job_data["inter_request_delay"] = float(inter_request_delay)
            except (TypeError, ValueError):
                return jsonify({"error": "inter_request_delay must be a number"}), 400

        job_service = get_job_service()
        result = job_service.queue_job(
            job_type=JobType.BACKUP,
            job_name=f"Backup: {backup_name}",
            job_data=job_data,
            target_node_id=node_id,
        )

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 409  # Conflict with existing job

    except ValueError as e:
        return jsonify({"error": f"Invalid node_id: {e}"}), 400
    except Exception as e:
        logger.error(f"Error queuing backup job: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/backups/restore/stream")
def api_restore_backup_stream():
    """
    SSE endpoint to restore a backup to a node with real-time progress updates.

    This performs a safe restore by:
    1. Validating the backup data
    2. Optionally creating a pre-restore backup of the target node
    3. Restoring core configs first (most critical)
    4. Restoring module configs
    5. Restoring channels (skipping primary channel by default for safety)
    6. Optionally rebooting the node to apply changes

    Returns a Server-Sent Events stream with progress messages during restore.
    """
    from ..services.admin_service import ConfigType, ModuleConfigType

    backup_id_str = request.args.get("backup_id")
    target_node_str = request.args.get("target_node_id")
    skip_primary_channel = request.args.get("skip_primary_channel", "true") == "true"
    skip_lora = request.args.get("skip_lora", "false") == "true"
    skip_security = request.args.get("skip_security", "true") == "true"
    reboot_after = request.args.get("reboot_after", "false") == "true"

    def generate():
        def send_event(data: dict) -> str:
            return f"data: {json.dumps(data)}\n\n"

        # Validate inputs
        if not backup_id_str:
            yield send_event(
                {"complete": True, "success": False, "error": "backup_id is required"}
            )
            return
        if not target_node_str:
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": "target_node_id is required",
                }
            )
            return

        # Convert IDs
        try:
            backup_id = int(backup_id_str)
        except (ValueError, TypeError):
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": f"Invalid backup_id: {backup_id_str}",
                }
            )
            return

        try:
            target_node_id = convert_node_id(target_node_str)
        except (ValueError, TypeError):
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": f"Invalid target_node_id: {target_node_str}",
                }
            )
            return

        # Fetch backup from database
        yield send_event(
            {
                "status": "Loading backup data...",
                "progress": 0,
                "phase": "init",
            }
        )

        backup = AdminRepository.get_backup(backup_id)
        if not backup:
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": f"Backup not found: {backup_id}",
                }
            )
            return

        # Parse backup data
        try:
            backup_data = (
                json.loads(backup["backup_data"])
                if isinstance(backup["backup_data"], str)
                else backup["backup_data"]
            )
        except json.JSONDecodeError as e:
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": f"Invalid backup data format: {e}",
                }
            )
            return

        admin_service = get_admin_service()

        # Define what we're restoring
        core_configs = backup_data.get("core_configs", {})
        module_configs = backup_data.get("module_configs", {})
        channels = backup_data.get("channels", {})

        # Build list of items to restore
        items_to_restore = []

        # Core configs (map keys to ConfigType)
        core_config_map = {
            "device": ConfigType.DEVICE,
            "position": ConfigType.POSITION,
            "power": ConfigType.POWER,
            "network": ConfigType.NETWORK,
            "display": ConfigType.DISPLAY,
            "lora": ConfigType.LORA,
            "bluetooth": ConfigType.BLUETOOTH,
            "security": ConfigType.SECURITY,
        }

        for config_name, config_type in core_config_map.items():
            if config_name in core_configs:
                # Apply skip filters
                if config_name == "lora" and skip_lora:
                    continue
                if config_name == "security" and skip_security:
                    continue
                items_to_restore.append(("core", config_name, config_type))

        # Module configs (map keys to ModuleConfigType)
        module_config_map = {
            "mqtt": ModuleConfigType.MQTT,
            "serial": ModuleConfigType.SERIAL,
            "extnotif": ModuleConfigType.EXTNOTIF,
            "storeforward": ModuleConfigType.STOREFORWARD,
            "rangetest": ModuleConfigType.RANGETEST,
            "telemetry": ModuleConfigType.TELEMETRY,
            "cannedmsg": ModuleConfigType.CANNEDMSG,
            "audio": ModuleConfigType.AUDIO,
            "remotehardware": ModuleConfigType.REMOTEHARDWARE,
            "neighborinfo": ModuleConfigType.NEIGHBORINFO,
            "ambientlighting": ModuleConfigType.AMBIENTLIGHTING,
            "detectionsensor": ModuleConfigType.DETECTIONSENSOR,
            "paxcounter": ModuleConfigType.PAXCOUNTER,
        }

        for module_name, module_type in module_config_map.items():
            if module_name in module_configs:
                items_to_restore.append(("module", module_name, module_type))

        # Channels (skip primary if requested)
        for channel_idx_str in channels.keys():
            channel_idx = int(channel_idx_str)
            if channel_idx == 0 and skip_primary_channel:
                continue
            items_to_restore.append(("channel", channel_idx_str, channel_idx))

        total_items = len(items_to_restore)
        if total_items == 0:
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": "No configurations to restore in backup",
                }
            )
            return

        current_item = 0
        successful_restores = []
        errors = []

        yield send_event(
            {
                "status": f"Starting restore of {total_items} configurations...",
                "progress": 1,
                "phase": "restoring",
                "total_items": total_items,
                "skip_primary_channel": skip_primary_channel,
                "skip_lora": skip_lora,
                "skip_security": skip_security,
            }
        )

        try:
            # Restore core configs first
            for item_type, item_name, item_enum in items_to_restore:
                current_item += 1
                progress = int((current_item / total_items) * 95) + 2

                if item_type == "core":
                    yield send_event(
                        {
                            "status": f"Restoring {item_name.upper()} config...",
                            "progress": progress,
                            "phase": "core",
                            "current": current_item,
                            "total": total_items,
                            "config_name": item_name.upper(),
                        }
                    )

                    # Extract the config data - need to handle nested structure
                    config_data = core_configs.get(item_name, {})
                    # The backup stores data like {"device": {...}} - extract inner
                    if item_name in config_data:
                        config_data = config_data[item_name]

                    result = admin_service.set_config(
                        target_node_id=target_node_id,
                        config_type=item_enum,
                        config_data=config_data,
                    )

                    if result.success:
                        successful_restores.append(f"core:{item_name}")
                        yield send_event(
                            {
                                "status": f"✓ {item_name.upper()} config restored",
                                "progress": progress,
                                "phase": "core",
                                "current": current_item,
                                "total": total_items,
                                "config_name": item_name.upper(),
                                "config_success": True,
                            }
                        )
                    else:
                        error_msg = result.error or "Unknown error"
                        errors.append(f"core:{item_name}: {error_msg}")
                        yield send_event(
                            {
                                "status": f"✗ {item_name.upper()} config failed",
                                "progress": progress,
                                "phase": "core",
                                "current": current_item,
                                "total": total_items,
                                "config_name": item_name.upper(),
                                "config_success": False,
                                "config_error": error_msg,
                            }
                        )

                elif item_type == "module":
                    yield send_event(
                        {
                            "status": f"Restoring {item_name.upper()} module...",
                            "progress": progress,
                            "phase": "module",
                            "current": current_item,
                            "total": total_items,
                            "config_name": item_name.upper(),
                        }
                    )

                    # Extract module config data
                    module_data = module_configs.get(item_name, {})
                    # Handle nested structure like {"mqtt": {...}}
                    if item_name in module_data:
                        module_data = module_data[item_name]

                    result = admin_service.set_module_config(
                        target_node_id=target_node_id,
                        module_config_type=item_enum,
                        module_data=module_data,
                    )

                    if result.success:
                        successful_restores.append(f"module:{item_name}")
                        yield send_event(
                            {
                                "status": f"✓ {item_name.upper()} module restored",
                                "progress": progress,
                                "phase": "module",
                                "current": current_item,
                                "total": total_items,
                                "config_name": item_name.upper(),
                                "config_success": True,
                            }
                        )
                    else:
                        error_msg = result.error or "Unknown error"
                        errors.append(f"module:{item_name}: {error_msg}")
                        yield send_event(
                            {
                                "status": f"✗ {item_name.upper()} module failed",
                                "progress": progress,
                                "phase": "module",
                                "current": current_item,
                                "total": total_items,
                                "config_name": item_name.upper(),
                                "config_success": False,
                                "config_error": error_msg,
                            }
                        )

                elif item_type == "channel":
                    channel_idx = item_enum
                    yield send_event(
                        {
                            "status": f"Restoring Channel {channel_idx}...",
                            "progress": progress,
                            "phase": "channels",
                            "current": current_item,
                            "total": total_items,
                            "config_name": f"Channel {channel_idx}",
                        }
                    )

                    # Extract channel data
                    channel_data = channels.get(item_name, {})
                    # Handle nested structure - check for various formats
                    if "channel" in channel_data:
                        # Old format: {"channel": {"role": 1, "settings": {...}}}
                        channel_info = channel_data["channel"]
                        set_channel_data = {
                            "role": channel_info.get("role", 0),
                            "name": channel_info.get("settings", {}).get("name", ""),
                            "psk": channel_info.get("settings", {}).get("psk", ""),
                            "position_precision": channel_info.get("settings", {})
                            .get("module_settings", {})
                            .get("position_precision", 0),
                        }
                    elif "settings" in channel_data:
                        # Current format: {"role": 1, "settings": {"name": ..., "psk": ...}}
                        settings = channel_data.get("settings", {})
                        set_channel_data = {
                            "role": channel_data.get("role", 0),
                            "name": settings.get("name", ""),
                            "psk": settings.get("psk", ""),
                            "position_precision": settings.get(
                                "module_settings", {}
                            ).get("position_precision", 0),
                        }
                    else:
                        # Flat format: {"role": 1, "name": ..., "psk": ...}
                        set_channel_data = channel_data

                    result = admin_service.set_channel(
                        target_node_id=target_node_id,
                        channel_index=channel_idx,
                        channel_data=set_channel_data,
                    )

                    if result.success:
                        successful_restores.append(f"channel:{channel_idx}")
                        yield send_event(
                            {
                                "status": f"✓ Channel {channel_idx} restored",
                                "progress": progress,
                                "phase": "channels",
                                "current": current_item,
                                "total": total_items,
                                "config_name": f"Channel {channel_idx}",
                                "config_success": True,
                            }
                        )
                    else:
                        error_msg = result.error or "Unknown error"
                        errors.append(f"channel:{channel_idx}: {error_msg}")
                        yield send_event(
                            {
                                "status": f"✗ Channel {channel_idx} failed",
                                "progress": progress,
                                "phase": "channels",
                                "current": current_item,
                                "total": total_items,
                                "config_name": f"Channel {channel_idx}",
                                "config_success": False,
                                "config_error": error_msg,
                            }
                        )

                # Small delay between configs to avoid overwhelming the node
                time.sleep(0.5)

            # Reboot if requested and restore was successful
            if reboot_after and successful_restores:
                yield send_event(
                    {
                        "status": "Sending reboot command to apply changes...",
                        "progress": 98,
                        "phase": "reboot",
                    }
                )

                reboot_result = admin_service.reboot_node(
                    target_node_id=target_node_id,
                    delay_seconds=5,
                )

                if reboot_result.success:
                    yield send_event(
                        {
                            "status": "Node will reboot in 5 seconds",
                            "progress": 99,
                            "phase": "reboot",
                            "reboot_sent": True,
                        }
                    )
                else:
                    yield send_event(
                        {
                            "status": f"Reboot command failed: {reboot_result.error}",
                            "progress": 99,
                            "phase": "reboot",
                            "reboot_sent": False,
                            "reboot_error": reboot_result.error,
                        }
                    )

            # Final result
            if successful_restores:
                yield send_event(
                    {
                        "complete": True,
                        "success": True,
                        "message": f"Restored {len(successful_restores)} configurations",
                        "successful_restores": successful_restores,
                        "failed_restores": errors,
                        "total_restored": len(successful_restores),
                        "total_failed": len(errors),
                        "reboot_after": reboot_after,
                    }
                )
            else:
                yield send_event(
                    {
                        "complete": True,
                        "success": False,
                        "error": "Failed to restore any configurations",
                        "failed_restores": errors,
                    }
                )

        except Exception as e:
            logger.error(f"Error during restore stream: {e}")
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": str(e),
                }
            )

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@admin_bp.route("/api/admin/backups/restore/job", methods=["POST"])
def api_restore_backup_job():
    """
    Queue a restore job for background execution.

    This allows the user to start a restore and check back later for the result.
    The restore will continue even if the browser is closed.

    Request body (JSON):
        backup_id: ID of the backup to restore (required)
        target_node_id: Node ID to restore to (required)
        skip_lora: Skip restoring LoRa config (default: false)
        skip_security: Skip restoring security config (default: true)
        reboot_after: Reboot node after restore (default: false)
        selected_core_configs: List of core config names to restore (optional, default: all)
        selected_module_configs: List of module config names to restore (optional, default: all)
        selected_channels: List of channel indices to restore (optional, default: all)

    Returns:
        job_id: ID of the queued job
        queue_position: Position in the queue
        status: "queued"
    """
    from ..database.job_repository import JobType
    from ..services.job_service import get_job_service

    try:
        data = request.get_json() or {}

        backup_id = data.get("backup_id")
        target_node_str = data.get("target_node_id")
        skip_lora = data.get("skip_lora", False)
        skip_security = data.get("skip_security", True)
        reboot_after = data.get("reboot_after", False)

        # Selective restore options
        selected_core_configs = data.get("selected_core_configs")  # None = all
        selected_module_configs = data.get("selected_module_configs")  # None = all
        selected_channels = data.get("selected_channels")  # None = all

        if not backup_id:
            return jsonify({"error": "backup_id is required"}), 400
        if not target_node_str:
            return jsonify({"error": "target_node_id is required"}), 400

        target_node_id = convert_node_id(target_node_str)

        # Get backup info for job name
        backup = AdminRepository.get_backup(backup_id)
        if not backup:
            return jsonify({"error": f"Backup {backup_id} not found"}), 404

        backup_name = backup.get("backup_name", f"Backup #{backup_id}")

        job_service = get_job_service()
        result = job_service.queue_job(
            job_type=JobType.RESTORE,
            job_name=f"Restore: {backup_name}",
            job_data={
                "backup_id": backup_id,
                "skip_lora": skip_lora,
                "skip_security": skip_security,
                "reboot_after": reboot_after,
                "selected_core_configs": selected_core_configs,
                "selected_module_configs": selected_module_configs,
                "selected_channels": selected_channels,
            },
            target_node_id=target_node_id,
        )

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 409  # Conflict with existing job

    except ValueError as e:
        return jsonify({"error": f"Invalid target_node_id: {e}"}), 400
    except Exception as e:
        logger.error(f"Error queuing restore job: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/node/<node_id>/config/<config_type>", methods=["POST"])
def api_set_node_config(node_id, config_type):
    """Set configuration on a remote node."""
    try:
        node_id_int = convert_node_id(node_id)

        # Map config type string to enum
        config_type_map = {
            "device": ConfigType.DEVICE,
            "position": ConfigType.POSITION,
            "power": ConfigType.POWER,
            "network": ConfigType.NETWORK,
            "display": ConfigType.DISPLAY,
            "lora": ConfigType.LORA,
            "bluetooth": ConfigType.BLUETOOTH,
        }

        if config_type.lower() not in config_type_map:
            return jsonify(
                {
                    "error": f"Invalid config type. Valid types: {list(config_type_map.keys())}",
                }
            ), 400

        data = request.get_json()
        if not data:
            return jsonify({"error": "No configuration data provided"}), 400

        admin_service = get_admin_service()
        result = admin_service.set_config(
            target_node_id=node_id_int,
            config_type=config_type_map[config_type.lower()],
            config_data=data,
        )

        if result.success:
            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "config_type": config_type,
                    "message": result.response.get("message")
                    if result.response
                    else None,
                    "log_id": result.log_id,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": result.error,
                    "log_id": result.log_id,
                }
            ), 200

    except Exception as e:
        logger.error(f"Error setting node config: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/node/<node_id>/channel/<int:channel_index>")
def api_get_node_channel(node_id, channel_index):
    """Get channel configuration from a remote node."""
    try:
        node_id_int = convert_node_id(node_id)

        if channel_index < 0 or channel_index > 7:
            return jsonify({"error": "Channel index must be 0-7"}), 400

        # Get retry parameters from query string
        max_retries = request.args.get("max_retries", 3, type=int)
        retry_delay = request.args.get("retry_delay", 2.0, type=float)
        timeout = request.args.get("timeout", 30.0, type=float)

        admin_service = get_admin_service()
        result = admin_service.get_channel(
            target_node_id=node_id_int,
            channel_index=channel_index,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
        )

        if result.success:
            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "channel_index": channel_index,
                    "channel": result.response,
                    "log_id": result.log_id,
                    "attempts": result.attempts,
                    "retry_info": result.retry_info,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": result.error,
                    "log_id": result.log_id,
                    "attempts": result.attempts,
                    "retry_info": result.retry_info,
                }
            ), 200

    except Exception as e:
        logger.error(f"Error getting node channel: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route(
    "/api/admin/node/<node_id>/channel/<int:channel_index>", methods=["POST"]
)
def api_set_node_channel(node_id, channel_index):
    """Set channel configuration on a remote node."""
    try:
        node_id_int = convert_node_id(node_id)

        if channel_index < 0 or channel_index > 7:
            return jsonify({"error": "Channel index must be 0-7"}), 400

        data = request.get_json()
        if not data:
            return jsonify({"error": "No channel data provided"}), 400

        admin_service = get_admin_service()
        result = admin_service.set_channel(
            target_node_id=node_id_int,
            channel_index=channel_index,
            channel_data=data,
        )

        if result.success:
            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "channel_index": channel_index,
                    "message": result.response.get("message")
                    if result.response
                    else None,
                    "log_id": result.log_id,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": result.error,
                    "log_id": result.log_id,
                }
            ), 200

    except Exception as e:
        logger.error(f"Error setting node channel: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/config/schema")
def api_config_schemas():
    """Get all config field schemas."""
    try:
        admin_service = get_admin_service()
        schemas = admin_service.get_all_config_schemas()
        return jsonify(schemas)
    except Exception as e:
        logger.error(f"Error getting config schemas: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/config/schema/<config_type>")
def api_config_schema(config_type):
    """Get config field schema for a specific type."""
    try:
        admin_service = get_admin_service()
        schema = admin_service.get_config_schema(config_type)
        if not schema:
            return jsonify({"error": f"Unknown config type: {config_type}"}), 404
        return jsonify(
            {
                "config_type": config_type,
                "schema": schema,
            }
        )
    except Exception as e:
        logger.error(f"Error getting config schema: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Commands
# ============================================================================


@admin_bp.route("/api/admin/node/<node_id>/reboot", methods=["POST"])
def api_reboot_node(node_id):
    """Reboot a remote node."""
    try:
        node_id_int = convert_node_id(node_id)

        data = request.get_json() or {}
        delay_seconds = data.get("delay_seconds", 5)

        if (
            not isinstance(delay_seconds, int)
            or delay_seconds < 1
            or delay_seconds > 300
        ):
            return jsonify(
                {"error": "delay_seconds must be an integer between 1 and 300"}
            ), 400

        admin_service = get_admin_service()
        result = admin_service.reboot_node(
            target_node_id=node_id_int,
            delay_seconds=delay_seconds,
        )

        if result.success:
            message = result.response.get("message") if result.response else None
            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "hex_id": f"!{node_id_int:08x}",
                    "message": message,
                    "log_id": result.log_id,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": result.error,
                    "log_id": result.log_id,
                }
            ), 200

    except Exception as e:
        logger.error(f"Error rebooting node: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/node/<node_id>/shutdown", methods=["POST"])
def api_shutdown_node(node_id):
    """Shutdown a remote node."""
    try:
        node_id_int = convert_node_id(node_id)

        data = request.get_json() or {}
        delay_seconds = data.get("delay_seconds", 5)

        if (
            not isinstance(delay_seconds, int)
            or delay_seconds < 1
            or delay_seconds > 300
        ):
            return jsonify(
                {"error": "delay_seconds must be an integer between 1 and 300"}
            ), 400

        admin_service = get_admin_service()
        result = admin_service.shutdown_node(
            target_node_id=node_id_int,
            delay_seconds=delay_seconds,
        )

        if result.success:
            message = result.response.get("message") if result.response else None
            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "hex_id": f"!{node_id_int:08x}",
                    "message": message,
                    "log_id": result.log_id,
                }
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": result.error,
                    "log_id": result.log_id,
                }
            ), 200

    except Exception as e:
        logger.error(f"Error shutting down node: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/node/<node_id>/telemetry/live", methods=["POST"])
def api_request_live_telemetry(node_id):
    """
    Request live telemetry from a remote node.

    This sends a telemetry request via the mesh network and waits for
    a response. Use this for real-time polling of node status.

    Request body (optional):
        telemetry_type: Type of telemetry (device_metrics, environment_metrics)
        timeout: Timeout in seconds (default: 10, max: 30)

    Returns:
        Telemetry data if successful, or error information
    """
    try:
        node_id_int = convert_node_id(node_id)

        data = request.get_json() or {}
        telemetry_type = data.get("telemetry_type", "device_metrics")
        timeout = min(data.get("timeout", 10), 30)  # Max 30 seconds

        admin_service = get_admin_service()
        connection_type = admin_service.connection_type.value

        if connection_type == "tcp":
            tcp_publisher = get_tcp_publisher()
            if not tcp_publisher.is_connected:
                return jsonify(
                    {
                        "success": False,
                        "error": "TCP not connected. Connect via Admin page first.",
                    }
                ), 400

            result = tcp_publisher.send_telemetry_request(
                target_node_id=node_id_int,
                telemetry_type=telemetry_type,
                timeout=timeout,
            )

            if result:
                return jsonify(
                    {
                        "success": True,
                        "node_id": node_id_int,
                        "hex_id": f"!{node_id_int:08x}",
                        "telemetry": result.get("telemetry", {}),
                        "timestamp": result.get("timestamp"),
                        "live": True,
                    }
                )
            else:
                return jsonify(
                    {
                        "success": False,
                        "error": f"No response from node within {timeout}s",
                        "node_id": node_id_int,
                        "hex_id": f"!{node_id_int:08x}",
                    }
                ), 408  # Request Timeout

        elif connection_type == "mqtt":
            # MQTT is fire-and-forget, we can't wait for response here
            return jsonify(
                {
                    "success": False,
                    "error": "Live telemetry requires TCP connection. "
                    "MQTT cannot wait for responses.",
                }
            ), 400

        else:
            return jsonify(
                {
                    "success": False,
                    "error": f"Unsupported connection type: {connection_type}",
                }
            ), 400

    except Exception as e:
        logger.error(f"Error requesting live telemetry: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Audit Log
# ============================================================================


@admin_bp.route("/api/admin/log")
def api_admin_log():
    """Get admin command audit log."""
    try:
        node_id = request.args.get("node_id")
        limit = request.args.get("limit", 100, type=int)

        node_id_int = None
        if node_id:
            node_id_int = convert_node_id(node_id)

        admin_service = get_admin_service()
        log_entries = admin_service.get_admin_log(
            target_node_id=node_id_int,
            limit=limit,
        )

        # Enhance log entries with hex node IDs
        for entry in log_entries:
            if entry.get("target_node_id"):
                entry["target_node_hex"] = f"!{entry['target_node_id']:08x}"

        return jsonify(
            {
                "log": log_entries,
                "count": len(log_entries),
            }
        )

    except Exception as e:
        logger.error(f"Error getting admin log: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# Configuration Template Routes
# ============================================================================


@admin_bp.route("/api/admin/templates")
def api_get_templates():
    """Get all configuration templates."""
    try:
        template_type = request.args.get("type")
        templates = AdminRepository.get_all_templates(template_type=template_type)

        return jsonify(
            {
                "templates": templates,
                "count": len(templates),
            }
        )

    except Exception as e:
        logger.error(f"Error getting templates: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/templates", methods=["POST"])
def api_create_template():
    """Create a new configuration template."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body required"}), 400

        name = data.get("name")
        template_type = data.get("template_type")
        config_data = data.get("config_data")
        description = data.get("description")

        if not name:
            return jsonify({"error": "name is required"}), 400
        if not template_type:
            return jsonify({"error": "template_type is required"}), 400
        if not config_data:
            return jsonify({"error": "config_data is required"}), 400

        # Validate template_type
        valid_types = [
            "device",
            "lora",
            "position",
            "power",
            "network",
            "display",
            "bluetooth",
            "security",
            "channel",
            "channels",  # Full channel set (all 8 channels)
        ]
        if template_type not in valid_types:
            return jsonify(
                {"error": f"template_type must be one of: {valid_types}"}
            ), 400

        # Check for duplicate name
        existing = AdminRepository.get_template_by_name(name)
        if existing:
            return jsonify(
                {"error": f"Template with name '{name}' already exists"}
            ), 409

        import json

        config_json = (
            json.dumps(config_data) if isinstance(config_data, dict) else config_data
        )

        template_id = AdminRepository.create_template(
            name=name,
            template_type=template_type,
            config_data=config_json,
            description=description,
        )

        return jsonify(
            {
                "success": True,
                "template_id": template_id,
                "message": f"Template '{name}' created successfully",
            }
        )

    except Exception as e:
        logger.error(f"Error creating template: {e}")
        return jsonify({"error": str(e)}), 500


# Fields to exclude when creating templates from node configs
# These are node-specific and shouldn't be templated
TEMPLATE_EXCLUDED_FIELDS = {
    "device": [],  # All device settings are templateable
    "lora": [],  # All LoRa settings are templateable
    "position": [
        "latitude_i",
        "longitude_i",
        "altitude",
        "fixed_position",
    ],  # Location is node-specific
    "power": [],  # All power settings are templateable
    "network": [
        "wifi_ssid",
        "wifi_psk",
        "ntp_server",
    ],  # Credentials are node-specific
    "display": [],  # All display settings are templateable
    "bluetooth": [],  # All bluetooth settings are templateable (including PIN for fleet)
    "security": [
        "public_key",
        "private_key",
        "is_managed",
        "serial_enabled",
        "debug_log_api_enabled",
        "admin_channel_enabled",
    ],  # Only admin_key is templateable (for standardizing admin access across fleet)
    "channel": [],  # Channel settings can be templated (name, psk, etc.)
    "channels": [],  # Full channel set (all 8 channels)
}


@admin_bp.route("/api/admin/templates/extract-from-node", methods=["POST"])
def api_extract_template_from_node():
    """
    Extract configuration from a node to create a template.

    This fetches the current config from a node and returns sanitized
    config data suitable for templating (with node-specific fields removed).
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body required"}), 400

        node_id_str = data.get("node_id")
        config_type = data.get("config_type")
        channel_index = data.get("channel_index", 0)

        if not node_id_str:
            return jsonify({"error": "node_id is required"}), 400
        if not config_type:
            return jsonify({"error": "config_type is required"}), 400

        # Convert node ID
        try:
            node_id = convert_node_id(node_id_str)
        except (ValueError, TypeError):
            return jsonify({"error": f"Invalid node_id: {node_id_str}"}), 400

        admin_service = get_admin_service()

        # Handle channel config separately
        if config_type == "channel":
            result = admin_service.get_channel(
                target_node_id=node_id,
                channel_index=channel_index,
            )
        elif config_type == "channels":
            # Fetch all 8 channels
            all_channels = []
            total_attempts = 0
            for idx in range(8):
                result = admin_service.get_channel(
                    target_node_id=node_id,
                    channel_index=idx,
                )
                total_attempts += result.attempts or 0
                if result.success and result.response:
                    channel_data = result.response
                    # Only include enabled channels (role != DISABLED)
                    if channel_data.get("role", 0) != 0:
                        all_channels.append(channel_data)

            return jsonify(
                {
                    "success": True,
                    "config_type": "channels",
                    "config_data": {"channels": all_channels},
                    "excluded_fields": [],
                    "source_node_id": node_id,
                    "attempts": total_attempts,
                }
            )
        else:
            # Map config_type string to ConfigType enum
            config_type_map = {
                "device": ConfigType.DEVICE,
                "position": ConfigType.POSITION,
                "power": ConfigType.POWER,
                "network": ConfigType.NETWORK,
                "display": ConfigType.DISPLAY,
                "lora": ConfigType.LORA,
                "bluetooth": ConfigType.BLUETOOTH,
                "security": ConfigType.SECURITY,
            }

            if config_type not in config_type_map:
                return jsonify({"error": f"Invalid config_type: {config_type}"}), 400

            result = admin_service.get_config(
                target_node_id=node_id,
                config_type=config_type_map[config_type],
            )

        if not result.success:
            return jsonify(
                {
                    "error": result.error or "Failed to get config from node",
                    "attempts": result.attempts,
                }
            ), 500

        # Extract the config data
        raw_config = result.response

        # For non-channel configs, extract the specific type's data
        if config_type != "channel" and isinstance(raw_config, dict):
            # The config response contains nested data for the config type
            config_data = raw_config.get(config_type, raw_config)
        else:
            config_data = raw_config

        # Sanitize the config - remove node-specific fields
        excluded_fields = TEMPLATE_EXCLUDED_FIELDS.get(config_type, [])
        if isinstance(config_data, dict):
            sanitized_config = {
                k: v for k, v in config_data.items() if k not in excluded_fields
            }
        else:
            sanitized_config = config_data

        return jsonify(
            {
                "success": True,
                "config_type": config_type,
                "config_data": sanitized_config,
                "excluded_fields": excluded_fields,
                "source_node_id": node_id,
                "attempts": result.attempts,
            }
        )

    except Exception as e:
        logger.error(f"Error extracting template from node: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/templates/extract-from-node/stream")
def api_extract_template_from_node_stream():
    """
    SSE endpoint to extract configuration from a node with real-time progress updates.

    Returns a Server-Sent Events stream with progress messages during config extraction.
    Particularly useful for "channels" extraction which fetches multiple channels.
    """
    node_id_str = request.args.get("node_id")
    config_type = request.args.get("config_type")
    channel_index = request.args.get("channel_index", "0")

    def generate():
        def send_event(data: dict) -> str:
            return f"data: {json.dumps(data)}\n\n"

        # Validate inputs
        if not node_id_str:
            yield send_event(
                {"complete": True, "success": False, "error": "node_id is required"}
            )
            return
        if not config_type:
            yield send_event(
                {"complete": True, "success": False, "error": "config_type is required"}
            )
            return

        # Convert node ID
        try:
            node_id = convert_node_id(node_id_str)
        except (ValueError, TypeError):
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": f"Invalid node_id: {node_id_str}",
                }
            )
            return

        admin_service = get_admin_service()

        try:
            # Handle channel config separately
            if config_type == "channel":
                yield send_event(
                    {
                        "status": "Fetching channel configuration...",
                        "progress": 10,
                        "details": f"Channel index: {channel_index}",
                    }
                )

                result = admin_service.get_channel(
                    target_node_id=node_id,
                    channel_index=int(channel_index),
                )

                yield send_event(
                    {
                        "status": "Processing response...",
                        "progress": 80,
                    }
                )

                if result.success:
                    excluded_fields = TEMPLATE_EXCLUDED_FIELDS.get("channel", [])
                    config_data = result.response
                    if isinstance(config_data, dict):
                        sanitized_config = {
                            k: v
                            for k, v in config_data.items()
                            if k not in excluded_fields
                        }
                    else:
                        sanitized_config = config_data

                    yield send_event(
                        {
                            "complete": True,
                            "success": True,
                            "config_type": config_type,
                            "config_data": sanitized_config,
                            "excluded_fields": excluded_fields,
                            "source_node_id": node_id,
                            "attempts": result.attempts,
                        }
                    )
                else:
                    yield send_event(
                        {
                            "complete": True,
                            "success": False,
                            "error": result.error or "Failed to get channel config",
                            "attempts": result.attempts,
                        }
                    )

            elif config_type == "channels":
                # Fetch all 8 channels with progress updates
                all_channels = []
                total_attempts = 0

                for idx in range(8):
                    progress = 10 + (idx * 10)  # 10-90% progress
                    yield send_event(
                        {
                            "status": f"Fetching channel {idx}...",
                            "progress": progress,
                            "details": f"Channel {idx + 1} of 8",
                        }
                    )

                    result = admin_service.get_channel(
                        target_node_id=node_id,
                        channel_index=idx,
                    )
                    total_attempts += result.attempts or 0

                    if result.success and result.response:
                        channel_data = result.response
                        # Only include enabled channels (role != DISABLED)
                        role = channel_data.get("role", 0)
                        if role != 0:
                            all_channels.append(channel_data)
                            yield send_event(
                                {
                                    "status": f"Fetched channel {idx}",
                                    "progress": progress + 5,
                                    "details": f"Found active channel (role={role})",
                                }
                            )
                        else:
                            yield send_event(
                                {
                                    "status": f"Channel {idx} disabled, skipping",
                                    "progress": progress + 5,
                                    "details": "Channel is disabled",
                                }
                            )

                yield send_event(
                    {
                        "complete": True,
                        "success": True,
                        "config_type": "channels",
                        "config_data": {"channels": all_channels},
                        "excluded_fields": [],
                        "source_node_id": node_id,
                        "attempts": total_attempts,
                    }
                )

            else:
                # Regular config type
                config_type_map = {
                    "device": ConfigType.DEVICE,
                    "position": ConfigType.POSITION,
                    "power": ConfigType.POWER,
                    "network": ConfigType.NETWORK,
                    "display": ConfigType.DISPLAY,
                    "lora": ConfigType.LORA,
                    "bluetooth": ConfigType.BLUETOOTH,
                    "security": ConfigType.SECURITY,
                }

                if config_type not in config_type_map:
                    yield send_event(
                        {
                            "complete": True,
                            "success": False,
                            "error": f"Invalid config_type: {config_type}",
                        }
                    )
                    return

                yield send_event(
                    {
                        "status": f"Fetching {config_type} configuration...",
                        "progress": 20,
                        "details": f"Sending request to node {node_id_str}",
                    }
                )

                result = admin_service.get_config(
                    target_node_id=node_id,
                    config_type=config_type_map[config_type],
                )

                yield send_event(
                    {
                        "status": "Processing response...",
                        "progress": 80,
                    }
                )

                if result.success:
                    raw_config = result.response
                    if isinstance(raw_config, dict):
                        config_data = raw_config.get(config_type, raw_config)
                    else:
                        config_data = raw_config

                    excluded_fields = TEMPLATE_EXCLUDED_FIELDS.get(config_type, [])
                    if isinstance(config_data, dict):
                        sanitized_config = {
                            k: v
                            for k, v in config_data.items()
                            if k not in excluded_fields
                        }
                    else:
                        sanitized_config = config_data

                    yield send_event(
                        {
                            "complete": True,
                            "success": True,
                            "config_type": config_type,
                            "config_data": sanitized_config,
                            "excluded_fields": excluded_fields,
                            "source_node_id": node_id,
                            "attempts": result.attempts,
                        }
                    )
                else:
                    yield send_event(
                        {
                            "complete": True,
                            "success": False,
                            "error": result.error or "Failed to get config from node",
                            "attempts": result.attempts,
                        }
                    )

        except Exception as e:
            logger.error(f"Error in extract stream: {e}")
            yield send_event({"complete": True, "success": False, "error": str(e)})

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@admin_bp.route("/api/admin/templates/<int:template_id>")
def api_get_template(template_id):
    """Get a specific configuration template."""
    try:
        template = AdminRepository.get_template(template_id)
        if not template:
            return jsonify({"error": "Template not found"}), 404

        import json

        # Parse config_data JSON
        try:
            template["config_data"] = json.loads(template["config_data"])
        except (json.JSONDecodeError, TypeError):
            pass  # Keep as string if not valid JSON

        return jsonify(template)

    except Exception as e:
        logger.error(f"Error getting template: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/templates/<int:template_id>", methods=["PUT"])
def api_update_template(template_id):
    """Update a configuration template."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body required"}), 400

        # Check template exists
        template = AdminRepository.get_template(template_id)
        if not template:
            return jsonify({"error": "Template not found"}), 404

        import json

        config_data = data.get("config_data")
        if config_data and isinstance(config_data, dict):
            config_data = json.dumps(config_data)

        success = AdminRepository.update_template(
            template_id=template_id,
            name=data.get("name"),
            description=data.get("description"),
            config_data=config_data,
        )

        if success:
            return jsonify(
                {
                    "success": True,
                    "message": "Template updated successfully",
                }
            )
        else:
            return jsonify({"error": "Failed to update template"}), 500

    except Exception as e:
        logger.error(f"Error updating template: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/templates/<int:template_id>", methods=["DELETE"])
def api_delete_template(template_id):
    """Delete a configuration template."""
    try:
        template = AdminRepository.get_template(template_id)
        if not template:
            return jsonify({"error": "Template not found"}), 404

        success = AdminRepository.delete_template(template_id)

        if success:
            return jsonify(
                {
                    "success": True,
                    "message": "Template deleted successfully",
                }
            )
        else:
            return jsonify({"error": "Failed to delete template"}), 500

    except Exception as e:
        logger.error(f"Error deleting template: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# Configuration Safety Validation
# ============================================================================

# Dangerous configuration settings that could break remote administration
DANGEROUS_CONFIG_WARNINGS = {
    "lora": {
        "region": "Changing LoRa region may make the node unreachable if it differs from gateway",
        "modem_preset": "Changing modem preset may break communication with gateway",
        "tx_enabled": "Disabling TX will prevent the node from responding to admin commands",
        "hop_limit": "Setting hop_limit to 0 may prevent multi-hop admin communication",
    },
    "network": {
        "wifi_enabled": "Disabling WiFi will break TCP/IP based administration",
        "wifi_ssid": "Changing WiFi network may make the node unreachable",
        "wifi_psk": "Changing WiFi password may make the node unreachable",
        "eth_enabled": "Disabling Ethernet may break network-based administration",
    },
    "bluetooth": {
        "enabled": "Disabling Bluetooth will break BLE-based administration",
        "mode": "Changing Bluetooth mode may affect BLE administration",
    },
    "power": {
        "is_power_saving": "Aggressive power saving may make node unresponsive to admin commands",
        "sds_secs": "Short sleep duration may affect responsiveness",
    },
    "device": {
        "rebroadcast_mode": "Changing rebroadcast mode may affect mesh connectivity",
    },
    "security": {
        "public_key": "Changing public key affects node identity and admin authentication",
        "private_key": "Changing private key affects node identity and admin authentication",
        "admin_key": "Changing admin key will affect who can administer the node",
        "admin_channel_enabled": "Disabling admin channel will break remote administration via mesh",
    },
    "channel": {
        "settings.psk": "Changing channel PSK will break communication if gateway uses different key",
        "role": "Disabling primary channel will break mesh communication",
    },
    "channels": {
        "settings.psk": "Changing channel PSK will break communication if gateway uses different key",
        "role": "Disabling primary channel will break mesh communication",
    },
}


def validate_config_safety(
    template_type: str, config_data: dict, force: bool = False
) -> tuple[bool, list[str], list[str]]:
    """
    Validate configuration for potentially dangerous settings.

    Args:
        template_type: The type of configuration (lora, network, etc.)
        config_data: The configuration data to validate
        force: If True, return warnings but don't block

    Returns:
        Tuple of (is_safe, warnings, blocking_issues)
    """
    warnings = []
    blocking_issues = []

    dangerous_fields = DANGEROUS_CONFIG_WARNINGS.get(template_type, {})

    def check_nested(data: dict, prefix: str = "") -> None:
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key

            # Check if this field is dangerous
            if key in dangerous_fields:
                warning_msg = dangerous_fields[key]

                # Some values are more dangerous than others
                if key == "tx_enabled" and value is False:
                    blocking_issues.append(
                        f"CRITICAL: {warning_msg} - This will make the node completely unreachable!"
                    )
                elif key == "wifi_enabled" and value is False:
                    warnings.append(f"WARNING: {warning_msg}")
                elif (
                    key == "enabled" and template_type == "bluetooth" and value is False
                ):
                    warnings.append(f"WARNING: {warning_msg}")
                elif key == "role" and value == 0:  # DISABLED role
                    if template_type == "channel":
                        # Only block if it's the primary channel
                        channel_index = config_data.get("index", 0)
                        if channel_index == 0:
                            blocking_issues.append(
                                "CRITICAL: Disabling primary channel (index 0) will break all mesh communication!"
                            )
                        else:
                            warnings.append(f"Note: Disabling channel {channel_index}")
                else:
                    warnings.append(f"Caution: {warning_msg}")

            # Check nested dicts (like settings.psk)
            if isinstance(value, dict):
                check_nested(value, full_key)

    # For channels template, check each channel
    if template_type == "channels" and "channels" in config_data:
        for i, channel in enumerate(config_data["channels"]):
            # Check primary channel specifically
            if channel.get("index", i) == 0:
                if channel.get("role", 1) == 0:
                    blocking_issues.append(
                        "CRITICAL: Disabling primary channel will break all mesh communication!"
                    )
            check_nested(channel, f"channel[{i}]")
    else:
        check_nested(config_data)

    is_safe = len(blocking_issues) == 0 or force
    return is_safe, warnings, blocking_issues


@admin_bp.route("/api/admin/templates/<int:template_id>/validate", methods=["POST"])
def api_validate_template(template_id):
    """
    Validate a template's configuration for safety before deployment.

    Returns warnings and blocking issues that could break remote administration.
    """
    try:
        template = AdminRepository.get_template(template_id)
        if not template:
            return jsonify({"error": "Template not found"}), 404

        import json

        config_data = template["config_data"]
        if isinstance(config_data, str):
            config_data = json.loads(config_data)

        template_type = template["template_type"]

        is_safe, warnings, blocking_issues = validate_config_safety(
            template_type, config_data
        )

        return jsonify(
            {
                "is_safe": is_safe,
                "warnings": warnings,
                "blocking_issues": blocking_issues,
                "template_name": template["name"],
                "template_type": template_type,
            }
        )

    except Exception as e:
        logger.error(f"Error validating template: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Compliance Checking
# ============================================================================


@admin_bp.route("/api/admin/templates/<int:template_id>/compliance")
def api_get_compliance_results(template_id):
    """
    Get compliance check results for a template.

    Returns the latest compliance status for each node that has been checked,
    along with a summary of compliant vs non-compliant nodes.
    """
    try:
        template = AdminRepository.get_template(template_id)
        if not template:
            return jsonify({"error": "Template not found"}), 404

        results = AdminRepository.get_latest_compliance_results(template_id)
        summary = AdminRepository.get_compliance_summary(template_id)

        # Parse diff_data JSON for each result
        import json as json_module

        for result in results:
            if result.get("diff_data"):
                try:
                    result["diff_data"] = json_module.loads(result["diff_data"])
                except (json_module.JSONDecodeError, TypeError):
                    pass

        # Parse template config for diff display
        template_config = template.get("config_data")
        if isinstance(template_config, str):
            try:
                template_config = json_module.loads(template_config)
            except json_module.JSONDecodeError:
                template_config = {}

        return jsonify(
            {
                "template_id": template_id,
                "template_name": template["name"],
                "template_type": template["template_type"],
                "template_config": template_config,  # Include for diff display
                "summary": summary,
                "results": results,
            }
        )

    except Exception as e:
        logger.error(f"Error getting compliance results: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route(
    "/api/admin/templates/<int:template_id>/compliance-check", methods=["POST"]
)
def api_run_compliance_check(template_id):
    """
    Run a compliance check against all administrable nodes (streaming SSE).

    This fetches the current configuration from each node and compares it
    against the template to identify differences. Results are streamed
    in real-time as each node is checked.

    Request body (optional):
        node_ids: List of specific node IDs to check (defaults to all administrable)
        timeout: Timeout per node in seconds (default: 30)
        max_retries: Max retries per node (default: 3)
    """
    import json as json_module

    template = AdminRepository.get_template(template_id)
    if not template:
        return jsonify({"error": "Template not found"}), 404

    template_config = template["config_data"]
    if isinstance(template_config, str):
        template_config = json_module.loads(template_config)

    template_type = template["template_type"]

    # Unwrap template_config if it's wrapped in the type key
    # Some templates might be stored as {"bluetooth": {...}} instead of {...}
    if (
        isinstance(template_config, dict)
        and len(template_config) == 1
        and template_type in template_config
    ):
        template_config = template_config[template_type]
        logger.debug(f"Unwrapped template_config from {template_type} wrapper")

    data = request.get_json() or {}
    specific_node_ids = data.get("node_ids")
    timeout = min(data.get("timeout", 30), 60)  # Default 30s, max 60s
    max_retries = min(data.get("max_retries", 3), 5)  # Default 3 retries
    retry_delay = data.get("retry_delay", 2.0)  # 2 seconds between retries

    # Get nodes to check
    admin_service = get_admin_service()
    if specific_node_ids:
        nodes = [
            {"node_id": nid}
            for nid in specific_node_ids
            if AdminRepository.is_node_administrable(nid)
        ]
    else:
        nodes = admin_service.get_administrable_nodes()

    if not nodes:
        return jsonify({"error": "No administrable nodes to check"}), 400

    # Check connection
    connection_type = admin_service.connection_type.value
    if connection_type not in ("tcp", "serial"):
        return jsonify(
            {
                "error": "Compliance check requires TCP or Serial connection. "
                "MQTT cannot retrieve node configurations."
            }
        ), 400

    tcp_publisher = get_tcp_publisher()
    if not tcp_publisher.is_connected:
        return jsonify(
            {
                "error": "TCP not connected. Connect via Admin page first.",
            }
        ), 400

    # Map template types to config fetch methods
    config_type_map = {
        "device": "device",
        "lora": "lora",
        "position": "position",
        "power": "power",
        "network": "network",
        "display": "display",
        "bluetooth": "bluetooth",
        "security": "security",
        "channel": "channel",
        "channels": "channel",
    }

    fetch_type = config_type_map.get(template_type)
    if not fetch_type:
        return jsonify(
            {"error": f"Unsupported template type for compliance: {template_type}"}
        ), 400

    # Map template types to ConfigType enum
    config_type_enum_map = {
        "device": ConfigType.DEVICE,
        "lora": ConfigType.LORA,
        "position": ConfigType.POSITION,
        "power": ConfigType.POWER,
        "network": ConfigType.NETWORK,
        "display": ConfigType.DISPLAY,
        "bluetooth": ConfigType.BLUETOOTH,
        "security": ConfigType.SECURITY,
    }

    def send_event(data: dict) -> str:
        return f"data: {json_module.dumps(data)}\n\n"

    def generate():
        compliant_count = 0
        non_compliant_count = 0
        error_count = 0
        results = []
        total_nodes = len(nodes)

        # Send initial status
        yield send_event(
            {
                "type": "start",
                "total_nodes": total_nodes,
                "template_name": template["name"],
                "template_type": template_type,
                "template_config": template_config,
            }
        )

        for idx, node in enumerate(nodes):
            node_id = node["node_id"]
            node_hex = f"!{node_id:08x}"
            node_name = node.get("long_name") or node.get("short_name") or node_hex

            # Send progress update - checking this node
            yield send_event(
                {
                    "type": "checking",
                    "node_id": node_id,
                    "node_hex": node_hex,
                    "node_name": node_name,
                    "current": idx + 1,
                    "total": total_nodes,
                }
            )

            try:
                # Fetch current config from node with patience (retries)
                if fetch_type == "channel":
                    result = admin_service.get_channel(
                        node_id,
                        0,
                        max_retries=max_retries,
                        retry_delay=retry_delay,
                        timeout=timeout,
                    )
                else:
                    config_type_enum = config_type_enum_map.get(fetch_type)
                    if not config_type_enum:
                        continue
                    result = admin_service.get_config(
                        node_id,
                        config_type_enum,
                        max_retries=max_retries,
                        retry_delay=retry_delay,
                        timeout=timeout,
                    )

                if not result.success or result.response is None:
                    error_msg = result.error or "Failed to retrieve configuration"
                    AdminRepository.save_compliance_check(
                        template_id=template_id,
                        node_id=node_id,
                        is_compliant=False,
                        error_message=error_msg,
                    )
                    error_count += 1
                    node_result = {
                        "node_id": node_id,
                        "node_hex": node_hex,
                        "node_name": node_name,
                        "is_compliant": False,
                        "error": error_msg,
                    }
                    results.append(node_result)

                    yield send_event(
                        {
                            "type": "result",
                            "result": node_result,
                            "compliant_count": compliant_count,
                            "non_compliant_count": non_compliant_count,
                            "error_count": error_count,
                            "current": idx + 1,
                            "total": total_nodes,
                        }
                    )
                    continue

                node_config = result.response
                logger.info(
                    f"Compliance check for {node_hex}: "
                    f"fetch_type={fetch_type}, "
                    f"node_config type={type(node_config).__name__}, "
                    f"node_config keys={list(node_config.keys()) if isinstance(node_config, dict) else 'N/A'}, "
                    f"template_config keys={list(template_config.keys())}"
                )
                differences = compare_configs(
                    template_config, node_config, config_type=fetch_type
                )
                is_compliant = len(differences) == 0

                AdminRepository.save_compliance_check(
                    template_id=template_id,
                    node_id=node_id,
                    is_compliant=is_compliant,
                    diff_data=json_module.dumps(differences) if differences else None,
                )

                if is_compliant:
                    compliant_count += 1
                else:
                    non_compliant_count += 1

                node_result = {
                    "node_id": node_id,
                    "node_hex": node_hex,
                    "node_name": node_name,
                    "is_compliant": is_compliant,
                    "differences": differences if not is_compliant else None,
                }
                results.append(node_result)

                yield send_event(
                    {
                        "type": "result",
                        "result": node_result,
                        "compliant_count": compliant_count,
                        "non_compliant_count": non_compliant_count,
                        "error_count": error_count,
                        "current": idx + 1,
                        "total": total_nodes,
                    }
                )

            except Exception as e:
                logger.error(f"Error checking compliance for {node_hex}: {e}")
                AdminRepository.save_compliance_check(
                    template_id=template_id,
                    node_id=node_id,
                    is_compliant=False,
                    error_message=str(e),
                )
                error_count += 1
                node_result = {
                    "node_id": node_id,
                    "node_hex": node_hex,
                    "node_name": node_name,
                    "is_compliant": False,
                    "error": str(e),
                }
                results.append(node_result)

                yield send_event(
                    {
                        "type": "result",
                        "result": node_result,
                        "compliant_count": compliant_count,
                        "non_compliant_count": non_compliant_count,
                        "error_count": error_count,
                        "current": idx + 1,
                        "total": total_nodes,
                    }
                )

        # Send completion event
        yield send_event(
            {
                "type": "complete",
                "template_id": template_id,
                "template_name": template["name"],
                "template_type": template_type,
                "template_config": template_config,
                "nodes_checked": len(results),
                "compliant_count": compliant_count,
                "non_compliant_count": non_compliant_count,
                "error_count": error_count,
                "results": results,
            }
        )

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@admin_bp.route(
    "/api/admin/templates/<int:template_id>/compliance-fix", methods=["POST"]
)
def api_run_compliance_fix(template_id):
    """
    Queue a background job to fix non-compliant nodes by applying the template config.

    Request body:
        node_ids: List of node IDs to fix (required)
        verify_after: Whether to verify compliance after fixing (default: True)
        reboot_after: Whether to reboot nodes after applying config (default: False)
                      Useful for configs like Bluetooth that require a reboot.

    Returns:
        Job ID for tracking progress
    """
    import json as json_module

    from ..database.job_repository import JobType
    from ..services.job_service import get_job_service

    template = AdminRepository.get_template(template_id)
    if not template:
        return jsonify({"error": "Template not found"}), 404

    data = request.get_json()
    if not data or not data.get("node_ids"):
        return jsonify({"error": "node_ids is required"}), 400

    node_ids = data.get("node_ids", [])
    verify_after = data.get("verify_after", True)
    reboot_after = data.get("reboot_after", False)

    # Validate node_ids are administrable
    valid_node_ids = []
    for node_id in node_ids:
        if isinstance(node_id, str):
            # Handle hex format
            if node_id.startswith("!"):
                node_id = int(node_id[1:], 16)
            else:
                try:
                    node_id = int(node_id)
                except ValueError:
                    continue
        if AdminRepository.is_node_administrable(node_id):
            valid_node_ids.append(node_id)

    if not valid_node_ids:
        return jsonify({"error": "No valid administrable nodes provided"}), 400

    # Parse template config
    template_config = template["config_data"]
    if isinstance(template_config, str):
        template_config = json_module.loads(template_config)

    template_type = template["template_type"]

    # Unwrap template_config if it's wrapped in the type key
    if (
        isinstance(template_config, dict)
        and len(template_config) == 1
        and template_type in template_config
    ):
        template_config = template_config[template_type]

    # Check connection
    admin_service = get_admin_service()
    connection_type = admin_service.connection_type.value
    if connection_type not in ("tcp", "serial"):
        return jsonify(
            {
                "error": "Compliance fix requires TCP or Serial connection. "
                "MQTT cannot send configurations."
            }
        ), 400

    tcp_publisher = get_tcp_publisher()
    if not tcp_publisher.is_connected:
        return jsonify(
            {
                "error": "TCP not connected. Connect via Admin page first.",
            }
        ), 400

    # Queue the job
    try:
        job_service = get_job_service()
        job_id = job_service.queue_job(
            job_type=JobType.COMPLIANCE_FIX,
            job_name=f"Fix compliance: {template['name']} ({len(valid_node_ids)} nodes)",
            job_data={
                "template_id": template_id,
                "template_name": template["name"],
                "template_type": template_type,
                "config_data": template_config,
                "node_ids": valid_node_ids,
                "verify_after": verify_after,
                "reboot_after": reboot_after,
            },
            target_node_id=None,  # Multiple nodes
        )

        return jsonify(
            {
                "success": True,
                "job_id": job_id,
                "message": f"Queued compliance fix for {len(valid_node_ids)} node(s)",
                "node_count": len(valid_node_ids),
            }
        )

    except Exception as e:
        logger.error(f"Error queuing compliance fix job: {e}")
        return jsonify({"error": str(e)}), 500


def compare_configs(
    template_config: dict, node_config: dict, config_type: str | None = None
) -> list[dict]:
    """
    Compare template configuration against node configuration.

    The template may store config at root level (e.g., {"admin_key": [...]})
    while node config may have it wrapped in type key (e.g., {"security": {"admin_key": [...]}}).

    Args:
        template_config: The expected configuration from template
        node_config: The actual configuration from node
        config_type: Optional config type (device, security, lora, etc.) to help unwrap node config

    Returns a list of differences found.
    """
    differences = []

    # Handle None or empty node_config
    if not node_config:
        logger.warning(
            f"compare_configs: node_config is empty/None. "
            f"config_type={config_type}, template_config keys={list(template_config.keys())}"
        )
        # Return all template fields as missing
        for key, value in template_config.items():
            differences.append(
                {
                    "field": key,
                    "expected": value,
                    "actual": None,
                    "type": "missing",
                }
            )
        return differences

    # Try to unwrap template_config if it's wrapped in a type key
    actual_template_config = template_config
    if len(template_config) == 1:
        template_wrapper_key = list(template_config.keys())[0]
        if template_wrapper_key in (
            "device",
            "lora",
            "position",
            "power",
            "network",
            "display",
            "bluetooth",
            "security",
        ):
            actual_template_config = template_config[template_wrapper_key]
            logger.debug(
                f"compare_configs: unwrapped template via wrapper_key={template_wrapper_key}"
            )

    # Try to unwrap node_config if it's wrapped in a type key
    # Node configs come back as {"device": {...}} or {"security": {...}} etc.
    actual_node_config = node_config

    # Log what we received for debugging
    logger.debug(
        f"compare_configs: node_config keys={list(node_config.keys())}, "
        f"config_type={config_type}"
    )

    # Check if node_config has a single key matching a config type
    if len(node_config) == 1:
        wrapper_key = list(node_config.keys())[0]
        if wrapper_key in (
            "device",
            "lora",
            "position",
            "power",
            "network",
            "display",
            "bluetooth",
            "security",
        ):
            actual_node_config = node_config[wrapper_key]
            logger.debug(
                f"compare_configs: unwrapped node via wrapper_key={wrapper_key}, "
                f"actual_node_config keys={list(actual_node_config.keys()) if isinstance(actual_node_config, dict) else 'not a dict'}"
            )
    elif config_type and config_type in node_config:
        # If config_type is provided and exists in node_config, use that
        actual_node_config = node_config[config_type]
        logger.debug(
            f"compare_configs: unwrapped node via config_type={config_type}, "
            f"actual_node_config keys={list(actual_node_config.keys()) if isinstance(actual_node_config, dict) else 'not a dict'}"
        )

    def compare_values(key: str, expected: Any, actual: Any, path: str = ""):
        full_key = f"{path}.{key}" if path else key

        if isinstance(expected, dict) and isinstance(actual, dict):
            # Recursively compare nested dicts
            for sub_key in expected:
                if sub_key in actual:
                    compare_values(
                        sub_key, expected[sub_key], actual[sub_key], full_key
                    )
                else:
                    differences.append(
                        {
                            "field": f"{full_key}.{sub_key}",
                            "expected": expected[sub_key],
                            "actual": None,
                            "type": "missing",
                        }
                    )
        elif isinstance(expected, list) and isinstance(actual, list):
            # Compare lists - check if they have the same elements (order-independent for keys)
            # For admin_key and similar, convert to sets for comparison
            expected_set = (
                set(expected) if all(isinstance(e, str) for e in expected) else None
            )
            actual_set = (
                set(actual) if all(isinstance(a, str) for a in actual) else None
            )

            if expected_set is not None and actual_set is not None:
                if expected_set != actual_set:
                    differences.append(
                        {
                            "field": full_key,
                            "expected": expected,
                            "actual": actual,
                            "type": "mismatch",
                        }
                    )
            elif expected != actual:
                differences.append(
                    {
                        "field": full_key,
                        "expected": expected,
                        "actual": actual,
                        "type": "mismatch",
                    }
                )
        elif expected != actual:
            differences.append(
                {
                    "field": full_key,
                    "expected": expected,
                    "actual": actual,
                    "type": "mismatch",
                }
            )

    for key in actual_template_config:
        if key in actual_node_config:
            compare_values(key, actual_template_config[key], actual_node_config[key])
        else:
            differences.append(
                {
                    "field": key,
                    "expected": actual_template_config[key],
                    "actual": None,
                    "type": "missing",
                }
            )

    return differences


@admin_bp.route("/api/admin/templates/<int:template_id>/deploy", methods=["POST"])
def api_deploy_template(template_id):
    """Deploy a configuration template to one or more nodes."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body required"}), 400

        node_ids = data.get("node_ids", [])
        if not node_ids:
            return jsonify({"error": "node_ids array is required"}), 400

        # Check if user is forcing deployment despite warnings
        force_deploy = data.get("force", False)
        acknowledged_warnings = data.get("acknowledged_warnings", False)

        # Get template
        template = AdminRepository.get_template(template_id)
        if not template:
            return jsonify({"error": "Template not found"}), 404

        import json

        config_data = template["config_data"]
        if isinstance(config_data, str):
            config_data = json.loads(config_data)

        template_type = template["template_type"]

        # Validate configuration safety
        is_safe, warnings, blocking_issues = validate_config_safety(
            template_type, config_data, force=force_deploy
        )

        # If there are blocking issues and force is not set, block deployment
        if blocking_issues and not force_deploy:
            return jsonify(
                {
                    "error": "Configuration contains dangerous settings",
                    "blocking_issues": blocking_issues,
                    "warnings": warnings,
                    "requires_force": True,
                    "message": "This configuration could break remote administration. "
                    "Set 'force: true' and 'acknowledged_warnings: true' to deploy anyway.",
                }
            ), 400

        # If there are warnings and user hasn't acknowledged, require acknowledgment
        if warnings and not acknowledged_warnings and not force_deploy:
            return jsonify(
                {
                    "error": "Configuration has warnings that require acknowledgment",
                    "warnings": warnings,
                    "blocking_issues": blocking_issues,
                    "requires_acknowledgment": True,
                    "message": "Please review warnings and set 'acknowledged_warnings: true' to proceed.",
                }
            ), 400

        admin_service = get_admin_service()

        # Deploy to each node
        results = []
        for node_id in node_ids:
            node_id_int = convert_node_id(node_id)

            # Log deployment attempt
            deployment_id = AdminRepository.log_deployment(
                template_id=template_id,
                node_id=node_id_int,
                status="pending",
            )

            try:
                # Deploy based on template type
                if template_type == "channel":
                    channel_index = config_data.get("index", 0)
                    result = admin_service.set_channel(
                        target_node_id=node_id_int,
                        channel_index=channel_index,
                        channel_data=config_data,
                    )
                elif template_type == "channels":
                    # Deploy all channels in the set
                    channels = config_data.get("channels", [])
                    if not channels:
                        raise ValueError("No channels in template")

                    # Apply each channel
                    all_success = True
                    channel_results = []
                    for channel in channels:
                        channel_index = channel.get("index", 0)
                        ch_result = admin_service.set_channel(
                            target_node_id=node_id_int,
                            channel_index=channel_index,
                            channel_data=channel,
                        )
                        channel_results.append(
                            {
                                "index": channel_index,
                                "success": ch_result.success,
                                "error": ch_result.error,
                            }
                        )
                        if not ch_result.success:
                            all_success = False

                    # Create synthetic result for the batch
                    from dataclasses import dataclass

                    @dataclass
                    class BatchResult:
                        success: bool
                        error: str | None
                        response: dict | None

                    result = BatchResult(
                        success=all_success,
                        error=None
                        if all_success
                        else f"Some channels failed: {channel_results}",
                        response={
                            "message": f"Applied {len(channels)} channels",
                            "details": channel_results,
                        },
                    )
                else:
                    # Map template type to ConfigType enum
                    config_type_map = {
                        "device": ConfigType.DEVICE,
                        "lora": ConfigType.LORA,
                        "position": ConfigType.POSITION,
                        "power": ConfigType.POWER,
                        "network": ConfigType.NETWORK,
                        "display": ConfigType.DISPLAY,
                        "bluetooth": ConfigType.BLUETOOTH,
                        "security": ConfigType.SECURITY,
                    }
                    config_type = config_type_map.get(template_type)
                    if not config_type:
                        raise ValueError(f"Unsupported template type: {template_type}")

                    result = admin_service.set_config(
                        target_node_id=node_id_int,
                        config_type=config_type,
                        config_data=config_data,
                    )

                if result.success:
                    AdminRepository.update_deployment_status(
                        deployment_id=deployment_id,
                        status="success",
                        result_message=result.response.get("message")
                        if result.response
                        else None,
                    )
                    results.append(
                        {
                            "node_id": node_id_int,
                            "hex_id": f"!{node_id_int:08x}",
                            "success": True,
                            "message": "Deployed successfully",
                        }
                    )
                else:
                    AdminRepository.update_deployment_status(
                        deployment_id=deployment_id,
                        status="failed",
                        result_message=result.error,
                    )
                    results.append(
                        {
                            "node_id": node_id_int,
                            "hex_id": f"!{node_id_int:08x}",
                            "success": False,
                            "error": result.error,
                        }
                    )

            except Exception as deploy_error:
                AdminRepository.update_deployment_status(
                    deployment_id=deployment_id,
                    status="failed",
                    result_message=str(deploy_error),
                )
                results.append(
                    {
                        "node_id": node_id_int,
                        "hex_id": f"!{node_id_int:08x}",
                        "success": False,
                        "error": str(deploy_error),
                    }
                )

        successful = sum(1 for r in results if r["success"])
        failed = len(results) - successful

        return jsonify(
            {
                "success": failed == 0,
                "results": results,
                "summary": {
                    "total": len(results),
                    "successful": successful,
                    "failed": failed,
                },
            }
        )

    except Exception as e:
        logger.error(f"Error deploying template: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/api/admin/templates/<int:template_id>/deploy/stream")
def api_deploy_template_stream(template_id):
    """
    SSE endpoint to deploy a configuration template to nodes with real-time progress.

    Query parameters:
        node_ids: Comma-separated list of node IDs to deploy to
        force: "true" to force deployment despite blocking issues
        acknowledged_warnings: "true" to acknowledge warnings

    Returns a Server-Sent Events stream with progress messages during deployment.
    """
    from ..services.admin_service import ConfigType

    node_ids_str = request.args.get("node_ids", "")
    force_deploy = request.args.get("force", "false") == "true"
    acknowledged_warnings = request.args.get("acknowledged_warnings", "false") == "true"

    def generate():
        def send_event(data: dict) -> str:
            return f"data: {json.dumps(data)}\n\n"

        # Parse node IDs
        if not node_ids_str:
            yield send_event(
                {"complete": True, "success": False, "error": "node_ids is required"}
            )
            return

        try:
            node_ids = [
                int(nid.strip()) for nid in node_ids_str.split(",") if nid.strip()
            ]
        except ValueError as e:
            yield send_event(
                {"complete": True, "success": False, "error": f"Invalid node_ids: {e}"}
            )
            return

        if not node_ids:
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": "No valid node IDs provided",
                }
            )
            return

        # Get template
        yield send_event(
            {"status": "Loading template...", "progress": 0, "phase": "init"}
        )

        template = AdminRepository.get_template(template_id)
        if not template:
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": f"Template not found: {template_id}",
                }
            )
            return

        # Parse template config
        config_data = template["config_data"]
        if isinstance(config_data, str):
            config_data = json.loads(config_data)

        template_type = template["template_type"]
        template_name = template.get("name", f"Template {template_id}")

        yield send_event(
            {
                "status": f"Template '{template_name}' loaded ({template_type})",
                "progress": 5,
                "phase": "init",
            }
        )

        # Validate configuration safety
        yield send_event(
            {
                "status": "Validating configuration safety...",
                "progress": 10,
                "phase": "validate",
            }
        )

        is_safe, warnings, blocking_issues = validate_config_safety(
            template_type, config_data, force=force_deploy
        )

        # Report validation results
        if blocking_issues:
            for issue in blocking_issues:
                yield send_event(
                    {
                        "status": f"⚠️ Critical: {issue}",
                        "progress": 10,
                        "phase": "validate",
                        "warning": True,
                    }
                )

        if warnings:
            for warning in warnings:
                yield send_event(
                    {
                        "status": f"⚠️ Warning: {warning}",
                        "progress": 10,
                        "phase": "validate",
                        "warning": True,
                    }
                )

        # Check if we should proceed
        if blocking_issues and not force_deploy:
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": "Configuration contains dangerous settings",
                    "blocking_issues": blocking_issues,
                    "warnings": warnings,
                    "requires_force": True,
                }
            )
            return

        if warnings and not acknowledged_warnings and not force_deploy:
            yield send_event(
                {
                    "complete": True,
                    "success": False,
                    "error": "Configuration has warnings that require acknowledgment",
                    "warnings": warnings,
                    "blocking_issues": blocking_issues,
                    "requires_acknowledgment": True,
                }
            )
            return

        admin_service = get_admin_service()

        # Calculate progress per node
        total_nodes = len(node_ids)
        progress_per_node = 80 / total_nodes  # Reserve 10% at start, 10% at end
        base_progress = 15

        results = []

        for idx, node_id in enumerate(node_ids):
            node_hex = f"!{node_id:08x}"
            current_progress = base_progress + (idx * progress_per_node)

            yield send_event(
                {
                    "status": f"Deploying to {node_hex} ({idx + 1}/{total_nodes})...",
                    "progress": int(current_progress),
                    "phase": "deploy",
                    "current_node": node_hex,
                    "node_index": idx + 1,
                    "total_nodes": total_nodes,
                }
            )

            # Log deployment attempt
            deployment_id = AdminRepository.log_deployment(
                template_id=template_id,
                node_id=node_id,
                status="pending",
            )

            try:
                # Deploy based on template type
                if template_type == "channel":
                    channel_index = config_data.get("index", 0)
                    result = admin_service.set_channel(
                        target_node_id=node_id,
                        channel_index=channel_index,
                        channel_data=config_data,
                    )
                elif template_type == "channels":
                    # Deploy all channels in the set
                    channels = config_data.get("channels", [])
                    if not channels:
                        raise ValueError("No channels in template")

                    all_success = True
                    channel_results = []
                    for ch_idx, channel in enumerate(channels):
                        channel_index = channel.get("index", 0)
                        yield send_event(
                            {
                                "status": f"Setting channel {channel_index} on {node_hex}...",
                                "progress": int(
                                    current_progress
                                    + (ch_idx / len(channels)) * progress_per_node * 0.8
                                ),
                                "phase": "deploy",
                                "current_node": node_hex,
                            }
                        )
                        ch_result = admin_service.set_channel(
                            target_node_id=node_id,
                            channel_index=channel_index,
                            channel_data=channel,
                        )
                        channel_results.append(
                            {
                                "index": channel_index,
                                "success": ch_result.success,
                                "error": ch_result.error,
                            }
                        )
                        if not ch_result.success:
                            all_success = False

                    # Create synthetic result for the batch
                    from dataclasses import dataclass

                    @dataclass
                    class BatchResult:
                        success: bool
                        error: str | None
                        response: dict | None

                    result = BatchResult(
                        success=all_success,
                        error=None
                        if all_success
                        else f"Some channels failed: {channel_results}",
                        response={
                            "message": f"Applied {len(channels)} channels",
                            "details": channel_results,
                        },
                    )
                else:
                    # Map template type to ConfigType enum
                    config_type_map = {
                        "device": ConfigType.DEVICE,
                        "lora": ConfigType.LORA,
                        "position": ConfigType.POSITION,
                        "power": ConfigType.POWER,
                        "network": ConfigType.NETWORK,
                        "display": ConfigType.DISPLAY,
                        "bluetooth": ConfigType.BLUETOOTH,
                        "security": ConfigType.SECURITY,
                    }
                    config_type = config_type_map.get(template_type)
                    if not config_type:
                        raise ValueError(f"Unsupported template type: {template_type}")

                    result = admin_service.set_config(
                        target_node_id=node_id,
                        config_type=config_type,
                        config_data=config_data,
                    )

                if result.success:
                    AdminRepository.update_deployment_status(
                        deployment_id=deployment_id,
                        status="success",
                        result_message=result.response.get("message")
                        if result.response
                        else None,
                    )
                    results.append(
                        {
                            "node_id": node_id,
                            "hex_id": node_hex,
                            "success": True,
                            "message": "Deployed successfully",
                        }
                    )
                    yield send_event(
                        {
                            "status": f"✓ {node_hex}: Deployed successfully",
                            "progress": int(current_progress + progress_per_node),
                            "phase": "deploy",
                            "node_result": {"node": node_hex, "success": True},
                        }
                    )
                else:
                    AdminRepository.update_deployment_status(
                        deployment_id=deployment_id,
                        status="failed",
                        result_message=result.error,
                    )
                    results.append(
                        {
                            "node_id": node_id,
                            "hex_id": node_hex,
                            "success": False,
                            "error": result.error,
                        }
                    )
                    yield send_event(
                        {
                            "status": f"✗ {node_hex}: {result.error}",
                            "progress": int(current_progress + progress_per_node),
                            "phase": "deploy",
                            "node_result": {
                                "node": node_hex,
                                "success": False,
                                "error": result.error,
                            },
                        }
                    )

            except Exception as deploy_error:
                AdminRepository.update_deployment_status(
                    deployment_id=deployment_id,
                    status="failed",
                    result_message=str(deploy_error),
                )
                results.append(
                    {
                        "node_id": node_id,
                        "hex_id": node_hex,
                        "success": False,
                        "error": str(deploy_error),
                    }
                )
                yield send_event(
                    {
                        "status": f"✗ {node_hex}: {deploy_error}",
                        "progress": int(current_progress + progress_per_node),
                        "phase": "deploy",
                        "node_result": {
                            "node": node_hex,
                            "success": False,
                            "error": str(deploy_error),
                        },
                    }
                )

        # Final summary
        successful = sum(1 for r in results if r["success"])
        failed = len(results) - successful

        yield send_event(
            {
                "complete": True,
                "success": failed == 0,
                "results": results,
                "summary": {
                    "total": len(results),
                    "successful": successful,
                    "failed": failed,
                },
                "template_name": template_name,
                "template_type": template_type,
            }
        )

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@admin_bp.route("/api/admin/deployments")
def api_get_deployments():
    """Get template deployment history."""
    try:
        template_id = request.args.get("template_id", type=int)
        node_id = request.args.get("node_id")
        limit = request.args.get("limit", 50, type=int)

        node_id_int = None
        if node_id:
            node_id_int = convert_node_id(node_id)

        deployments = AdminRepository.get_deployment_history(
            template_id=template_id,
            node_id=node_id_int,
            limit=limit,
        )

        # Add hex IDs
        for dep in deployments:
            if dep.get("node_id"):
                dep["node_hex"] = f"!{dep['node_id']:08x}"

        return jsonify(
            {
                "deployments": deployments,
                "count": len(deployments),
            }
        )

    except Exception as e:
        logger.error(f"Error getting deployments: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Bulk Operations
# ============================================================================


@admin_bp.route("/api/admin/nodes/hop-estimates")
def api_nodes_hop_estimates():
    """
    Get estimated hop counts for all administrable nodes.

    This uses traceroute data to estimate how many hops away each node is
    from the gateway. Nodes farther away should be given more time for
    responses when doing bulk operations.

    Returns:
        Dict with node_id -> hop_count mappings and default delay recommendations
    """
    try:
        from ..services.job_service import JobService

        admin_service = get_admin_service()
        nodes = admin_service.get_administrable_nodes()
        job_service = JobService()

        hop_estimates = {}
        for node in nodes:
            node_id = node["node_id"]
            # Use the existing hop estimation logic from job service
            estimated_hops = job_service._estimate_hop_count(node_id)
            hop_estimates[str(node_id)] = {
                "node_id": node_id,
                "hex_id": f"!{node_id:08x}",
                "estimated_hops": estimated_hops,
                # Recommended delay in ms: base 5s + 2s per hop
                "recommended_delay_ms": 5000 + (estimated_hops or 1) * 2000,
            }

        return jsonify(
            {
                "hop_estimates": hop_estimates,
                "node_count": len(nodes),
                "gateway_id": admin_service.gateway_node_id,
                "gateway_hex": (
                    f"!{admin_service.gateway_node_id:08x}"
                    if admin_service.gateway_node_id
                    else None
                ),
            }
        )

    except Exception as e:
        logger.error(f"Error getting hop estimates: {e}")
        return jsonify({"error": str(e)}), 500
