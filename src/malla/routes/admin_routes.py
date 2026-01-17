"""
Admin routes for remote node administration.

Provides REST API endpoints and page routes for the Mesh Admin functionality.
"""

import logging

from flask import Blueprint, jsonify, render_template, request

from ..database.admin_repository import AdminRepository
from ..services.admin_service import ConfigType, get_admin_service
from ..services.tcp_publisher import get_tcp_publisher
from ..utils.node_utils import convert_node_id

logger = logging.getLogger(__name__)

admin_bp = Blueprint("admin", __name__)


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

        admin_service = get_admin_service()
        result = admin_service.get_config(
            target_node_id=node_id_int,
            config_type=config_type_map[config_type.lower()],
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
        logger.error(f"Error getting node config: {e}")
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

        admin_service = get_admin_service()
        result = admin_service.get_channel(
            target_node_id=node_id_int,
            channel_index=channel_index,
        )

        if result.success:
            return jsonify(
                {
                    "success": True,
                    "node_id": node_id_int,
                    "channel_index": channel_index,
                    "channel": result.response,
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
