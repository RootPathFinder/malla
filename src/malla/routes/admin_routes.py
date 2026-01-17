"""
Admin routes for remote node administration.

Provides REST API endpoints and page routes for the Mesh Admin functionality.
"""

import logging

from flask import Blueprint, jsonify, render_template, request

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
            "channel",
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
    "bluetooth": ["fixed_pin"],  # PIN is node-specific
    "channel": [],  # Channel settings can be templated (name, psk, etc.)
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

        # Get template
        template = AdminRepository.get_template(template_id)
        if not template:
            return jsonify({"error": "Template not found"}), 404

        import json

        config_data = template["config_data"]
        if isinstance(config_data, str):
            config_data = json.loads(config_data)

        template_type = template["template_type"]

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
