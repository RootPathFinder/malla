"""
Bot routes for mesh bot management.

Provides REST API endpoints for controlling the mesh bot, viewing status,
and managing commands.
"""

import logging

from flask import Blueprint, jsonify, request

from ..services.bot_service import BotMessagePriority, get_bot_service

logger = logging.getLogger(__name__)

bot_bp = Blueprint("bot", __name__)


# ============================================================================
# API Routes - Bot Status and Control
# ============================================================================


@bot_bp.route("/api/bot/status")
def api_bot_status():
    """Get bot status and configuration."""
    try:
        bot = get_bot_service()

        return jsonify(
            {
                "enabled": bot.is_enabled,
                "running": bot.is_running,
                "queue_size": bot.get_queue_size(),
                "command_prefix": bot._command_prefix,
                "listen_channels": list(bot._listen_channels),
                "respond_channel_index": bot._respond_channel_index,
                "wait_for_jobs": bot._wait_for_jobs,
                "commands": [
                    {
                        "name": name,
                        "description": bot._command_descriptions.get(name, ""),
                        "enabled": bot.is_command_enabled(name),
                    }
                    for name in bot._commands.keys()
                ],
                "stats": bot.get_stats(),
            }
        )

    except Exception as e:
        logger.error(f"Error getting bot status: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/start", methods=["POST"])
def api_bot_start():
    """Start the bot service."""
    try:
        bot = get_bot_service()

        if bot.is_running:
            return jsonify({"message": "Bot is already running", "success": True})

        bot.start()

        return jsonify({"message": "Bot started", "success": True})

    except Exception as e:
        logger.error(f"Error starting bot: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/stop", methods=["POST"])
def api_bot_stop():
    """Stop the bot service."""
    try:
        bot = get_bot_service()

        if not bot.is_running:
            return jsonify({"message": "Bot is already stopped", "success": True})

        bot.stop()

        return jsonify({"message": "Bot stopped", "success": True})

    except Exception as e:
        logger.error(f"Error stopping bot: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/enable", methods=["POST"])
def api_bot_enable():
    """Enable the bot (start listening without worker thread)."""
    try:
        bot = get_bot_service()
        bot.enable()
        return jsonify({"message": "Bot enabled", "success": True})

    except Exception as e:
        logger.error(f"Error enabling bot: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/disable", methods=["POST"])
def api_bot_disable():
    """Disable the bot (stop listening)."""
    try:
        bot = get_bot_service()
        bot.disable()
        return jsonify({"message": "Bot disabled", "success": True})

    except Exception as e:
        logger.error(f"Error disabling bot: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Bot Configuration
# ============================================================================


@bot_bp.route("/api/bot/config", methods=["GET"])
def api_bot_get_config():
    """Get bot configuration."""
    try:
        bot = get_bot_service()

        return jsonify(
            {
                "command_prefix": bot._command_prefix,
                "listen_channels": list(bot._listen_channels),
                "respond_channel_index": bot._respond_channel_index,
                "wait_for_jobs": bot._wait_for_jobs,
                "min_send_interval": bot._min_send_interval,
            }
        )

    except Exception as e:
        logger.error(f"Error getting bot config: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/config", methods=["PUT"])
def api_bot_update_config():
    """Update bot configuration."""
    try:
        bot = get_bot_service()
        data = request.get_json() or {}

        if "command_prefix" in data:
            bot._command_prefix = str(data["command_prefix"])

        if "listen_channels" in data:
            channels = data["listen_channels"]
            if isinstance(channels, list):
                bot._listen_channels = set(channels)

        if "respond_channel_index" in data:
            bot._respond_channel_index = int(data["respond_channel_index"])

        if "wait_for_jobs" in data:
            bot._wait_for_jobs = bool(data["wait_for_jobs"])

        if "min_send_interval" in data:
            bot._min_send_interval = float(data["min_send_interval"])

        return jsonify(
            {
                "message": "Configuration updated",
                "success": True,
                "config": {
                    "command_prefix": bot._command_prefix,
                    "listen_channels": list(bot._listen_channels),
                    "respond_channel_index": bot._respond_channel_index,
                    "wait_for_jobs": bot._wait_for_jobs,
                    "min_send_interval": bot._min_send_interval,
                },
            }
        )

    except Exception as e:
        logger.error(f"Error updating bot config: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Bot Commands
# ============================================================================


@bot_bp.route("/api/bot/commands")
def api_bot_commands():
    """Get list of registered bot commands."""
    try:
        bot = get_bot_service()

        commands = []
        for name in sorted(bot._commands.keys()):
            commands.append(
                {
                    "name": name,
                    "full_command": f"{bot._command_prefix}{name}",
                    "description": bot._command_descriptions.get(name, ""),
                    "enabled": name not in bot._disabled_commands,
                }
            )

        return jsonify({"commands": commands, "prefix": bot._command_prefix})

    except Exception as e:
        logger.error(f"Error getting bot commands: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Manual Message Sending
# ============================================================================


@bot_bp.route("/api/bot/send", methods=["POST"])
def api_bot_send_message():
    """
    Send a message via the bot.

    Request body:
        text: Message text (required)
        destination: Node ID or 'broadcast' (default: broadcast)
        channel_index: Channel index (default: configured respond channel)
        priority: 'high', 'normal', or 'low' (default: normal)
    """
    try:
        bot = get_bot_service()
        data = request.get_json() or {}

        text = data.get("text")
        if not text:
            return jsonify({"error": "text is required"}), 400

        # Parse destination
        destination = data.get("destination", "broadcast")
        if destination == "broadcast":
            destination = 0xFFFFFFFF
        elif isinstance(destination, str):
            if destination.startswith("!"):
                destination = int(destination[1:], 16)
            else:
                destination = int(destination)

        # Parse channel
        channel_index = data.get("channel_index")
        if channel_index is not None:
            channel_index = int(channel_index)

        # Parse priority
        priority_str = data.get("priority", "normal").lower()
        priority_map = {
            "high": BotMessagePriority.HIGH,
            "normal": BotMessagePriority.NORMAL,
            "low": BotMessagePriority.LOW,
        }
        priority = priority_map.get(priority_str, BotMessagePriority.NORMAL)

        # Queue the message
        bot.queue_message(
            text=text,
            destination=destination,
            channel_index=channel_index,
            priority=priority,
        )

        return jsonify(
            {
                "message": "Message queued",
                "success": True,
                "queue_size": bot.get_queue_size(),
            }
        )

    except Exception as e:
        logger.error(f"Error sending bot message: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/queue")
def api_bot_queue():
    """Get current message queue status."""
    try:
        bot = get_bot_service()

        return jsonify(
            {
                "queue_size": bot.get_queue_size(),
                "is_running": bot.is_running,
                "wait_for_jobs": bot._wait_for_jobs,
            }
        )

    except Exception as e:
        logger.error(f"Error getting queue status: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/queue/clear", methods=["POST"])
def api_bot_clear_queue():
    """Clear the message queue."""
    try:
        bot = get_bot_service()

        # Clear queue by creating a new one
        cleared_count = bot.get_queue_size()
        bot._message_queue = type(bot._message_queue)()

        return jsonify(
            {
                "message": f"Cleared {cleared_count} messages",
                "success": True,
            }
        )

    except Exception as e:
        logger.error(f"Error clearing queue: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Activity Log and Stats
# ============================================================================


@bot_bp.route("/api/bot/activity")
def api_bot_activity():
    """Get bot activity log."""
    try:
        bot = get_bot_service()

        limit = request.args.get("limit", 50, type=int)
        since = request.args.get("since", None, type=float)

        activity = bot.get_activity_log(limit=limit, since=since)

        return jsonify(
            {
                "activity": activity,
                "count": len(activity),
            }
        )

    except Exception as e:
        logger.error(f"Error getting bot activity: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/stats")
def api_bot_stats():
    """Get bot statistics."""
    try:
        bot = get_bot_service()

        return jsonify(bot.get_stats())

    except Exception as e:
        logger.error(f"Error getting bot stats: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Command Toggle
# ============================================================================


@bot_bp.route("/api/bot/command/<command_name>/enable", methods=["POST"])
def api_bot_enable_command(command_name: str):
    """Enable a specific bot command."""
    try:
        bot = get_bot_service()

        if bot.enable_command(command_name):
            return jsonify(
                {
                    "message": f"Command '{command_name}' enabled",
                    "success": True,
                    "command": command_name,
                    "enabled": True,
                }
            )
        else:
            return jsonify({"error": f"Command '{command_name}' not found"}), 404

    except Exception as e:
        logger.error(f"Error enabling command: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/command/<command_name>/disable", methods=["POST"])
def api_bot_disable_command(command_name: str):
    """Disable a specific bot command."""
    try:
        bot = get_bot_service()

        if bot.disable_command(command_name):
            return jsonify(
                {
                    "message": f"Command '{command_name}' disabled",
                    "success": True,
                    "command": command_name,
                    "enabled": False,
                }
            )
        else:
            return jsonify({"error": f"Command '{command_name}' not found"}), 404

    except Exception as e:
        logger.error(f"Error disabling command: {e}")
        return jsonify({"error": str(e)}), 500


@bot_bp.route("/api/bot/command/<command_name>/toggle", methods=["POST"])
def api_bot_toggle_command(command_name: str):
    """Toggle a specific bot command on/off."""
    try:
        bot = get_bot_service()

        if command_name.lower() not in bot._commands:
            return jsonify({"error": f"Command '{command_name}' not found"}), 404

        if bot.is_command_enabled(command_name):
            bot.disable_command(command_name)
            enabled = False
        else:
            bot.enable_command(command_name)
            enabled = True

        return jsonify(
            {
                "message": f"Command '{command_name}' {'enabled' if enabled else 'disabled'}",
                "success": True,
                "command": command_name,
                "enabled": enabled,
            }
        )

    except Exception as e:
        logger.error(f"Error toggling command: {e}")
        return jsonify({"error": str(e)}), 500
