"""
Chat routes for viewing and sending text messages.

Provides REST API endpoints and page routes for the Chat functionality.
This allows admins to watch text messages and send messages to the mesh.
"""

import json
import logging
import time

from flask import (
    Blueprint,
    Response,
    jsonify,
    render_template,
    request,
    stream_with_context,
)
from flask_login import current_user

from ..config import get_config
from ..database.connection import get_db_connection
from ..models.user import UserRole
from ..utils.node_utils import get_bulk_node_names

logger = logging.getLogger(__name__)

chat_bp = Blueprint("chat", __name__)


def _check_viewer_access():
    """Check if current user has viewer or higher access."""
    if not current_user.is_authenticated:
        if request.is_json:
            return jsonify({"error": "Authentication required"}), 401
        from flask import redirect, url_for

        return redirect(url_for("auth.login", next=request.path))
    return None


def _check_operator_access():
    """Check if current user has operator or higher access (required for sending)."""
    if not current_user.is_authenticated:
        if request.is_json:
            return jsonify({"error": "Authentication required"}), 401
        from flask import redirect, url_for

        return redirect(url_for("auth.login", next=request.path))
    if not current_user.has_role(UserRole.OPERATOR):
        if request.is_json:
            return jsonify(
                {
                    "error": "Insufficient permissions",
                    "required_role": "operator",
                    "your_role": current_user.role.value,
                }
            ), 403
        from flask import flash, redirect, url_for

        flash("You need operator or admin access to send messages.", "warning")
        return redirect(url_for("chat.chat_page"))
    return None


def _check_admin_enabled_for_sending():
    """Check if admin features are enabled (required for sending messages)."""
    config = get_config()
    if not config.admin_enabled:
        if request.is_json:
            return jsonify(
                {
                    "error": "Admin features are disabled",
                    "admin_enabled": False,
                    "message": "Message sending requires admin features to be enabled. "
                    "Set MALLA_ADMIN_ENABLED=true to enable.",
                }
            ), 403
        from flask import flash, redirect, url_for

        flash("Message sending requires admin features to be enabled.", "warning")
        return redirect(url_for("chat.chat_page"))
    return None


# ============================================================================
# Page Routes
# ============================================================================


@chat_bp.route("/chat")
def chat_page():
    """Main chat page for viewing and sending messages."""
    logger.info("Chat page accessed")

    # Check viewer access
    auth_result = _check_viewer_access()
    if auth_result:
        return auth_result

    # Check if user can send messages (admin_enabled AND operator role)
    config = get_config()
    can_send = config.admin_enabled and current_user.has_role(UserRole.OPERATOR)

    return render_template("chat.html", can_send=can_send)


# ============================================================================
# API Routes - Message Retrieval
# ============================================================================


@chat_bp.route("/api/chat/messages")
def api_get_messages():
    """
    Get recent text messages.

    Query parameters:
        - limit: Number of messages to return (default: 500, max: 1000)
        - since_id: Get messages with ID greater than this (for polling new messages)
        - before_id: Get messages with ID less than this (for loading older messages)
        - channel: Filter by channel index (optional)
        - hours: Time window in hours from now (default: 24, max: 168)
        - start_time: Unix timestamp for start of time range (overrides hours)
        - end_time: Unix timestamp for end of time range (defaults to now)
    """
    # Check viewer access
    auth_result = _check_viewer_access()
    if auth_result:
        return auth_result

    try:
        import time as time_module

        limit = min(request.args.get("limit", 500, type=int), 1000)
        since_id = request.args.get("since_id", 0, type=int)
        before_id = request.args.get("before_id", type=int)
        channel = request.args.get("channel", type=int)
        hours = min(
            request.args.get("hours", 6, type=int), 168
        )  # Max 7 days, default 6h
        start_time = request.args.get("start_time", type=float)
        end_time = request.args.get("end_time", type=float)

        # Calculate time bounds
        now = time_module.time()
        if end_time is None:
            end_time = now
        if start_time is None:
            start_time = end_time - (hours * 3600)

        conn = get_db_connection()
        cursor = conn.cursor()

        # Build query for TEXT_MESSAGE_APP packets
        query = """
            SELECT
                id, timestamp, from_node_id, to_node_id,
                gateway_id, channel_index, raw_payload,
                rssi, snr, hop_limit, hop_start
            FROM packet_history
            WHERE portnum_name = 'TEXT_MESSAGE_APP'
            AND timestamp >= ?
            AND timestamp <= ?
        """
        params = [start_time, end_time]

        # Filter by since_id for polling new messages
        if since_id > 0:
            query += " AND id > ?"
            params.append(since_id)

        # Filter by before_id for loading older messages
        if before_id is not None:
            query += " AND id < ?"
            params.append(before_id)

        if channel is not None:
            query += " AND channel_index = ?"
            params.append(channel)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        # Collect node IDs for bulk lookup
        node_ids = set()
        for row in rows:
            if row["from_node_id"]:
                node_ids.add(row["from_node_id"])
            if row["to_node_id"]:
                node_ids.add(row["to_node_id"])

        # Get node names
        node_names = get_bulk_node_names(list(node_ids))

        # Format messages
        messages = []
        for row in rows:
            # Decode text from raw_payload
            text = ""
            if row["raw_payload"]:
                try:
                    text = row["raw_payload"].decode("utf-8", errors="replace")
                except Exception:
                    text = "[Unable to decode message]"

            # Determine if broadcast
            to_node_id = row["to_node_id"]
            is_broadcast = to_node_id in (
                "^all",
                "ffffffff",
                "!ffffffff",
                None,
            ) or (
                to_node_id
                and isinstance(to_node_id, str)
                and to_node_id.lower() in ("^all", "ffffffff", "!ffffffff")
            )

            # Calculate hop count
            hop_count = None
            if row["hop_start"] is not None and row["hop_limit"] is not None:
                hop_count = row["hop_start"] - row["hop_limit"]

            messages.append(
                {
                    "id": row["id"],
                    "timestamp": row["timestamp"],
                    "from_node_id": row["from_node_id"],
                    "from_node_name": node_names.get(row["from_node_id"], "Unknown"),
                    "to_node_id": row["to_node_id"],
                    "to_node_name": node_names.get(row["to_node_id"], "Broadcast")
                    if not is_broadcast
                    else "Broadcast",
                    "is_broadcast": is_broadcast,
                    "channel_index": row["channel_index"],
                    "text": text,
                    "rssi": row["rssi"],
                    "snr": row["snr"],
                    "hop_count": hop_count,
                    "gateway_id": row["gateway_id"],
                }
            )

        conn.close()

        # Reverse to get chronological order (oldest first)
        messages.reverse()

        # Calculate pagination info
        oldest_id = messages[0]["id"] if messages else None
        newest_id = messages[-1]["id"] if messages else None
        oldest_timestamp = messages[0]["timestamp"] if messages else None

        return jsonify(
            {
                "messages": messages,
                "count": len(messages),
                "limit": limit,
                "start_time": start_time,
                "end_time": end_time,
                "oldest_id": oldest_id,
                "newest_id": newest_id,
                "oldest_timestamp": oldest_timestamp,
                "has_more": len(messages) >= limit,
            }
        )

    except Exception as e:
        logger.error(f"Error getting chat messages: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@chat_bp.route("/api/chat/channels")
def api_get_channels():
    """Get list of channels available for chat.

    Combines:
    1. Channels from the connected admin node (if connected)
    2. Channels that have text messages in the database
    3. All 8 possible channel indices (0-7) as fallback
    """
    auth_result = _check_viewer_access()
    if auth_result:
        return auth_result

    try:
        # Start with all 8 possible channels (0-7)
        channels_dict = {}
        for i in range(8):
            channels_dict[i] = {
                "index": i,
                "name": f"Channel {i}" if i > 0 else "Primary",
                "message_count": 0,
                "source": "default",
            }

        # Try to get configured channels from connected admin node
        try:
            from ..services.tcp_publisher import get_tcp_publisher

            tcp_publisher = get_tcp_publisher()
            if tcp_publisher.is_connected and tcp_publisher._interface:
                local_node = tcp_publisher._interface.localNode
                if local_node and hasattr(local_node, "channels"):
                    for channel in local_node.channels:
                        if channel and hasattr(channel, "index"):
                            idx = channel.index
                            if 0 <= idx < 8:
                                name = "Primary" if idx == 0 else f"Channel {idx}"
                                if (
                                    hasattr(channel, "settings")
                                    and channel.settings
                                    and hasattr(channel.settings, "name")
                                    and channel.settings.name
                                ):
                                    name = channel.settings.name
                                channels_dict[idx]["name"] = name
                                channels_dict[idx]["source"] = "node"
        except Exception as e:
            logger.debug(f"Could not get channels from node: {e}")

        # Get message counts from database
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT channel_index, COUNT(*) as message_count
            FROM packet_history
            WHERE portnum_name = 'TEXT_MESSAGE_APP'
            AND channel_index IS NOT NULL
            GROUP BY channel_index
            ORDER BY channel_index
        """
        )

        for row in cursor.fetchall():
            idx = row["channel_index"]
            if idx in channels_dict:
                channels_dict[idx]["message_count"] = row["message_count"]
                if channels_dict[idx]["source"] == "default":
                    channels_dict[idx]["source"] = "database"

        conn.close()

        # Convert to list and sort by index
        channels = sorted(channels_dict.values(), key=lambda c: c["index"])

        return jsonify({"channels": channels})

    except Exception as e:
        logger.error(f"Error getting channels: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@chat_bp.route("/api/chat/stream")
def api_message_stream():
    """
    Server-Sent Events stream for new messages.

    Returns:
        Response: SSE stream with new message events
    """
    auth_result = _check_viewer_access()
    if auth_result:
        return auth_result

    logger.info("Chat message stream endpoint accessed")

    def generate():
        last_id = 0
        conn = None

        try:
            # Get the most recent message ID so we only stream NEW messages
            # (not replay old ones)
            try:
                init_conn = get_db_connection()
                init_cursor = init_conn.cursor()
                init_cursor.execute(
                    """
                    SELECT MAX(id) as max_id FROM packet_history
                    WHERE portnum_name = 'TEXT_MESSAGE_APP'
                """
                )
                result = init_cursor.fetchone()
                if result and result["max_id"]:
                    last_id = result["max_id"]
                init_conn.close()
            except Exception as e:
                logger.debug(f"Could not get max message ID: {e}")

            # Send initial connection message
            conn_msg = json.dumps({"type": "connected", "timestamp": time.time()})
            yield f"data: {conn_msg}\n\n"

            while True:
                try:
                    conn = get_db_connection()
                    cursor = conn.cursor()

                    # Check for new messages
                    cursor.execute(
                        """
                        SELECT
                            id, timestamp, from_node_id, to_node_id,
                            gateway_id, channel_index, raw_payload
                        FROM packet_history
                        WHERE portnum_name = 'TEXT_MESSAGE_APP'
                        AND id > ?
                        ORDER BY id ASC
                        LIMIT 50
                    """,
                        (last_id,),
                    )

                    rows = cursor.fetchall()

                    for row in rows:
                        last_id = row["id"]

                        # Decode text
                        text = ""
                        if row["raw_payload"]:
                            try:
                                text = row["raw_payload"].decode(
                                    "utf-8", errors="replace"
                                )
                            except Exception:
                                text = "[Unable to decode message]"

                        # Determine broadcast
                        to_node_id = row["to_node_id"]
                        is_broadcast = to_node_id in (
                            "^all",
                            "ffffffff",
                            "!ffffffff",
                            None,
                        )

                        # Get node names
                        node_ids = [row["from_node_id"]]
                        if not is_broadcast and row["to_node_id"]:
                            node_ids.append(row["to_node_id"])
                        node_names = get_bulk_node_names(node_ids)

                        msg_data = {
                            "type": "message",
                            "data": {
                                "id": row["id"],
                                "timestamp": row["timestamp"],
                                "from_node_id": row["from_node_id"],
                                "from_node_name": node_names.get(
                                    row["from_node_id"], "Unknown"
                                ),
                                "to_node_id": row["to_node_id"],
                                "to_node_name": "Broadcast"
                                if is_broadcast
                                else node_names.get(row["to_node_id"], "Unknown"),
                                "is_broadcast": is_broadcast,
                                "channel_index": row["channel_index"],
                                "text": text,
                                "gateway_id": row["gateway_id"],
                            },
                        }

                        yield f"data: {json.dumps(msg_data)}\n\n"

                    conn.close()
                    conn = None

                except Exception as e:
                    logger.error(f"Error in message stream: {e}")
                    if conn:
                        conn.close()
                        conn = None

                # Send heartbeat every iteration
                yield ":heartbeat\n\n"

                # Poll interval
                time.sleep(2)

        except GeneratorExit:
            logger.info("Chat stream client disconnected")
            if conn:
                conn.close()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ============================================================================
# API Routes - Message Sending
# ============================================================================


@chat_bp.route("/api/chat/send", methods=["POST"])
def api_send_message():
    """
    Send a text message to the mesh.

    Request body:
        text: Message text (required)
        destination: Node ID or 'broadcast' (default: broadcast)
        channel_index: Channel index (default: 0)
    """
    # Check admin enabled for sending
    admin_result = _check_admin_enabled_for_sending()
    if admin_result:
        return admin_result

    # Check operator access for sending
    auth_result = _check_operator_access()
    if auth_result:
        return auth_result

    try:
        data = request.get_json() or {}

        text = data.get("text", "").strip()
        if not text:
            return jsonify({"error": "text is required"}), 400

        if len(text) > 228:  # Meshtastic max message length
            return jsonify({"error": "Message too long (max 228 characters)"}), 400

        # Parse destination
        destination = data.get("destination", "broadcast")
        if destination == "broadcast":
            destination_id = 0xFFFFFFFF
        elif isinstance(destination, str):
            if destination.startswith("!"):
                destination_id = int(destination[1:], 16)
            else:
                destination_id = int(destination, 16) if destination else 0xFFFFFFFF
        else:
            destination_id = int(destination)

        # Parse channel
        channel_index = data.get("channel_index", 0)
        if channel_index is not None:
            channel_index = int(channel_index)
        else:
            channel_index = 0

        # Use the bot service to send the message (reuses existing infrastructure)
        from ..services.bot_service import BotMessagePriority, get_bot_service

        bot = get_bot_service()

        if not bot.is_running:
            # Try to use TCP publisher directly if bot isn't running
            from ..services.tcp_publisher import get_tcp_publisher

            tcp_publisher = get_tcp_publisher()

            if not tcp_publisher.is_connected:
                return jsonify(
                    {
                        "error": "Not connected to a node. Please connect via Admin page first.",
                        "success": False,
                    }
                ), 503

            # Send directly via TCP
            interface = tcp_publisher._interface
            if interface is None:
                return jsonify({"error": "No interface available"}), 503

            interface.sendText(
                text=text,
                destinationId=destination_id,
                channelIndex=channel_index,
                wantAck=False,
            )

            logger.info(
                f"Chat message sent via TCP: '{text[:50]}...' to {destination_id:08x}"
            )

            return jsonify(
                {
                    "message": "Message sent",
                    "success": True,
                    "method": "tcp",
                }
            )
        else:
            # Use bot's message queue
            bot.queue_message(
                text=text,
                destination=destination_id,
                channel_index=channel_index,
                priority=BotMessagePriority.HIGH,
            )

            logger.info(
                f"Chat message queued via bot: '{text[:50]}...' to {destination_id:08x}"
            )

            return jsonify(
                {
                    "message": "Message queued",
                    "success": True,
                    "method": "bot",
                    "queue_size": bot.get_queue_size(),
                }
            )

    except Exception as e:
        logger.error(f"Error sending chat message: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@chat_bp.route("/api/chat/connection-status")
def api_connection_status():
    """Get current connection status for sending messages."""
    auth_result = _check_viewer_access()
    if auth_result:
        return auth_result

    try:
        # Check TCP connection
        from ..services.tcp_publisher import get_tcp_publisher

        tcp_publisher = get_tcp_publisher()
        tcp_connected = tcp_publisher.is_connected

        # Check bot status
        from ..services.bot_service import get_bot_service

        bot = get_bot_service()
        bot_running = bot.is_running

        # Get connected node info if available
        connected_node = None
        if tcp_connected and tcp_publisher._interface:
            try:
                node_info = tcp_publisher._interface.getMyNodeInfo()
                if node_info:
                    connected_node = {
                        "id": f"!{node_info.get('num', 0):08x}",
                        "name": node_info.get("user", {}).get("longName", "Unknown"),
                    }
            except Exception:
                pass

        can_send = tcp_connected or bot_running

        return jsonify(
            {
                "tcp_connected": tcp_connected,
                "bot_running": bot_running,
                "can_send": can_send,
                "connected_node": connected_node,
            }
        )

    except Exception as e:
        logger.error(f"Error getting connection status: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
