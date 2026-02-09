"""
Bot Service - Mesh bot that responds to commands on the LongFast channel.

This module provides a bot that listens for commands (e.g., !ping, !traceroute, !status)
on the LongFast channel and automatically responds. It integrates with the job queue
to ensure responses don't interfere with active admin operations.
"""

import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pubsub import pub

from ..config import get_config
from ..database.job_repository import JobRepository, JobStatus

logger = logging.getLogger(__name__)


class BotMessagePriority(Enum):
    """Priority levels for bot messages."""

    HIGH = 1  # Immediate responses (ping)
    NORMAL = 2  # Standard responses
    LOW = 3  # Non-urgent responses


@dataclass
class BotMessage:
    """A message queued for sending by the bot."""

    text: str
    destination: int  # Node ID to send to (0xFFFFFFFF for broadcast)
    channel_index: int = 0  # Channel to send on (0 = primary, 1 = secondary, etc.)
    priority: BotMessagePriority = BotMessagePriority.NORMAL
    created_at: float = field(default_factory=time.time)
    reply_to_node: int | None = None  # Original sender to mention
    max_age: float = 300.0  # Max age in seconds before message is discarded

    def __lt__(self, other: "BotMessage") -> bool:
        """Compare by priority for queue ordering."""
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        return self.created_at < other.created_at


@dataclass
class CommandContext:
    """Context provided to command handlers."""

    command: str
    args: list[str]
    raw_message: str
    sender_id: int
    sender_name: str | None
    channel_index: int
    channel_name: str
    received_at: float
    packet: dict[str, Any]
    is_dm: bool = False  # True if this was a direct message to the bot


# Type alias for command handlers
CommandHandler = Callable[[CommandContext], str | None]


class BotService:
    """
    Mesh bot service that responds to commands on configured channels.

    Features:
    - Listens for !commands in text messages
    - Queues responses to avoid interfering with admin operations
    - Waits for active jobs to complete before sending
    - Configurable command prefix and channels
    """

    _instance: "BotService | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "BotService":
        """Singleton pattern for bot service."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize the bot service."""
        if self._initialized:
            return

        self._initialized = True
        self._config = get_config()
        self._running = False
        self._enabled = False

        # Message queue (priority queue) - tuple of (priority_value, counter, message)
        self._message_queue: queue.PriorityQueue[tuple[int, int, BotMessage]] = (
            queue.PriorityQueue()
        )
        self._queue_counter = 0  # For stable sorting

        # Worker thread
        self._worker_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        # Command handlers
        self._commands: dict[str, CommandHandler] = {}
        self._command_descriptions: dict[str, str] = {}  # Store descriptions separately
        self._disabled_commands: set[str] = set()  # Commands that are disabled
        self._command_prefix = "!"

        # Activity log (circular buffer)
        self._activity_log: list[dict[str, Any]] = []
        self._activity_log_max_size = 100  # Keep last 100 entries
        self._activity_lock = threading.Lock()

        # Pending traceroutes: maps dest_node_id -> (requester_id, requester_name, channel_index, timestamp)
        self._pending_traceroutes: dict[int, tuple[int, str | None, int, float]] = {}
        self._traceroute_timeout = 60.0  # Seconds to wait for traceroute response

        # Traceroute rate limiting (hardware firmware limit)
        self._traceroute_min_interval = 30.0  # Seconds between traceroutes
        self._traceroute_max_per_window = 3  # Max traceroutes per window
        self._traceroute_window_seconds = 300.0  # 5 minute window
        self._traceroute_history: list[float] = []  # Timestamps of sent traceroutes
        self._last_traceroute_time = 0.0
        # Queued traceroute requests: (sender_id, sender_name, channel_index, request_time)
        self._queued_traceroutes: list[tuple[int, str | None, int, float]] = []
        # Track last cooldown reminder time to avoid spamming
        self._last_cooldown_reminder_time = 0.0
        self._cooldown_reminder_interval = 30.0  # Only remind once per 30 seconds

        # Statistics
        self._stats = {
            "commands_received": 0,
            "commands_processed": 0,
            "commands_ignored": 0,
            "messages_sent": 0,
            "messages_failed": 0,
            "errors": 0,
        }
        self._start_time: float | None = None

        # Configuration
        self._listen_channels: set[str] = {"LongFast"}  # Channel names to listen on
        self._respond_channel_index = (
            1  # Channel index to respond on (1 = LongFast typically)
        )
        self._wait_for_jobs = True  # Wait for admin jobs before responding
        self._min_send_interval = 2.0  # Minimum seconds between sends
        self._last_send_time = 0.0

        # Channel directory broadcast scheduling
        self._broadcast_thread: threading.Thread | None = None
        self._broadcast_interval_hours = 12  # Broadcast every 12 hours
        self._last_broadcast_time: float = 0.0

        # Register built-in commands
        self._register_builtin_commands()

        logger.info("BotService initialized")

    def _register_builtin_commands(self) -> None:
        """Register the built-in command handlers."""
        self.register_command("ping", self._cmd_ping, "Check if the bot is online")
        self.register_command("status", self._cmd_status, "Get mesh status summary")
        self.register_command(
            "traceroute", self._cmd_traceroute, "Request traceroute to this node"
        )
        self.register_command("help", self._cmd_help, "Show available commands")
        self.register_command("nodes", self._cmd_nodes, "Count of known nodes")
        self.register_command("uptime", self._cmd_uptime, "Bot uptime")
        self.register_command("mystats", self._cmd_mystats, "Your node statistics")
        # Additional useful commands
        self.register_command("whoami", self._cmd_whoami, "Full node details")
        self.register_command("heard", self._cmd_heard, "Who heard you")
        self.register_command("neighbors", self._cmd_neighbors, "Nearby nodes")
        self.register_command("find", self._cmd_find, "Find node by name")
        self.register_command("busy", self._cmd_busy, "Channel utilization")
        self.register_command("lowbat", self._cmd_lowbat, "Low battery nodes")
        self.register_command("offline", self._cmd_offline, "Recently offline")
        self.register_command("quality", self._cmd_quality, "Your link quality")
        self.register_command("distance", self._cmd_distance, "Distance to node")
        self.register_command("time", self._cmd_time, "Server time")
        self.register_command("weather", self._cmd_weather, "Environment sensors")
        self.register_command("lastseen", self._cmd_lastseen, "Node last activity")
        self.register_command("top", self._cmd_top, "Most active nodes")
        # Channel directory commands
        self.register_command(
            "channels", self._cmd_channels, "List community channels"
        )
        self.register_command(
            "addchannel", self._cmd_addchannel, "Register a channel"
        )
        self.register_command(
            "rmchannel", self._cmd_rmchannel, "Remove your channel"
        )
        self.register_command(
            "channelinfo", self._cmd_channelinfo, "Channel details"
        )

    @property
    def is_enabled(self) -> bool:
        """Check if the bot is enabled."""
        return self._enabled

    @property
    def is_running(self) -> bool:
        """Check if the bot worker is running."""
        return self._running

    def enable(self) -> None:
        """Enable the bot (start listening for commands)."""
        if self._enabled:
            return

        self._enabled = True
        self._start_time = time.time()

        # Subscribe to meshtastic receive events
        try:
            pub.subscribe(self._on_message_received, "meshtastic.receive")
            # Also subscribe to traceroute-specific topic if it exists
            try:
                pub.subscribe(
                    self._on_traceroute_received, "meshtastic.receive.traceroute"
                )
            except Exception:
                pass  # Topic may not exist in all meshtastic versions
            logger.info("Bot enabled - listening for commands")
        except Exception as e:
            logger.error(f"Failed to subscribe to meshtastic events: {e}")
            self._enabled = False

    def disable(self) -> None:
        """Disable the bot (stop listening for commands)."""
        if not self._enabled:
            return

        self._enabled = False

        # Unsubscribe from events
        try:
            pub.unsubscribe(self._on_message_received, "meshtastic.receive")
            try:
                pub.unsubscribe(
                    self._on_traceroute_received, "meshtastic.receive.traceroute"
                )
            except Exception:
                pass
        except Exception:
            pass

        logger.info("Bot disabled")

    def start(self) -> None:
        """Start the bot worker thread."""
        if self._running:
            return

        self._stop_event.clear()
        self._worker_thread = threading.Thread(
            target=self._worker_loop, daemon=True, name="BotWorker"
        )
        self._worker_thread.start()
        self._running = True

        # Start channel directory broadcast thread
        self._start_broadcast_thread()

        # Also enable message listening
        self.enable()

        self._log_activity(
            "bot_started", {"listen_channels": list(self._listen_channels)}
        )
        logger.info("Bot worker started")

    def stop(self) -> None:
        """Stop the bot worker thread."""
        if not self._running:
            return

        self.disable()
        self._stop_event.set()

        if self._broadcast_thread:
            self._broadcast_thread.join(timeout=5.0)
            self._broadcast_thread = None

        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
            self._worker_thread = None

        self._running = False
        self._log_activity("bot_stopped", {"stats": dict(self._stats)})
        logger.info("Bot worker stopped")

    def register_command(
        self,
        name: str,
        handler: CommandHandler,
        description: str = "",
    ) -> None:
        """
        Register a command handler.

        Args:
            name: Command name (without prefix)
            handler: Function that takes CommandContext and returns response text
            description: Help text for the command
        """
        cmd_name = name.lower()
        self._commands[cmd_name] = handler
        self._command_descriptions[cmd_name] = description
        logger.debug(f"Registered command: {self._command_prefix}{name}")

    def is_command_enabled(self, name: str) -> bool:
        """Check if a command is enabled."""
        return name.lower() not in self._disabled_commands

    def enable_command(self, name: str) -> bool:
        """Enable a command. Returns True if command exists."""
        cmd_name = name.lower()
        if cmd_name in self._commands:
            self._disabled_commands.discard(cmd_name)
            self._log_activity("command_enabled", {"command": cmd_name})
            return True
        return False

    def disable_command(self, name: str) -> bool:
        """Disable a command. Returns True if command exists."""
        cmd_name = name.lower()
        if cmd_name in self._commands:
            self._disabled_commands.add(cmd_name)
            self._log_activity("command_disabled", {"command": cmd_name})
            return True
        return False

    def _log_activity(
        self,
        event_type: str,
        data: dict[str, Any] | None = None,
        level: str = "info",
    ) -> None:
        """Log an activity event."""
        entry = {
            "timestamp": time.time(),
            "type": event_type,
            "level": level,
            "data": data or {},
        }
        with self._activity_lock:
            self._activity_log.append(entry)
            # Trim to max size
            if len(self._activity_log) > self._activity_log_max_size:
                self._activity_log = self._activity_log[-self._activity_log_max_size :]

    def get_activity_log(
        self, limit: int = 50, since: float | None = None
    ) -> list[dict[str, Any]]:
        """Get recent activity log entries."""
        with self._activity_lock:
            if since is not None:
                entries = [e for e in self._activity_log if e["timestamp"] > since]
            else:
                entries = list(self._activity_log)
            # Sort by timestamp to ensure chronological order
            entries.sort(key=lambda e: e["timestamp"])
            return entries[-limit:]

    def get_stats(self) -> dict[str, Any]:
        """Get bot statistics."""
        uptime = None
        if self._start_time:
            uptime = time.time() - self._start_time
        return {
            **self._stats,
            "uptime": uptime,
            "start_time": self._start_time,
        }

    def unregister_command(self, name: str) -> None:
        """Unregister a command handler."""
        cmd_name = name.lower()
        self._commands.pop(cmd_name, None)
        self._command_descriptions.pop(cmd_name, None)
        self._disabled_commands.discard(cmd_name)

    def queue_message(
        self,
        text: str,
        destination: int = 0xFFFFFFFF,
        channel_index: int | None = None,
        priority: BotMessagePriority = BotMessagePriority.NORMAL,
        reply_to_node: int | None = None,
    ) -> None:
        """
        Queue a message for sending.

        Args:
            text: Message text to send
            destination: Destination node ID (default: broadcast)
            channel_index: Channel to send on (default: configured respond channel)
            priority: Message priority
            reply_to_node: Node ID of original sender (for context)
        """
        if channel_index is None:
            channel_index = self._respond_channel_index

        msg = BotMessage(
            text=text,
            destination=destination,
            channel_index=channel_index,
            priority=priority,
            reply_to_node=reply_to_node,
        )

        # Add to priority queue with counter for stable sorting
        self._queue_counter += 1
        self._message_queue.put((msg.priority.value, self._queue_counter, msg))
        logger.debug(f"Queued message: {text[:50]}...")

    def get_queue_size(self) -> int:
        """Get the current message queue size."""
        return self._message_queue.qsize()

    def _on_message_received(
        self, packet: dict[str, Any], interface: Any = None
    ) -> None:
        """Handle received packets from pubsub."""
        if not self._enabled:
            return

        try:
            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum")

            # Log all text messages for debugging DM issues
            if portnum == "TEXT_MESSAGE_APP":
                from_id = packet.get("from") or packet.get("fromId")
                to_id = packet.get("to") or packet.get("toId")
                channel = packet.get("channel", 0)
                text = decoded.get("text", decoded.get("payload", ""))
                if isinstance(text, bytes):
                    text = text.decode("utf-8", errors="replace")
                # Truncate for logging
                text_preview = text[:50] + "..." if len(str(text)) > 50 else text
                logger.info(
                    f"Bot received TEXT_MESSAGE_APP: from={from_id}, to={to_id}, "
                    f"channel={channel}, text={text_preview!r}"
                )

            # Check if this is a traceroute response we're waiting for
            if portnum == "TRACEROUTE_APP":
                self._handle_traceroute_packet(packet)
                return

            # Only process text messages for commands
            if portnum != "TEXT_MESSAGE_APP":
                return

            # Get message text
            text = decoded.get("text", "")
            if not text:
                payload = decoded.get("payload")
                if payload:
                    if isinstance(payload, bytes):
                        text = payload.decode("utf-8", errors="replace")
                    elif isinstance(payload, str):
                        text = payload

            if not text:
                return

            # Check if this is a command
            if not text.startswith(self._command_prefix):
                return

            # Get sender info early (needed for logging)
            sender_id_raw = packet.get("from") or packet.get("fromId")
            if isinstance(sender_id_raw, str) and sender_id_raw.startswith("!"):
                sender_id = int(sender_id_raw[1:], 16)
            elif isinstance(sender_id_raw, int):
                sender_id = sender_id_raw
            else:
                sender_id = 0

            # Parse command and args
            parts = text[len(self._command_prefix) :].strip().split()
            if not parts:
                return

            command = parts[0].lower()
            args = parts[1:]

            # Check if we have a handler for this command
            if command not in self._commands:
                logger.debug(f"Unknown command: {command}")
                self._stats["commands_ignored"] += 1
                self._log_activity(
                    "unknown_command",
                    {"command": command, "sender": f"!{sender_id:08x}"},
                    level="warning",
                )
                return

            # Check if command is disabled
            if command in self._disabled_commands:
                logger.debug(f"Command disabled: {command}")
                self._stats["commands_ignored"] += 1
                self._log_activity(
                    "command_disabled_ignored",
                    {"command": command, "sender": f"!{sender_id:08x}"},
                    level="info",
                )
                return

            self._stats["commands_received"] += 1

            # sender_id and sender_id_raw already extracted above

            # Get channel info
            channel_index = packet.get("channel", 0)
            channel_name = self._get_channel_name(channel_index)

            # Check if this is a direct message to us
            to_id = packet.get("to") or packet.get("toId")
            if isinstance(to_id, str) and to_id.startswith("!"):
                to_id = int(to_id[1:], 16)
            elif not isinstance(to_id, int):
                to_id = 0xFFFFFFFF  # Assume broadcast if unknown

            # Get our local node ID to check if this is a DM to us
            local_node_id = self._get_local_node_id()
            is_dm = to_id != 0xFFFFFFFF and to_id == local_node_id

            # Bot responds to all channels (no channel filtering)
            # Previously filtered to only _listen_channels but this was too restrictive

            # Build context
            context = CommandContext(
                command=command,
                args=args,
                raw_message=text,
                sender_id=sender_id,
                sender_name=self._get_node_name(sender_id),
                channel_index=channel_index,
                channel_name=channel_name or "Unknown",
                received_at=time.time(),
                packet=packet,
                is_dm=is_dm,
            )

            dm_indicator = " (DM)" if is_dm else ""
            logger.info(
                f"Bot command received: {self._command_prefix}{command} "
                f"from {context.sender_name or f'!{sender_id:08x}'}{dm_indicator}"
            )

            # Log the command received
            self._log_activity(
                "command_received",
                {
                    "command": command,
                    "args": args,
                    "sender_id": f"!{sender_id:08x}",
                    "sender_name": context.sender_name,
                    "channel": channel_name or f"ch{channel_index}",
                    "is_dm": is_dm,
                },
            )

            # Execute handler
            handler = self._commands[command]
            try:
                logger.info(f"Executing handler for command: {command}")
                response = handler(context)
                logger.info(
                    f"Command {command} returned: {response[:50] if response else 'None/empty'}..."
                )
                if response:
                    self._stats["commands_processed"] += 1
                    # Queue the response
                    priority = (
                        BotMessagePriority.HIGH
                        if command == "ping"
                        else BotMessagePriority.NORMAL
                    )
                    # For DMs, respond directly to sender; for channel msgs, broadcast
                    destination = sender_id if context.is_dm else 0xFFFFFFFF
                    self.queue_message(
                        text=response,
                        destination=destination,
                        channel_index=channel_index,  # Respond on same channel
                        priority=priority,
                        reply_to_node=sender_id,
                    )
                    # Log the response
                    dest_str = (
                        f"DM to !{sender_id:08x}" if context.is_dm else "broadcast"
                    )
                    self._log_activity(
                        "response_queued",
                        {
                            "command": command,
                            "response_preview": response[:100],
                            "priority": priority.name,
                            "destination": dest_str,
                            "channel": channel_name or f"ch{channel_index}",
                            "is_dm": is_dm,
                        },
                    )
                else:
                    logger.info(
                        f"Command {command} returned empty/None response, no message queued"
                    )
            except Exception as e:
                self._stats["errors"] += 1
                self._log_activity(
                    "command_error",
                    {"command": command, "error": str(e)},
                    level="error",
                )
                logger.error(f"Error executing command {command}: {e}", exc_info=True)

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Error processing message for bot: {e}", exc_info=True)

    def _on_traceroute_received(
        self, packet: dict[str, Any], interface: Any = None
    ) -> None:
        """Handle incoming traceroute response packets (from dedicated pubsub topic)."""
        if not self._enabled:
            return
        self._handle_traceroute_packet(packet)

    def _handle_traceroute_packet(self, packet: dict[str, Any]) -> None:
        """Process a traceroute packet and send results if we initiated it."""
        try:
            # Check if this is a traceroute we initiated
            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum")

            if portnum != "TRACEROUTE_APP":
                return

            # Get the from/to nodes to check if this is our traceroute
            from_id = packet.get("fromId") or packet.get("from")

            if isinstance(from_id, str) and from_id.startswith("!"):
                from_id = int(from_id[1:], 16)
            elif not isinstance(from_id, int):
                return

            # Clean up expired pending traceroutes
            current_time = time.time()
            expired = [
                k
                for k, v in self._pending_traceroutes.items()
                if current_time - v[3] > self._traceroute_timeout
            ]
            for k in expired:
                del self._pending_traceroutes[k]

            # Check if we're waiting for a traceroute from this node
            if from_id not in self._pending_traceroutes:
                logger.debug(
                    f"Received traceroute response from !{from_id:08x} but not in pending list"
                )
                return

            requester_id, requester_name, channel_index, _ = (
                self._pending_traceroutes.pop(from_id)
            )
            logger.info(
                f"Processing traceroute response from !{from_id:08x} for channel {channel_index}"
            )

            # Parse the traceroute data from the packet
            # The data can be under 'traceroute' or 'routeDiscovery' key depending on version
            route_discovery = decoded.get("traceroute") or decoded.get("routeDiscovery")

            # Log the packet structure for debugging (INFO level to ensure visibility)
            logger.debug(f"[TR DEBUG] Full decoded dict: {decoded}")
            logger.debug(f"[TR DEBUG] decoded keys: {list(decoded.keys())}")
            logger.debug(
                f"[TR DEBUG] routeDiscovery type: {type(route_discovery)}, "
                f"value: {route_discovery}"
            )
            if route_discovery is not None:
                logger.debug(f"[TR DEBUG] routeDiscovery dir: {dir(route_discovery)}")

            route: list[int] = []
            route_back: list[int] = []
            snr_towards: list[float] = []
            snr_back: list[float] = []

            if route_discovery is not None:
                # Check if it's a protobuf object (has 'route' as attribute)
                if hasattr(route_discovery, "route"):
                    # It's a protobuf object
                    logger.debug("[TR DEBUG] Detected protobuf object")
                    route = list(route_discovery.route)
                    route_back = list(route_discovery.route_back)
                    # SNR values are scaled by 4 in protobuf
                    snr_towards = [float(s) / 4.0 for s in route_discovery.snr_towards]
                    snr_back = [float(s) / 4.0 for s in route_discovery.snr_back]
                elif isinstance(route_discovery, dict):
                    # It's a dict (already parsed)
                    logger.debug(f"[TR DEBUG] Detected dict: {route_discovery}")
                    route = route_discovery.get("route", [])
                    route_back = route_discovery.get("routeBack", [])
                    # SNR values may be scaled by 4 in the dict too
                    raw_snr_towards = route_discovery.get("snrTowards", [])
                    raw_snr_back = route_discovery.get("snrBack", [])
                    # Scale down by 4 (protobuf encoding)
                    snr_towards = [float(s) / 4.0 for s in raw_snr_towards]
                    snr_back = [float(s) / 4.0 for s in raw_snr_back]
                else:
                    logger.debug(f"[TR DEBUG] Unknown type: {type(route_discovery)}")

            logger.debug(
                f"[TR DEBUG] Parsed result: route={route}, route_back={route_back}, "
                f"snr_towards={snr_towards}, snr_back={snr_back}"
            )

            # Get local node ID (source of traceroute)
            local_node_id = self._get_local_node_id() or 0

            # Format the response (from_id is the destination we traced to)
            response = self._format_traceroute_result(
                route, route_back, snr_towards, snr_back, local_node_id, from_id
            )

            # Queue the response
            self.queue_message(
                text=response,
                destination=0xFFFFFFFF,
                channel_index=channel_index,
                priority=BotMessagePriority.NORMAL,
            )

            channel_name = self._get_channel_name(channel_index)
            self._log_activity(
                "traceroute_result",
                {
                    "target": f"!{from_id:08x}",
                    "target_name": requester_name,
                    "channel": channel_name or f"ch{channel_index}",
                    "hops_forward": len(route),
                    "hops_return": len(route_back),
                    "status": "result received",
                },
            )
            logger.info(f"Traceroute result sent for !{from_id:08x}")

        except Exception as e:
            logger.error(f"Error processing traceroute response: {e}", exc_info=True)

    def _format_traceroute_result(
        self,
        route: list[int],
        route_back: list[int],
        snr_towards: list[float],
        snr_back: list[float],
        source_id: int = 0,
        dest_id: int = 0,
    ) -> str:
        """Format traceroute results for sending back to channel."""
        # Build traceroute output showing each hop
        # Format: → src→A(6.5)→B(5.2)→dst | ← dst→B(4.8)→A(5.0)→src
        lines = []

        # Short IDs for source and destination
        src_short = f"{source_id:08x}"[-4:] if source_id else "?"
        dst_short = f"{dest_id:08x}"[-4:] if dest_id else "?"

        # Forward path
        if route:
            # Show each hop with SNR: src→hop1(snr)→hop2(snr)→dst
            hops_str = self._format_hop_chain(route, snr_towards, "→")
            lines.append(f"→ {src_short}→{hops_str}→{dst_short}")
        else:
            # Direct connection (no intermediate hops)
            if snr_towards:
                lines.append(f"→ {src_short}→{dst_short} ({snr_towards[0]:.1f}dB)")
            else:
                lines.append(f"→ {src_short}→{dst_short}")

        # Return path
        if route_back:
            hops_str = self._format_hop_chain(route_back, snr_back, "→")
            lines.append(f"← {dst_short}→{hops_str}→{src_short}")
        else:
            # Direct return or no return path
            if snr_back:
                lines.append(f"← {dst_short}→{src_short} ({snr_back[0]:.1f}dB)")
            else:
                lines.append("← none")

        return "🔍 TR: " + " | ".join(lines)

    def _format_hop_chain(
        self, nodes: list[int], snrs: list[float], separator: str = "→"
    ) -> str:
        """Format a chain of hops with SNR values.

        Args:
            nodes: List of node IDs in the path
            snrs: List of SNR values for each hop
            separator: Character to use between hops

        Returns:
            Formatted string like "A(6.5)→B(5.2)→C"
        """
        parts = []
        for i, node_id in enumerate(nodes):
            # Use short node ID (last 4 hex chars)
            short_id = f"{node_id:08x}"[-4:]
            if i < len(snrs):
                parts.append(f"{short_id}({snrs[i]:.1f})")
            else:
                parts.append(short_id)
        return separator.join(parts)

    def _worker_loop(self) -> None:
        """Worker loop that sends queued messages."""
        logger.info("Bot worker loop started")

        while not self._stop_event.is_set():
            try:
                # Process any queued traceroute requests
                self._process_queued_traceroutes()

                # Check for timed-out traceroutes and notify users
                self._check_traceroute_timeouts()

                # Wait for admin jobs if configured
                if self._wait_for_jobs and self._has_active_jobs():
                    logger.debug("Waiting for active admin jobs to complete...")
                    self._stop_event.wait(timeout=2.0)
                    continue

                # Try to get a message from the queue
                try:
                    item = self._message_queue.get(timeout=1.0)
                    # Queue items are (priority, counter, message) tuples
                    if isinstance(item, tuple) and len(item) >= 3:
                        msg = item[2]
                    else:
                        logger.warning(f"Unexpected queue item format: {type(item)}")
                        continue
                except queue.Empty:
                    continue

                # Check message age
                age = time.time() - msg.created_at
                if age > msg.max_age:
                    logger.warning(
                        f"Discarding stale message (age={age:.1f}s): {msg.text[:50]}..."
                    )
                    continue

                # Rate limiting
                time_since_last = time.time() - self._last_send_time
                if time_since_last < self._min_send_interval:
                    time.sleep(self._min_send_interval - time_since_last)

                # Send the message
                success = self._send_message(msg)
                channel_name = self._get_channel_name(msg.channel_index)
                if success:
                    self._last_send_time = time.time()
                    self._stats["messages_sent"] += 1
                    dest_str = (
                        f"!{msg.destination:08x}"
                        if msg.destination != 0xFFFFFFFF
                        else "broadcast"
                    )
                    self._log_activity(
                        "message_sent",
                        {
                            "text_preview": msg.text[:80],
                            "destination": dest_str,
                            "channel": channel_name or f"ch{msg.channel_index}",
                            "status": "sent",
                            "is_dm": msg.destination != 0xFFFFFFFF,
                        },
                    )
                    logger.info(f"Bot sent: {msg.text[:50]}...")
                else:
                    self._stats["messages_failed"] += 1
                    dest_str = (
                        f"!{msg.destination:08x}"
                        if msg.destination != 0xFFFFFFFF
                        else "broadcast"
                    )
                    self._log_activity(
                        "message_failed",
                        {
                            "text_preview": msg.text[:80],
                            "destination": dest_str,
                            "channel": channel_name or f"ch{msg.channel_index}",
                            "status": "failed",
                            "reason": "send failed",
                        },
                        level="error",
                    )
                    logger.error(f"Failed to send bot message: {msg.text[:50]}...")

            except Exception as e:
                logger.error(f"Error in bot worker loop: {e}", exc_info=True)
                self._stop_event.wait(timeout=5.0)

        logger.info("Bot worker loop stopped")

    def _has_active_jobs(self) -> bool:
        """Check if there are any active admin jobs running."""
        try:
            active_jobs = JobRepository.get_active_jobs()
            # Check for running jobs (not just queued)
            running = [j for j in active_jobs if j["status"] == JobStatus.RUNNING.value]
            return len(running) > 0
        except Exception as e:
            logger.debug(f"Could not check active jobs: {e}")
            return False

    def _get_publisher(self) -> Any | None:
        """Get the appropriate publisher based on config (TCP or serial).

        Returns:
            Publisher instance or None if not available
        """
        config = get_config()
        conn_type = config.admin_connection_type.lower()

        if conn_type == "serial":
            from .serial_publisher import get_serial_publisher

            return get_serial_publisher()
        else:
            # Default to TCP for "tcp" or "mqtt" (bot uses direct connection)
            from .tcp_publisher import get_tcp_publisher

            return get_tcp_publisher()

    def _send_message(self, msg: BotMessage) -> bool:
        """Send a message via the configured interface (TCP or serial)."""
        try:
            publisher = self._get_publisher()
            if publisher is None:
                logger.warning("Cannot send bot message: No publisher available")
                return False

            if not publisher.is_connected:
                logger.warning("Cannot send bot message: Not connected")
                return False

            interface = publisher._interface
            if interface is None:
                logger.warning("Cannot send bot message: No interface")
                return False

            # Send the text message
            interface.sendText(
                text=msg.text,
                destinationId=msg.destination,
                channelIndex=msg.channel_index,
                wantAck=False,  # Don't require ACK for bot messages
            )

            return True

        except Exception as e:
            logger.error(f"Error sending bot message: {e}", exc_info=True)
            return False

    def _get_channel_name(self, channel_index: int) -> str | None:
        """Get the channel name for a channel index."""
        try:
            publisher = self._get_publisher()
            if (
                publisher is None
                or not publisher.is_connected
                or publisher._interface is None
            ):
                return None

            channels = publisher._interface.localNode.channels
            if channels and channel_index < len(channels):
                channel = channels[channel_index]
                if hasattr(channel, "settings") and hasattr(channel.settings, "name"):
                    return channel.settings.name
        except Exception:
            pass
        return None

    def _get_node_name(self, node_id: int) -> str | None:
        """Get the display name for a node ID."""
        try:
            from ..database.repositories import NodeRepository

            node = NodeRepository.get_node_details(node_id)
            if node:
                return node.get("long_name") or node.get("short_name")
        except Exception:
            pass
        return None

    def _get_local_node_id(self) -> int | None:
        """Get the local node ID (the bot's own node)."""
        try:
            publisher = self._get_publisher()
            if (
                publisher is None
                or not publisher.is_connected
                or publisher._interface is None
            ):
                return None

            local_node = publisher._interface.localNode
            if local_node and hasattr(local_node, "nodeNum"):
                return local_node.nodeNum
        except Exception:
            pass
        return None

    # =========================================================================
    # Built-in Command Handlers
    # =========================================================================

    def _cmd_ping(self, ctx: CommandContext) -> str:
        """Handle !ping command."""
        latency = (time.time() - ctx.received_at) * 1000
        return f"Pong! 🏓 (processed in {latency:.0f}ms)"

    def _cmd_status(self, ctx: CommandContext) -> str:
        """Handle !status command."""
        try:
            from ..database.connection import get_db_connection
            from ..database.repositories import PacketRepository

            # Get node counts for last 30 days using direct query (efficient)
            conn = get_db_connection()
            cursor = conn.cursor()
            thirty_days_ago = time.time() - (30 * 24 * 3600)

            # Count nodes seen in last 30 days (based on packet activity)
            cursor.execute(
                """
                SELECT COUNT(DISTINCT from_node_id) as active_nodes
                FROM packet_history
                WHERE timestamp > ?
                """,
                (thirty_days_ago,),
            )
            total_count = cursor.fetchone()["active_nodes"]

            # Count nodes seen in last 15 minutes (online)
            fifteen_min_ago = time.time() - 900
            cursor.execute(
                """
                SELECT COUNT(DISTINCT from_node_id) as online_nodes
                FROM packet_history
                WHERE timestamp > ?
                """,
                (fifteen_min_ago,),
            )
            online_count = cursor.fetchone()["online_nodes"]
            conn.close()

            # Get recent packet count
            recent_packets = PacketRepository.get_packet_count_since(
                time.time() - 3600  # Last hour
            )

            # Get gateway info (truncate to fit payload)
            from .tcp_publisher import get_tcp_publisher

            publisher = get_tcp_publisher()
            gateway_name = publisher.get_local_node_name() or "Unknown"
            if len(gateway_name) > 20:
                gateway_name = gateway_name[:17] + "..."

            return (
                f"📊 Mesh Status\n"
                f"GW: {gateway_name}\n"
                f"Nodes: {online_count}/{total_count} (30d)\n"
                f"Pkts/1h: {recent_packets}"
            )
        except Exception as e:
            logger.error(f"Error in status command: {e}")
            return "Status unavailable"

    def _cmd_traceroute(self, ctx: CommandContext) -> str:
        """Handle !traceroute command with rate limiting."""
        try:
            from .tcp_publisher import get_tcp_publisher

            publisher = get_tcp_publisher()
            if not publisher.is_connected or publisher._interface is None:
                logger.warning("Traceroute failed: TCP publisher not connected")
                return "Not connected"

            current_time = time.time()

            # Clean up old traceroute history (outside the 5-minute window)
            self._traceroute_history = [
                t
                for t in self._traceroute_history
                if current_time - t < self._traceroute_window_seconds
            ]

            # Check if we've hit the rate limit (3 per 5 minutes)
            if len(self._traceroute_history) >= self._traceroute_max_per_window:
                oldest = min(self._traceroute_history)
                wait_time = int(
                    self._traceroute_window_seconds - (current_time - oldest)
                )
                logger.info(
                    f"Traceroute rate limit (window) for !{ctx.sender_id:08x}, wait {wait_time}s"
                )
                return f"⏳ TR limit reached. Try in {wait_time}s"

            # Check if we need to wait for the 30-second interval
            time_since_last = current_time - self._last_traceroute_time
            if time_since_last < self._traceroute_min_interval:
                # Queue this request
                wait_time = int(self._traceroute_min_interval - time_since_last)
                self._queue_traceroute(
                    ctx.sender_id, ctx.sender_name, ctx.channel_index
                )
                sender_name = ctx.sender_name or f"!{ctx.sender_id:08x}"
                if len(sender_name) > 15:
                    sender_name = sender_name[:12] + "..."
                logger.info(f"Traceroute queued for {sender_name}, wait {wait_time}s")

                # Only send reminder once per cooldown_reminder_interval
                time_since_reminder = current_time - self._last_cooldown_reminder_time
                if time_since_reminder >= self._cooldown_reminder_interval:
                    self._last_cooldown_reminder_time = current_time
                    return f"⏳ TR to {sender_name} queued ({wait_time}s)"
                else:
                    # Silently queue without sending a message
                    return ""

            # Execute the traceroute immediately
            logger.info(f"Executing traceroute to !{ctx.sender_id:08x}")
            return self._execute_traceroute(
                ctx.sender_id, ctx.sender_name, ctx.channel_index, publisher
            )

        except Exception as e:
            logger.error(f"Error in traceroute command: {e}", exc_info=True)
            return "Traceroute failed"

    def _queue_traceroute(
        self, sender_id: int, sender_name: str | None, channel_index: int
    ) -> None:
        """Queue a traceroute request to be processed after rate limit expires."""
        # Don't queue duplicate requests for the same sender
        for queued in self._queued_traceroutes:
            if queued[0] == sender_id:
                return  # Already queued

        self._queued_traceroutes.append(
            (sender_id, sender_name, channel_index, time.time())
        )
        channel_name = self._get_channel_name(channel_index)
        self._log_activity(
            "traceroute_queued",
            {
                "target": f"!{sender_id:08x}",
                "target_name": sender_name,
                "channel": channel_name or f"ch{channel_index}",
                "queue_size": len(self._queued_traceroutes),
                "status": "queued (rate limit)",
            },
        )

    def _execute_traceroute(
        self,
        sender_id: int,
        sender_name: str | None,
        channel_index: int,
        publisher: Any = None,
    ) -> str:
        """Execute a traceroute and update rate limiting state."""
        try:
            if publisher is None:
                from .tcp_publisher import get_tcp_publisher

                publisher = get_tcp_publisher()
                if not publisher.is_connected or publisher._interface is None:
                    return "Not connected"

            current_time = time.time()

            # Update rate limiting state
            self._last_traceroute_time = current_time
            self._traceroute_history.append(current_time)

            # Register this as a pending traceroute so we can send results
            self._pending_traceroutes[sender_id] = (
                sender_id,
                sender_name,
                channel_index,
                current_time,
            )

            # Send traceroute to the requesting node
            publisher._interface.sendTraceRoute(
                dest=sender_id,
                hopLimit=7,
            )

            display_name = sender_name or f"!{sender_id:08x}"
            # Truncate name to fit payload
            if len(display_name) > 20:
                display_name = display_name[:17] + "..."

            channel_name = self._get_channel_name(channel_index)
            self._log_activity(
                "traceroute_sent",
                {
                    "target": f"!{sender_id:08x}",
                    "target_name": display_name,
                    "channel": channel_name or f"ch{channel_index}",
                    "status": "sent",
                },
            )

            return f"🔍 TR to {display_name}..."
        except Exception as e:
            logger.error(f"Error executing traceroute: {e}")
            return "Traceroute failed"

    def _process_queued_traceroutes(self) -> None:
        """Process any queued traceroute requests if rate limit allows."""
        if not self._queued_traceroutes:
            return

        current_time = time.time()

        # Check if we can send now
        time_since_last = current_time - self._last_traceroute_time
        if time_since_last < self._traceroute_min_interval:
            return

        # Clean up old traceroute history
        self._traceroute_history = [
            t
            for t in self._traceroute_history
            if current_time - t < self._traceroute_window_seconds
        ]

        # Check window limit
        if len(self._traceroute_history) >= self._traceroute_max_per_window:
            return

        # Remove expired queued requests (older than 2 minutes)
        self._queued_traceroutes = [
            q for q in self._queued_traceroutes if current_time - q[3] < 120.0
        ]

        if not self._queued_traceroutes:
            return

        # Process the oldest queued request
        sender_id, sender_name, channel_index, _ = self._queued_traceroutes.pop(0)

        try:
            from .tcp_publisher import get_tcp_publisher

            publisher = get_tcp_publisher()
            if publisher.is_connected and publisher._interface is not None:
                result = self._execute_traceroute(
                    sender_id, sender_name, channel_index, publisher
                )
                # Queue the "traceroute started" message
                self.queue_message(
                    text=result,
                    destination=0xFFFFFFFF,
                    channel_index=channel_index,
                    priority=BotMessagePriority.NORMAL,
                )
        except Exception as e:
            logger.error(f"Error processing queued traceroute: {e}")

    def _check_traceroute_timeouts(self) -> None:
        """Check for timed-out traceroutes and notify users."""
        if not self._pending_traceroutes:
            return

        current_time = time.time()
        timed_out = []

        # Find timed-out traceroutes
        for dest_id, (
            requester_id,
            requester_name,
            channel_index,
            start_time,
        ) in self._pending_traceroutes.items():
            if current_time - start_time > self._traceroute_timeout:
                timed_out.append((dest_id, requester_id, requester_name, channel_index))

        # Process timed-out traceroutes
        for dest_id, requester_id, requester_name, channel_index in timed_out:
            del self._pending_traceroutes[dest_id]

            display_name = requester_name or f"!{requester_id:08x}"
            if len(display_name) > 15:
                display_name = display_name[:12] + "..."

            # Notify user of timeout
            self.queue_message(
                text=f"⏱️ TR to {display_name} timed out",
                destination=0xFFFFFFFF,
                channel_index=channel_index,
                priority=BotMessagePriority.LOW,
            )

            channel_name = self._get_channel_name(channel_index)
            self._log_activity(
                "traceroute_timeout",
                {
                    "target": f"!{dest_id:08x}",
                    "target_name": requester_name,
                    "channel": channel_name or f"ch{channel_index}",
                    "status": "timed out",
                },
            )
            logger.info(f"Traceroute to !{dest_id:08x} timed out after 60s")

    def _cmd_help(self, ctx: CommandContext) -> str:
        """Handle !help command."""
        # Keep help message short to fit in Meshtastic payload (~230 bytes)
        enabled_cmds = [
            name
            for name in sorted(self._commands.keys())
            if name not in self._disabled_commands
        ]
        cmd_list = " ".join(f"!{name}" for name in enabled_cmds)
        return f"Cmds: {cmd_list}"

    def _cmd_nodes(self, ctx: CommandContext) -> str:
        """Handle !nodes command."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()

            # Total nodes in database (not archived)
            cursor.execute(
                "SELECT COUNT(*) as cnt FROM node_info WHERE COALESCE(archived, 0) = 0"
            )
            total_nodes = cursor.fetchone()["cnt"]

            # Nodes seen in last 15 minutes (online)
            fifteen_min_ago = time.time() - 900
            cursor.execute(
                """
                SELECT COUNT(DISTINCT from_node_id) as cnt
                FROM packet_history
                WHERE timestamp > ?
                """,
                (fifteen_min_ago,),
            )
            online_nodes = cursor.fetchone()["cnt"]

            # Nodes with position data (from POSITION_APP packets)
            cursor.execute(
                """
                SELECT COUNT(DISTINCT from_node_id) as cnt FROM packet_history
                WHERE portnum = 3 AND raw_payload IS NOT NULL
                """
            )
            with_position = cursor.fetchone()["cnt"]

            conn.close()

            return (
                f"📡 {total_nodes} nodes, {online_nodes} online, {with_position} w/pos"
            )
        except Exception as e:
            logger.error(f"Error in nodes command: {e}", exc_info=True)
            return "Node info unavailable"

    def _cmd_mystats(self, ctx: CommandContext) -> str:
        """Handle !mystats command - show requesting node's statistics."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            node_id = ctx.sender_id

            # Get node name
            cursor.execute(
                "SELECT long_name, short_name FROM node_info WHERE node_id = ?",
                (node_id,),
            )
            node_row = cursor.fetchone()
            node_name = (
                node_row["short_name"] or node_row["long_name"]
                if node_row
                else f"!{node_id:08x}"
            )
            if node_name and len(node_name) > 10:
                node_name = node_name[:10]

            # Get message counts (last 24h and 7d)
            one_day_ago = time.time() - 86400
            seven_days_ago = time.time() - (7 * 86400)

            cursor.execute(
                """
                SELECT COUNT(*) as cnt FROM packet_history
                WHERE from_node_id = ? AND timestamp > ?
                """,
                (node_id, one_day_ago),
            )
            msgs_24h = cursor.fetchone()["cnt"]

            cursor.execute(
                """
                SELECT COUNT(*) as cnt FROM packet_history
                WHERE from_node_id = ? AND timestamp > ?
                """,
                (node_id, seven_days_ago),
            )
            msgs_7d = cursor.fetchone()["cnt"]

            # Get average hop count (from hop_start - hop_limit when > 0)
            cursor.execute(
                """
                SELECT AVG(hop_start - hop_limit) as avg_hops
                FROM packet_history
                WHERE from_node_id = ? AND timestamp > ?
                AND hop_start IS NOT NULL AND hop_limit IS NOT NULL
                AND hop_start > hop_limit
                """,
                (node_id, seven_days_ago),
            )
            hop_row = cursor.fetchone()
            avg_hops = hop_row["avg_hops"] if hop_row and hop_row["avg_hops"] else 0

            # Get latest telemetry
            cursor.execute(
                """
                SELECT battery_level, voltage, uptime_seconds, channel_utilization
                FROM telemetry_data
                WHERE node_id = ?
                ORDER BY timestamp DESC LIMIT 1
                """,
                (node_id,),
            )
            telem = cursor.fetchone()

            # Calculate availability (% of 15-min windows with activity in last 24h)
            # 24 hours = 96 fifteen-minute windows
            cursor.execute(
                """
                SELECT COUNT(DISTINCT CAST(timestamp / 900 AS INTEGER)) as active_windows
                FROM packet_history
                WHERE from_node_id = ? AND timestamp > ?
                """,
                (node_id, one_day_ago),
            )
            active_windows = cursor.fetchone()["active_windows"]
            availability = min(100, int((active_windows / 96.0) * 100))

            conn.close()

            # Build concise response (fits in ~230 bytes)
            lines = [f"📊 {node_name}"]

            # Availability and messages
            lines.append(f"Avail: {availability}% | Msgs: {msgs_24h}/24h {msgs_7d}/7d")

            # Hops info
            if avg_hops > 0:
                lines.append(f"Avg hops: {avg_hops:.1f}")

            # Telemetry line
            if telem:
                telem_parts = []
                if telem["battery_level"] is not None:
                    telem_parts.append(f"🔋{telem['battery_level']}%")
                if telem["voltage"] is not None:
                    telem_parts.append(f"{telem['voltage']:.1f}V")
                if telem["uptime_seconds"] is not None:
                    uptime_h = telem["uptime_seconds"] // 3600
                    telem_parts.append(f"Up:{uptime_h}h")
                if telem["channel_utilization"] is not None:
                    telem_parts.append(f"ChUtil:{telem['channel_utilization']:.0f}%")
                if telem_parts:
                    lines.append(" ".join(telem_parts))

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"Error in mystats command: {e}", exc_info=True)
            return "Stats unavailable"

    def _cmd_uptime(self, ctx: CommandContext) -> str:
        """Handle !uptime command."""
        if self._start_time is None:
            return "Uptime unknown"

        uptime_seconds = time.time() - self._start_time
        hours = int(uptime_seconds // 3600)
        minutes = int((uptime_seconds % 3600) // 60)
        seconds = int(uptime_seconds % 60)

        if hours > 0:
            return f"⏱️ Bot uptime: {hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"⏱️ Bot uptime: {minutes}m {seconds}s"
        else:
            return f"⏱️ Bot uptime: {seconds}s"

    def _cmd_whoami(self, ctx: CommandContext) -> str:
        """Handle !whoami command - detailed node info."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            node_id = ctx.sender_id

            # Get node info
            cursor.execute(
                """SELECT long_name, short_name, hw_model, role, primary_channel,
                   first_seen, last_updated
                   FROM node_info WHERE node_id = ?""",
                (node_id,),
            )
            node = cursor.fetchone()

            if not node:
                conn.close()
                return f"📋 !{node_id:08x} - No data"

            # Get latest telemetry
            cursor.execute(
                """SELECT battery_level, voltage, uptime_seconds
                   FROM telemetry_data WHERE node_id = ?
                   ORDER BY timestamp DESC LIMIT 1""",
                (node_id,),
            )
            telem = cursor.fetchone()
            conn.close()

            lines = [f"📋 {node['long_name'] or node['short_name'] or '?'}"]
            lines.append(f"ID: !{node_id:08x}")
            if node["hw_model"]:
                lines.append(f"HW: {node['hw_model']}")
            if node["role"]:
                lines.append(f"Role: {node['role']}")
            if telem and telem["uptime_seconds"]:
                hours = telem["uptime_seconds"] // 3600
                lines.append(f"Node up: {hours}h")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"Error in whoami: {e}", exc_info=True)
            return "Info unavailable"

    def _cmd_heard(self, ctx: CommandContext) -> str:
        """Handle !heard command - show who heard the requester."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            node_id = ctx.sender_id
            one_day_ago = time.time() - 86400

            # Find gateways that received packets from this node
            cursor.execute(
                """SELECT gateway_id, COUNT(*) as cnt, AVG(snr) as avg_snr,
                   MAX(timestamp) as last_ts
                   FROM packet_history
                   WHERE from_node_id = ? AND timestamp > ? AND gateway_id IS NOT NULL
                   GROUP BY gateway_id
                   ORDER BY cnt DESC LIMIT 5""",
                (node_id, one_day_ago),
            )
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                return "👂 No gateways heard you in 24h"

            lines = ["👂 Heard by (24h):"]
            for row in rows:
                gw = row["gateway_id"] or "?"
                if len(gw) > 10:
                    gw = gw[-8:]
                snr = f"{row['avg_snr']:.1f}dB" if row["avg_snr"] else "?"
                lines.append(f"{gw}: {row['cnt']}pkts {snr}")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"Error in heard: {e}", exc_info=True)
            return "Heard data unavailable"

    def _cmd_neighbors(self, ctx: CommandContext) -> str:
        """Handle !neighbors command - nearby nodes by direct link."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            node_id = ctx.sender_id
            one_day_ago = time.time() - 86400

            # Find nodes that relayed packets with good SNR (direct neighbors)
            cursor.execute(
                """SELECT DISTINCT relay_node, AVG(snr) as avg_snr, COUNT(*) as cnt
                   FROM packet_history
                   WHERE from_node_id = ? AND timestamp > ?
                   AND relay_node IS NOT NULL AND relay_node != 0
                   GROUP BY relay_node
                   ORDER BY avg_snr DESC LIMIT 5""",
                (node_id, one_day_ago),
            )
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                return "📡 No relay neighbors found"

            lines = ["📡 Neighbors (24h):"]
            for row in rows:
                snr = f"{row['avg_snr']:.1f}dB" if row["avg_snr"] else "?"
                lines.append(f"!{row['relay_node']:08x}: {snr}")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"Error in neighbors: {e}", exc_info=True)
            return "Neighbor data unavailable"

    def _cmd_find(self, ctx: CommandContext) -> str:
        """Handle !find <name> command - find node by name."""
        try:
            if not ctx.args:
                return "Usage: !find <name>"

            search = " ".join(ctx.args).lower()
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                """SELECT node_id, long_name, short_name, last_updated
                   FROM node_info
                   WHERE (LOWER(long_name) LIKE ? OR LOWER(short_name) LIKE ?)
                   AND COALESCE(archived, 0) = 0
                   ORDER BY last_updated DESC LIMIT 3""",
                (f"%{search}%", f"%{search}%"),
            )
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                return f"🔍 No nodes match '{search}'"

            lines = [f"🔍 Found {len(rows)}:"]
            for row in rows:
                name = row["short_name"] or row["long_name"] or "?"
                age = time.time() - row["last_updated"]
                if age < 3600:
                    age_str = f"{int(age / 60)}m"
                elif age < 86400:
                    age_str = f"{int(age / 3600)}h"
                else:
                    age_str = f"{int(age / 86400)}d"
                lines.append(f"{name} !{row['node_id']:08x} ({age_str})")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"Error in find: {e}", exc_info=True)
            return "Find failed"

    def _cmd_busy(self, ctx: CommandContext) -> str:
        """Handle !busy command - channel utilization."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            one_hour_ago = time.time() - 3600

            # Get average channel utilization from recent telemetry
            cursor.execute(
                """SELECT AVG(channel_utilization) as avg_util,
                   AVG(air_util_tx) as avg_tx,
                   COUNT(DISTINCT node_id) as node_cnt
                   FROM telemetry_data
                   WHERE timestamp > ? AND channel_utilization IS NOT NULL""",
                (one_hour_ago,),
            )
            row = cursor.fetchone()

            # Get packet rate
            cursor.execute(
                "SELECT COUNT(*) as cnt FROM packet_history WHERE timestamp > ?",
                (one_hour_ago,),
            )
            pkt_row = cursor.fetchone()
            conn.close()

            if not row or row["avg_util"] is None:
                return f"📊 {pkt_row['cnt']} pkts/h | No util data"

            return (
                f"📊 Channel: {row['avg_util']:.1f}% util\n"
                f"TX: {row['avg_tx']:.1f}% | {pkt_row['cnt']} pkts/h\n"
                f"From {row['node_cnt']} nodes"
            )
        except Exception as e:
            logger.error(f"Error in busy: {e}", exc_info=True)
            return "Busy data unavailable"

    def _cmd_lowbat(self, ctx: CommandContext) -> str:
        """Handle !lowbat command - nodes with low battery."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            one_day_ago = time.time() - 86400

            # Get nodes with battery < 20% using most recent telemetry only
            cursor.execute(
                """SELECT t.node_id, n.short_name, n.long_name,
                   t.battery_level, t.voltage
                   FROM telemetry_data t
                   JOIN node_info n ON t.node_id = n.node_id
                   JOIN (
                       SELECT node_id, MAX(timestamp) as max_ts
                       FROM telemetry_data
                       WHERE timestamp > ? AND battery_level IS NOT NULL
                       GROUP BY node_id
                   ) latest ON t.node_id = latest.node_id AND t.timestamp = latest.max_ts
                   WHERE t.battery_level < 20 AND t.battery_level > 0
                   ORDER BY t.battery_level ASC LIMIT 5""",
                (one_day_ago,),
            )
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                return "🔋 No low battery nodes"

            lines = ["🔋 Low battery:"]
            for row in rows:
                name = row["short_name"] or row["long_name"] or f"!{row['node_id']:08x}"
                if len(name) > 10:
                    name = name[:10]
                v = f"{row['voltage']:.1f}V" if row["voltage"] else ""
                lines.append(f"{name}: {row['battery_level']}% {v}")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"Error in lowbat: {e}", exc_info=True)
            return "Battery data unavailable"

    def _cmd_offline(self, ctx: CommandContext) -> str:
        """Handle !offline command - recently offline nodes."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            one_hour_ago = time.time() - 3600
            one_day_ago = time.time() - 86400

            # Nodes seen in last 24h but not in last hour
            cursor.execute(
                """SELECT n.node_id, n.short_name, n.long_name,
                   MAX(p.timestamp) as last_seen
                   FROM node_info n
                   JOIN packet_history p ON n.node_id = p.from_node_id
                   WHERE p.timestamp > ? AND p.timestamp < ?
                   AND COALESCE(n.archived, 0) = 0
                   GROUP BY n.node_id
                   ORDER BY last_seen DESC LIMIT 5""",
                (one_day_ago, one_hour_ago),
            )
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                return "📴 No recently offline nodes"

            lines = ["📴 Offline (1-24h):"]
            for row in rows:
                name = row["short_name"] or row["long_name"] or f"!{row['node_id']:08x}"
                if len(name) > 12:
                    name = name[:12]
                age = time.time() - row["last_seen"]
                hours = int(age / 3600)
                lines.append(f"{name}: {hours}h ago")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"Error in offline: {e}", exc_info=True)
            return "Offline data unavailable"

    def _cmd_quality(self, ctx: CommandContext) -> str:
        """Handle !quality command - link quality for requester."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            node_id = ctx.sender_id
            one_day_ago = time.time() - 86400

            # Get SNR stats for this node's packets
            cursor.execute(
                """SELECT AVG(snr) as avg_snr, MIN(snr) as min_snr,
                   MAX(snr) as max_snr, COUNT(*) as pkt_cnt,
                   AVG(hop_start - hop_limit) as avg_hops
                   FROM packet_history
                   WHERE from_node_id = ? AND timestamp > ?
                   AND snr IS NOT NULL""",
                (node_id, one_day_ago),
            )
            row = cursor.fetchone()
            conn.close()

            if not row or row["pkt_cnt"] == 0:
                return "📶 No quality data in 24h"

            lines = ["📶 Your link quality (24h):"]
            lines.append(
                f"SNR: {row['avg_snr']:.1f}dB avg "
                f"({row['min_snr']:.0f}/{row['max_snr']:.0f})"
            )
            if row["avg_hops"] and row["avg_hops"] > 0:
                lines.append(f"Avg hops: {row['avg_hops']:.1f}")
            lines.append(f"Packets: {row['pkt_cnt']}")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"Error in quality: {e}", exc_info=True)
            return "Quality data unavailable"

    def _cmd_distance(self, ctx: CommandContext) -> str:
        """Handle !distance <node> command - distance to another node."""
        try:
            if not ctx.args:
                return "Usage: !distance <node>"

            import math

            from meshtastic.protobuf import mesh_pb2

            from ..database.connection import get_db_connection

            target = ctx.args[0].lower().replace("!", "")
            conn = get_db_connection()
            cursor = conn.cursor()

            # Helper to get latest position from POSITION_APP packets
            def get_position(node_id: int) -> tuple[float | None, float | None]:
                cursor.execute(
                    """SELECT raw_payload FROM packet_history
                       WHERE from_node_id = ? AND portnum = 3 AND raw_payload IS NOT NULL
                       ORDER BY timestamp DESC LIMIT 1""",
                    (node_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return None, None
                try:
                    pos = mesh_pb2.Position()
                    pos.ParseFromString(row["raw_payload"])
                    lat = pos.latitude_i / 1e7 if pos.latitude_i else None
                    lon = pos.longitude_i / 1e7 if pos.longitude_i else None
                    return lat, lon
                except Exception:
                    return None, None

            # Get requester position
            src_lat, src_lon = get_position(ctx.sender_id)
            if not src_lat or not src_lon:
                conn.close()
                return "📍 Your position unknown"

            # Find target node
            target_id = None
            target_name = None
            if target.isalnum() and len(target) == 8:
                try:
                    target_id = int(target, 16)
                    cursor.execute(
                        "SELECT short_name FROM node_info WHERE node_id = ?",
                        (target_id,),
                    )
                    row = cursor.fetchone()
                    target_name = row["short_name"] if row else None
                except ValueError:
                    pass

            if target_id is None:
                cursor.execute(
                    """SELECT node_id, short_name FROM node_info
                       WHERE LOWER(short_name) LIKE ? OR LOWER(long_name) LIKE ?
                       LIMIT 1""",
                    (f"%{target}%", f"%{target}%"),
                )
                row = cursor.fetchone()
                if row:
                    target_id = row["node_id"]
                    target_name = row["short_name"]

            if not target_id:
                conn.close()
                return f"📍 Node '{target}' not found"

            dst_lat, dst_lon = get_position(target_id)
            conn.close()

            if not dst_lat or not dst_lon:
                return f"📍 {target_name or target} has no position"

            # Haversine formula
            lat1, lon1 = math.radians(src_lat), math.radians(src_lon)
            lat2, lon2 = math.radians(dst_lat), math.radians(dst_lon)
            dlat, dlon = lat2 - lat1, lon2 - lon1
            a = (
                math.sin(dlat / 2) ** 2
                + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
            )
            c = 2 * math.asin(math.sqrt(a))
            km = 6371 * c

            name = target_name or f"!{target_id:08x}"
            if km < 1:
                return f"📍 {name}: {km * 1000:.0f}m"
            else:
                return f"📍 {name}: {km:.1f}km"

        except Exception as e:
            logger.error(f"Error in distance: {e}", exc_info=True)
            return "Distance calc failed"

    def _cmd_time(self, ctx: CommandContext) -> str:
        """Handle !time command - server time."""
        from datetime import datetime

        now = datetime.now()
        utc = datetime.utcnow()
        return f"🕐 {now.strftime('%Y-%m-%d %H:%M:%S')} (UTC: {utc.strftime('%H:%M')})"

    def _cmd_weather(self, ctx: CommandContext) -> str:
        """Handle !weather command - environment sensor data."""
        try:
            from meshtastic.protobuf import telemetry_pb2

            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            six_hours_ago = time.time() - 21600  # 6 hours for better coverage

            # Get recent TELEMETRY_APP packets and parse for environment data
            cursor.execute(
                """SELECT ph.from_node_id, ph.raw_payload, ph.timestamp, n.short_name
                   FROM packet_history ph
                   LEFT JOIN node_info n ON ph.from_node_id = n.node_id
                   WHERE ph.portnum = 67 AND ph.raw_payload IS NOT NULL
                   AND ph.timestamp > ?
                   ORDER BY ph.timestamp DESC LIMIT 50""",
                (six_hours_ago,),
            )
            rows = cursor.fetchall()
            conn.close()

            # Parse and find environment readings
            env_readings: list[dict] = []
            seen_nodes: set[int] = set()

            for row in rows:
                node_id = row["from_node_id"]
                if node_id in seen_nodes:
                    continue  # Only show most recent per node

                try:
                    tel = telemetry_pb2.Telemetry()
                    tel.ParseFromString(row["raw_payload"])
                    if tel.HasField("environment_metrics"):
                        env = tel.environment_metrics
                        # Only include if has meaningful data
                        if env.temperature != 0 or env.relative_humidity != 0:
                            seen_nodes.add(node_id)
                            env_readings.append(
                                {
                                    "name": row["short_name"] or f"!{node_id:08x}",
                                    "temp": env.temperature,
                                    "humidity": env.relative_humidity,
                                    "pressure": env.barometric_pressure,
                                }
                            )
                            if len(env_readings) >= 3:
                                break
                except Exception:
                    continue

            if not env_readings:
                return "🌡️ No weather sensors active"

            lines = ["🌡️ Environment:"]
            for r in env_readings:
                name = r["name"]
                if len(name) > 8:
                    name = name[:8]
                parts = []
                if r["temp"] != 0:
                    parts.append(f"{r['temp']:.1f}°C")
                if r["humidity"] != 0:
                    parts.append(f"{r['humidity']:.0f}%RH")
                if r["pressure"] != 0:
                    parts.append(f"{r['pressure']:.0f}hPa")
                if parts:
                    lines.append(f"{name}: {' '.join(parts)}")

            return "\n".join(lines) if len(lines) > 1 else "🌡️ No readings"

        except Exception as e:
            logger.error(f"Weather data error: {e}", exc_info=True)
            return "🌡️ Weather data unavailable"

    def _cmd_lastseen(self, ctx: CommandContext) -> str:
        """Handle !lastseen <node> command - when was node last active."""
        try:
            if not ctx.args:
                return "Usage: !lastseen <node>"

            from ..database.connection import get_db_connection

            target = " ".join(ctx.args).lower().replace("!", "")
            conn = get_db_connection()
            cursor = conn.cursor()

            # Find node
            if len(target) == 8 and all(c in "0123456789abcdef" for c in target):
                try:
                    target_id = int(target, 16)
                    cursor.execute(
                        """SELECT n.node_id, n.short_name, n.long_name,
                           MAX(p.timestamp) as last_seen
                           FROM node_info n
                           LEFT JOIN packet_history p ON n.node_id = p.from_node_id
                           WHERE n.node_id = ?
                           GROUP BY n.node_id""",
                        (target_id,),
                    )
                except ValueError:
                    pass
            else:
                cursor.execute(
                    """SELECT n.node_id, n.short_name, n.long_name,
                       MAX(p.timestamp) as last_seen
                       FROM node_info n
                       LEFT JOIN packet_history p ON n.node_id = p.from_node_id
                       WHERE LOWER(n.short_name) LIKE ? OR LOWER(n.long_name) LIKE ?
                       GROUP BY n.node_id LIMIT 1""",
                    (f"%{target}%", f"%{target}%"),
                )

            row = cursor.fetchone()
            conn.close()

            if not row:
                return f"👁️ '{target}' not found"

            name = row["short_name"] or row["long_name"] or f"!{row['node_id']:08x}"
            if not row["last_seen"]:
                return f"👁️ {name}: never seen"

            age = time.time() - row["last_seen"]
            if age < 60:
                age_str = f"{int(age)}s ago"
            elif age < 3600:
                age_str = f"{int(age / 60)}m ago"
            elif age < 86400:
                age_str = f"{int(age / 3600)}h ago"
            else:
                age_str = f"{int(age / 86400)}d ago"

            return f"👁️ {name}: {age_str}"

        except Exception as e:
            logger.error(f"Error in lastseen: {e}", exc_info=True)
            return "Lastseen failed"

    def _cmd_top(self, ctx: CommandContext) -> str:
        """Handle !top command - most active nodes this week."""
        try:
            from ..database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            one_week_ago = time.time() - (7 * 86400)

            cursor.execute(
                """SELECT p.from_node_id, n.short_name, n.long_name,
                   COUNT(*) as pkt_cnt
                   FROM packet_history p
                   JOIN node_info n ON p.from_node_id = n.node_id
                   WHERE p.timestamp > ?
                   GROUP BY p.from_node_id
                   ORDER BY pkt_cnt DESC LIMIT 5""",
                (one_week_ago,),
            )
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                return "🏆 No activity this week"

            lines = ["🏆 Top nodes (7d):"]
            for i, row in enumerate(rows, 1):
                name = (
                    row["short_name"]
                    or row["long_name"]
                    or f"!{row['from_node_id']:08x}"
                )
                if len(name) > 12:
                    name = name[:12]
                lines.append(f"{i}. {name}: {row['pkt_cnt']} pkts")

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"Error in top: {e}", exc_info=True)
            return "Top data unavailable"

    # =========================================================================
    # Channel Directory Commands
    # =========================================================================

    def _cmd_channels(self, ctx: CommandContext) -> str:
        """Handle !channels command - list community channels."""
        try:
            from ..database.channel_directory_repository import (
                ChannelDirectoryRepository,
            )

            channels = ChannelDirectoryRepository.get_all_channels(active_only=True)
            if not channels:
                return "📻 No channels registered. Use !addchannel <name> <key> [desc]"

            lines = [f"📻 Channels ({len(channels)}):"]
            for ch in channels:
                name = ch["channel_name"]
                desc = ch.get("description") or ""
                if desc and len(desc) > 20:
                    desc = desc[:17] + "..."
                if desc:
                    lines.append(f"• {name}: {desc}")
                else:
                    lines.append(f"• {name}")

            # Truncate to fit Meshtastic payload (~230 bytes)
            result = "\n".join(lines)
            if len(result) > 220:
                result = result[:217] + "..."

            return result
        except Exception as e:
            logger.error(f"Error in channels command: {e}", exc_info=True)
            return "Channel list unavailable"

    def _cmd_addchannel(self, ctx: CommandContext) -> str:
        """Handle !addchannel <name> <psk> [description...] command.

        Examples:
            !addchannel Dispatches AQ== EMS/Fire dispatches
            !addchannel Weather AQ==
        """
        try:
            if len(ctx.args) < 2:
                return "Usage: !addchannel <name> <key> [description]"

            channel_name = ctx.args[0]
            psk = ctx.args[1]
            description = " ".join(ctx.args[2:]) if len(ctx.args) > 2 else None

            # Validate channel name length
            if len(channel_name) > 30:
                return "Channel name too long (max 30)"

            # Validate PSK is base64-ish (basic check)
            if len(psk) < 2 or len(psk) > 48:
                return "Invalid key (base64 expected)"

            from ..database.channel_directory_repository import (
                ChannelDirectoryRepository,
            )

            result = ChannelDirectoryRepository.add_channel(
                channel_name=channel_name,
                psk=psk,
                description=description,
                registered_by_node_id=ctx.sender_id,
                registered_by_name=ctx.sender_name,
            )

            if result["success"]:
                self._log_activity(
                    "channel_registered",
                    {
                        "channel": channel_name,
                        "by": ctx.sender_name or f"!{ctx.sender_id:08x}",
                    },
                )
                return f"✅ Channel '{channel_name}' registered!"
            else:
                return f"❌ {result['error']}"
        except Exception as e:
            logger.error(f"Error in addchannel command: {e}", exc_info=True)
            return "Failed to add channel"

    def _cmd_rmchannel(self, ctx: CommandContext) -> str:
        """Handle !rmchannel <name> command - remove your registered channel."""
        try:
            if not ctx.args:
                return "Usage: !rmchannel <name>"

            channel_name = ctx.args[0]

            from ..database.channel_directory_repository import (
                ChannelDirectoryRepository,
            )

            result = ChannelDirectoryRepository.remove_channel(
                channel_name=channel_name,
                requester_node_id=ctx.sender_id,
            )

            if result["success"]:
                self._log_activity(
                    "channel_removed",
                    {
                        "channel": channel_name,
                        "by": ctx.sender_name or f"!{ctx.sender_id:08x}",
                    },
                )
                return f"✅ Channel '{channel_name}' removed"
            else:
                return f"❌ {result['error']}"
        except Exception as e:
            logger.error(f"Error in rmchannel command: {e}", exc_info=True)
            return "Failed to remove channel"

    def _cmd_channelinfo(self, ctx: CommandContext) -> str:
        """Handle !channelinfo <name> command - show channel details."""
        try:
            if not ctx.args:
                return "Usage: !channelinfo <name>"

            channel_name = ctx.args[0]

            from ..database.channel_directory_repository import (
                ChannelDirectoryRepository,
            )

            ch = ChannelDirectoryRepository.get_channel(channel_name)
            if not ch:
                return f"📻 Channel '{channel_name}' not found"

            lines = [f"📻 {ch['channel_name']}"]

            if ch.get("description"):
                desc = ch["description"]
                if len(desc) > 60:
                    desc = desc[:57] + "..."
                lines.append(desc)

            lines.append(f"Key: {ch['psk']}")

            if ch.get("registered_by_name"):
                lines.append(f"By: {ch['registered_by_name']}")

            # Show how long ago it was registered
            age = time.time() - ch["created_at"]
            if age < 3600:
                age_str = f"{int(age / 60)}m ago"
            elif age < 86400:
                age_str = f"{int(age / 3600)}h ago"
            else:
                age_str = f"{int(age / 86400)}d ago"
            lines.append(f"Added: {age_str}")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"Error in channelinfo command: {e}", exc_info=True)
            return "Channel info unavailable"

    # =========================================================================
    # Channel Directory Scheduled Broadcast
    # =========================================================================

    def _start_broadcast_thread(self) -> None:
        """Start the background thread that broadcasts channel directory."""
        if self._broadcast_thread and self._broadcast_thread.is_alive():
            return

        self._broadcast_thread = threading.Thread(
            target=self._broadcast_loop, daemon=True, name="BotChannelBroadcast"
        )
        self._broadcast_thread.start()
        logger.info("Channel directory broadcast thread started")

    def _broadcast_loop(self) -> None:
        """Background loop that broadcasts channel directory twice per day."""
        # Wait 60 seconds after startup before first potential broadcast
        self._stop_event.wait(timeout=60.0)

        while not self._stop_event.is_set():
            try:
                now = time.time()
                elapsed = now - self._last_broadcast_time
                interval_seconds = self._broadcast_interval_hours * 3600

                if elapsed >= interval_seconds:
                    self._broadcast_channel_directory()
                    self._last_broadcast_time = now

                # Check every 5 minutes
                self._stop_event.wait(timeout=300.0)
            except Exception as e:
                logger.error(
                    f"Error in channel broadcast loop: {e}", exc_info=True
                )
                self._stop_event.wait(timeout=60.0)

        logger.info("Channel directory broadcast thread stopped")

    def _broadcast_channel_directory(self) -> None:
        """Build and send the channel directory advertisement message."""
        try:
            from ..database.channel_directory_repository import (
                ChannelDirectoryRepository,
            )

            channels = ChannelDirectoryRepository.get_all_channels(active_only=True)
            if not channels:
                logger.debug("No channels to broadcast")
                return

            lines = [f"📻 Channel Directory ({len(channels)}):"]
            for ch in channels:
                name = ch["channel_name"]
                psk = ch["psk"]
                desc = ch.get("description") or ""
                if desc:
                    if len(desc) > 25:
                        desc = desc[:22] + "..."
                    lines.append(f"• {name} [{psk}] - {desc}")
                else:
                    lines.append(f"• {name} [{psk}]")

            lines.append("!channelinfo <name> for details")

            message = "\n".join(lines)
            # Truncate to fit Meshtastic payload
            if len(message) > 220:
                message = message[:217] + "..."

            self.queue_message(
                text=message,
                destination=0xFFFFFFFF,  # Broadcast
                channel_index=self._respond_channel_index,
                priority=BotMessagePriority.LOW,
            )

            self._log_activity(
                "channel_broadcast",
                {"channel_count": len(channels)},
            )
            logger.info(
                f"Broadcast channel directory ({len(channels)} channels)"
            )

        except Exception as e:
            logger.error(
                f"Error broadcasting channel directory: {e}", exc_info=True
            )


# Singleton accessor
_bot_service_instance: BotService | None = None


def get_bot_service() -> BotService:
    """Get the singleton bot service instance."""
    global _bot_service_instance
    if _bot_service_instance is None:
        _bot_service_instance = BotService()
    return _bot_service_instance


def init_bot_service() -> BotService:
    """Initialize and start the bot service."""
    service = get_bot_service()
    service.start()
    return service
