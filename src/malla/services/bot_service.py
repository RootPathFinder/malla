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

            # Only process text messages
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

            # Check if we should respond on this channel
            if channel_name and channel_name not in self._listen_channels:
                # Also check if it's a direct message (to us specifically)
                to_id = packet.get("to") or packet.get("toId")
                if to_id == 0xFFFFFFFF:  # Broadcast
                    logger.debug(
                        f"Ignoring command on non-monitored channel: {channel_name}"
                    )
                    return

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
            )

            logger.info(
                f"Bot command received: {self._command_prefix}{command} "
                f"from {context.sender_name or f'!{sender_id:08x}'}"
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
                },
            )

            # Execute handler
            handler = self._commands[command]
            try:
                response = handler(context)
                if response:
                    self._stats["commands_processed"] += 1
                    # Queue the response
                    priority = (
                        BotMessagePriority.HIGH
                        if command == "ping"
                        else BotMessagePriority.NORMAL
                    )
                    self.queue_message(
                        text=response,
                        destination=0xFFFFFFFF,  # Broadcast response
                        channel_index=channel_index,  # Respond on same channel
                        priority=priority,
                        reply_to_node=sender_id,
                    )
                    # Log the response
                    self._log_activity(
                        "response_queued",
                        {
                            "command": command,
                            "response_preview": response[:100],
                            "priority": priority.name,
                        },
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

    def _worker_loop(self) -> None:
        """Worker loop that sends queued messages."""
        logger.info("Bot worker loop started")

        while not self._stop_event.is_set():
            try:
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
                if success:
                    self._last_send_time = time.time()
                    self._stats["messages_sent"] += 1
                    self._log_activity(
                        "message_sent",
                        {
                            "text_preview": msg.text[:80],
                            "destination": f"!{msg.destination:08x}"
                            if msg.destination != 0xFFFFFFFF
                            else "broadcast",
                            "channel_index": msg.channel_index,
                        },
                    )
                    logger.info(f"Bot sent: {msg.text[:50]}...")
                else:
                    self._stats["messages_failed"] += 1
                    self._log_activity(
                        "message_failed",
                        {"text_preview": msg.text[:80], "reason": "send failed"},
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

    def _send_message(self, msg: BotMessage) -> bool:
        """Send a message via the TCP interface."""
        try:
            from .tcp_publisher import get_tcp_publisher

            publisher = get_tcp_publisher()

            if not publisher.is_connected:
                logger.warning("Cannot send bot message: TCP not connected")
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
            from .tcp_publisher import get_tcp_publisher

            publisher = get_tcp_publisher()
            if not publisher.is_connected or publisher._interface is None:
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
            from ..database.repositories import NodeRepository, PacketRepository

            # Get node count using get_nodes
            result = NodeRepository.get_nodes(limit=10000)
            nodes = result.get("data", [])
            online_count = sum(1 for n in nodes if n.get("is_online", False))
            total_count = len(nodes)

            # Get recent packet count
            recent_packets = PacketRepository.get_packet_count_since(
                time.time() - 3600  # Last hour
            )

            # Get gateway info
            from .tcp_publisher import get_tcp_publisher

            publisher = get_tcp_publisher()
            gateway_name = publisher.get_local_node_name() or "Unknown"

            return (
                f"📊 Mesh Status\n"
                f"Gateway: {gateway_name}\n"
                f"Nodes: {online_count}/{total_count} online\n"
                f"Packets (1h): {recent_packets}"
            )
        except Exception as e:
            logger.error(f"Error in status command: {e}")
            return "Status unavailable"

    def _cmd_traceroute(self, ctx: CommandContext) -> str:
        """Handle !traceroute command."""
        try:
            from .tcp_publisher import get_tcp_publisher

            publisher = get_tcp_publisher()
            if not publisher.is_connected or publisher._interface is None:
                return "Traceroute unavailable - not connected"

            # Send traceroute to the requesting node
            publisher._interface.sendTraceRoute(
                dest=ctx.sender_id,
                hopLimit=7,
            )

            sender_name = ctx.sender_name or f"!{ctx.sender_id:08x}"
            return f"🔍 Traceroute initiated to {sender_name}"
        except Exception as e:
            logger.error(f"Error in traceroute command: {e}")
            return "Traceroute failed"

    def _cmd_help(self, ctx: CommandContext) -> str:
        """Handle !help command."""
        lines = ["📖 Available Commands:"]
        for name in sorted(self._commands.keys()):
            desc = self._command_descriptions.get(name, "")
            lines.append(f"  {self._command_prefix}{name} - {desc}")
        return "\n".join(lines)

    def _cmd_nodes(self, ctx: CommandContext) -> str:
        """Handle !nodes command."""
        try:
            from ..database.repositories import NodeRepository

            result = NodeRepository.get_nodes(limit=10000)
            nodes = result.get("data", [])
            online = sum(1 for n in nodes if n.get("is_online", False))
            with_position = sum(
                1
                for n in nodes
                if n.get("latitude") is not None and n.get("longitude") is not None
            )

            return (
                f"📡 Nodes: {len(nodes)} total\n"
                f"Online: {online}\n"
                f"With position: {with_position}"
            )
        except Exception as e:
            logger.error(f"Error in nodes command: {e}")
            return "Node info unavailable"

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
