"""
MQTT Publisher service for sending Meshtastic packets.

This module provides functionality to publish messages to the Meshtastic mesh
via MQTT, including admin commands for remote node administration.
"""

import base64
import hashlib
import logging
import random
import threading
import time
from typing import Any

import paho.mqtt.client as mqtt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from meshtastic import (
    admin_pb2,
    mesh_pb2,
    mqtt_pb2,
    portnums_pb2,
)
from paho.mqtt.enums import CallbackAPIVersion

from ..config import get_config

logger = logging.getLogger(__name__)


class MQTTPublisher:
    """
    MQTT client for publishing Meshtastic mesh packets.

    This class handles:
    - Connection to MQTT broker
    - Encryption of packets for the mesh
    - Publishing admin commands and other messages
    """

    _instance: "MQTTPublisher | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "MQTTPublisher":
        """Singleton pattern to ensure only one MQTT publisher exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize the MQTT publisher."""
        if self._initialized:
            return

        self._initialized = True
        self._config = get_config()
        self._client: mqtt.Client | None = None
        self._connected = False
        self._connect_lock = threading.Lock()

        # Track pending responses
        self._pending_responses: dict[int, dict[str, Any]] = {}
        self._response_lock = threading.Lock()

    @property
    def is_connected(self) -> bool:
        """Check if connected to MQTT broker."""
        return self._connected and self._client is not None

    def connect(self) -> bool:
        """
        Connect to the MQTT broker.

        Returns:
            True if connection successful or already connected
        """
        with self._connect_lock:
            if self._connected and self._client is not None:
                return True

            try:
                self._client = mqtt.Client(CallbackAPIVersion.VERSION2)

                if self._config.mqtt_username and self._config.mqtt_password:
                    self._client.username_pw_set(
                        self._config.mqtt_username,
                        self._config.mqtt_password,
                    )

                self._client.on_connect = self._on_connect
                self._client.on_disconnect = self._on_disconnect
                self._client.on_message = self._on_message

                logger.info(
                    f"Connecting to MQTT broker at {self._config.mqtt_broker_address}:{self._config.mqtt_port}"
                )

                self._client.connect(
                    self._config.mqtt_broker_address,
                    self._config.mqtt_port,
                    keepalive=60,
                )

                self._client.loop_start()

                # Wait briefly for connection
                for _ in range(10):
                    if self._connected:
                        return True
                    time.sleep(0.1)

                return self._connected

            except Exception as e:
                logger.error(f"Failed to connect to MQTT broker: {e}")
                self._client = None
                self._connected = False
                return False

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        with self._connect_lock:
            if self._client is not None:
                try:
                    self._client.loop_stop()
                    self._client.disconnect()
                except Exception as e:
                    logger.warning(f"Error disconnecting from MQTT: {e}")
                finally:
                    self._client = None
                    self._connected = False

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        """Callback when connected to MQTT broker."""
        if reason_code == 0:
            logger.info("Connected to MQTT broker for publishing")
            self._connected = True

            # Subscribe to receive responses
            topic = f"{self._config.mqtt_topic_prefix}/#"
            client.subscribe(topic)
            logger.info(f"Subscribed to {topic} for admin responses")
        else:
            logger.error(f"MQTT connection failed with code: {reason_code}")
            self._connected = False

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        """Callback when disconnected from MQTT broker."""
        logger.warning(f"Disconnected from MQTT broker: {reason_code}")
        self._connected = False

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        msg: mqtt.MQTTMessage,
    ) -> None:
        """
        Callback when a message is received.

        This handles admin responses from remote nodes.
        """
        try:
            # Parse the service envelope
            service_envelope = mqtt_pb2.ServiceEnvelope()
            service_envelope.ParseFromString(msg.payload)

            if not service_envelope.HasField("packet"):
                return

            mesh_packet = service_envelope.packet

            # Check if this is an admin response
            if mesh_packet.HasField("decoded"):
                if mesh_packet.decoded.portnum == portnums_pb2.PortNum.ADMIN_APP:
                    self._handle_admin_response(mesh_packet)

        except Exception as e:
            logger.debug(f"Error processing MQTT message: {e}")

    def _handle_admin_response(self, mesh_packet: mesh_pb2.MeshPacket) -> None:
        """
        Handle an admin response packet.

        Args:
            mesh_packet: The received mesh packet containing admin response
        """
        try:
            from_node = getattr(mesh_packet, "from")
            packet_id = mesh_packet.id

            logger.info(
                f"Received admin response from node {from_node}, packet_id={packet_id}"
            )

            # Parse the admin message
            admin_msg = admin_pb2.AdminMessage()
            admin_msg.ParseFromString(mesh_packet.decoded.payload)

            # Store the response for retrieval
            with self._response_lock:
                self._pending_responses[packet_id] = {
                    "from_node": from_node,
                    "timestamp": time.time(),
                    "admin_message": admin_msg,
                    "raw_payload": mesh_packet.decoded.payload,
                }

            # Mark the node as administrable
            from ..database.admin_repository import AdminRepository

            firmware_version = None
            device_metadata = None

            # Extract device metadata if present
            if admin_msg.HasField("get_device_metadata_response"):
                meta = admin_msg.get_device_metadata_response
                firmware_version = meta.firmware_version
                device_metadata = str(meta)

            AdminRepository.mark_node_administrable(
                node_id=from_node,
                firmware_version=firmware_version,
                device_metadata=device_metadata,
            )

            logger.info(f"Node {from_node} confirmed as administrable")

        except Exception as e:
            logger.error(f"Error handling admin response: {e}")

    def get_response(
        self, packet_id: int, timeout: float = 30.0
    ) -> dict[str, Any] | None:
        """
        Wait for and retrieve a response to a sent packet.

        Args:
            packet_id: The packet ID to wait for a response to
            timeout: Maximum time to wait in seconds

        Returns:
            Response data or None if timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            with self._response_lock:
                if packet_id in self._pending_responses:
                    return self._pending_responses.pop(packet_id)
            time.sleep(0.1)

        return None

    @staticmethod
    def _derive_key(channel_name: str, key_base64: str) -> bytes:
        """
        Derive encryption key from channel name and base key.

        Args:
            channel_name: The channel name for key derivation
            key_base64: Base64-encoded encryption key

        Returns:
            Derived 32-byte key for AES-256
        """
        try:
            key_bytes = base64.b64decode(key_base64)

            if channel_name:
                hasher = hashlib.sha256()
                hasher.update(key_bytes)
                hasher.update(channel_name.encode("utf-8"))
                return hasher.digest()
            else:
                return key_bytes

        except Exception as e:
            logger.warning(f"Error deriving key: {e}")
            return b"\x00" * 32

    @staticmethod
    def _encrypt_payload(
        payload: bytes,
        packet_id: int,
        sender_id: int,
        key: bytes,
    ) -> bytes:
        """
        Encrypt a payload using AES256-CTR.

        Args:
            payload: The plaintext payload to encrypt
            packet_id: Packet ID for nonce construction
            sender_id: Sender node ID for nonce construction
            key: 32-byte encryption key

        Returns:
            Encrypted payload bytes
        """
        try:
            # Construct nonce: packet_id (8 bytes) + sender_id (8 bytes)
            packet_id_bytes = packet_id.to_bytes(8, byteorder="little")
            sender_id_bytes = sender_id.to_bytes(8, byteorder="little")
            nonce = packet_id_bytes + sender_id_bytes

            # Create AES-CTR cipher
            cipher = Cipher(
                algorithms.AES(key),
                modes.CTR(nonce),
                backend=default_backend(),
            )
            encryptor = cipher.encryptor()

            return encryptor.update(payload) + encryptor.finalize()

        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return b""

    def send_admin_message(
        self,
        target_node_id: int,
        admin_message: admin_pb2.AdminMessage,
        from_node_id: int,
        channel_index: int = 0,
        want_response: bool = True,
    ) -> int | None:
        """
        Send an admin message to a remote node.

        Args:
            target_node_id: The target node ID
            admin_message: The AdminMessage protobuf to send
            from_node_id: The sender node ID (our gateway node)
            channel_index: The channel index to use (default 0)
            want_response: Whether to request a response

        Returns:
            Packet ID if sent successfully, None otherwise
        """
        if not self.connect():
            logger.error("Cannot send admin message: not connected to MQTT")
            return None

        try:
            # Generate a unique packet ID
            packet_id = random.getrandbits(32)

            # Create the Data payload
            data = mesh_pb2.Data()
            data.portnum = portnums_pb2.PortNum.ADMIN_APP
            data.payload = admin_message.SerializeToString()
            data.want_response = want_response

            # Create the MeshPacket
            mesh_packet = mesh_pb2.MeshPacket()
            setattr(mesh_packet, "from", from_node_id)
            mesh_packet.to = target_node_id
            mesh_packet.id = packet_id
            mesh_packet.channel = channel_index
            mesh_packet.want_ack = True
            mesh_packet.hop_limit = 3

            # Get encryption key
            keys = self._config.get_decryption_keys()
            if keys:
                key = self._derive_key("", keys[0])  # Use primary channel key
                encrypted = self._encrypt_payload(
                    data.SerializeToString(),
                    packet_id,
                    from_node_id,
                    key,
                )
                mesh_packet.encrypted = encrypted
            else:
                # No encryption - send decoded (not recommended for admin)
                mesh_packet.decoded.CopyFrom(data)
                logger.warning("Sending admin message without encryption!")

            # Create the ServiceEnvelope
            envelope = mqtt_pb2.ServiceEnvelope()
            envelope.packet.CopyFrom(mesh_packet)
            envelope.channel_id = "LongFast"  # Default channel name
            envelope.gateway_id = f"!{from_node_id:08x}"

            # Publish to MQTT
            topic = f"{self._config.mqtt_topic_prefix}/2/e/LongFast/!{from_node_id:08x}"

            if self._client is None:
                logger.error("MQTT client is None, cannot publish")
                return None

            result = self._client.publish(topic, envelope.SerializeToString())

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(
                    f"Sent admin message to node {target_node_id}, packet_id={packet_id}"
                )
                return packet_id
            else:
                logger.error(f"Failed to publish admin message: {result.rc}")
                return None

        except Exception as e:
            logger.error(f"Error sending admin message: {e}")
            return None

    def send_get_device_metadata(
        self,
        target_node_id: int,
        from_node_id: int,
    ) -> int | None:
        """
        Send a request for device metadata to test if node is administrable.

        Args:
            target_node_id: The target node ID
            from_node_id: The sender node ID

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.get_device_metadata_request = True

        return self.send_admin_message(target_node_id, admin_msg, from_node_id)

    def send_get_config(
        self,
        target_node_id: int,
        from_node_id: int,
        config_type: int,
    ) -> int | None:
        """
        Send a request for configuration.

        Args:
            target_node_id: The target node ID
            from_node_id: The sender node ID
            config_type: The AdminMessage config type enum value

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        # Use the ConfigType enum - config_type is an int that maps to the enum values
        admin_msg.get_config_request = config_type  # type: ignore[assignment]

        return self.send_admin_message(target_node_id, admin_msg, from_node_id)

    def send_get_module_config(
        self,
        target_node_id: int,
        from_node_id: int,
        module_config_type: int,
    ) -> int | None:
        """
        Send a request for module configuration.

        Args:
            target_node_id: The target node ID
            from_node_id: The sender node ID
            module_config_type: The AdminMessage module config type enum value

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.get_module_config_request = module_config_type  # type: ignore[assignment]

        return self.send_admin_message(target_node_id, admin_msg, from_node_id)

    def send_get_channel(
        self,
        target_node_id: int,
        from_node_id: int,
        channel_index: int,
    ) -> int | None:
        """
        Send a request for channel configuration.

        Args:
            target_node_id: The target node ID
            from_node_id: The sender node ID
            channel_index: The channel index to retrieve

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.get_channel_request = channel_index + 1  # 1-indexed in protocol

        return self.send_admin_message(target_node_id, admin_msg, from_node_id)

    def send_reboot(
        self,
        target_node_id: int,
        from_node_id: int,
        delay_seconds: int = 5,
    ) -> int | None:
        """
        Send a reboot command to a node.

        Args:
            target_node_id: The target node ID
            from_node_id: The sender node ID
            delay_seconds: Seconds to wait before rebooting

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.reboot_seconds = delay_seconds

        return self.send_admin_message(
            target_node_id,
            admin_msg,
            from_node_id,
            want_response=False,
        )

    def send_shutdown(
        self,
        target_node_id: int,
        from_node_id: int,
        delay_seconds: int = 5,
    ) -> int | None:
        """
        Send a shutdown command to a node.

        Args:
            target_node_id: The target node ID
            from_node_id: The sender node ID
            delay_seconds: Seconds to wait before shutdown

        Returns:
            Packet ID if sent successfully
        """
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.shutdown_seconds = delay_seconds

        return self.send_admin_message(
            target_node_id,
            admin_msg,
            from_node_id,
            want_response=False,
        )


# Global instance getter
def get_mqtt_publisher() -> MQTTPublisher:
    """Get the singleton MQTTPublisher instance."""
    return MQTTPublisher()
