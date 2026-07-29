"""MeshCore hashtag channel key derivation and GRP_TXT decryption."""

from __future__ import annotations

import hashlib
import hmac
import logging
import struct
from dataclasses import dataclass

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

logger = logging.getLogger(__name__)

PAYLOAD_GRP_TXT = 0x05


@dataclass(frozen=True, slots=True)
class ChannelPlaintext:
    timestamp: int
    sender: str
    message: str
    channel_name: str


def derive_channel_key(channel_name: str) -> bytes:
    """SHA-256(channelName)[:16] — same as CoreScope internal/channel."""
    return hashlib.sha256(channel_name.encode("utf-8")).digest()[:16]


def channel_hash(key: bytes) -> int:
    """First byte of SHA-256(key)."""
    return hashlib.sha256(key).digest()[0]


def decrypt_grp_txt(key: bytes, mac: bytes, ciphertext: bytes) -> bytes | None:
    """Verify 2-byte HMAC-SHA256 MAC and AES-128-ECB decrypt."""
    if len(key) != 16 or len(mac) != 2 or not ciphertext or len(ciphertext) % 16:
        return None

    channel_secret = key + (b"\x00" * 16)
    calculated = hmac.new(channel_secret, ciphertext, hashlib.sha256).digest()
    if calculated[0] != mac[0] or calculated[1] != mac[1]:
        return None

    # AES-ECB block-by-block (cryptography has no ECB mode helper for multi-block
    # in older APIs; use Cipher with ECB).
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
    decryptor = cipher.decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()


def parse_plaintext(plaintext: bytes) -> tuple[int, str, str] | None:
    """Parse timestamp(4 LE) + flags(1) + 'sender: message\\0...'."""
    if len(plaintext) < 5:
        return None
    timestamp = struct.unpack_from("<I", plaintext, 0)[0]
    text = plaintext[5:].split(b"\x00", 1)[0].decode("utf-8", errors="replace")
    if not text.strip():
        return None

    colon = text.find(": ")
    if 0 < colon < 50:
        sender = text[:colon]
        if ":" not in sender and "[" not in sender and "]" not in sender:
            return timestamp, sender, text[colon + 2 :]
    return timestamp, "", text


def extract_grp_txt_payload(raw: bytes) -> tuple[int, bytes] | None:
    """
    Extract GRP_TXT payload bytes from a MeshCore raw packet.

    Packet layout: header(1) [transport(4)] pathLen(1) path... payload...
    """
    if len(raw) < 2:
        return None
    header = raw[0]
    route_type = header & 0x03
    payload_type = (header >> 2) & 0x0F
    if payload_type != PAYLOAD_GRP_TXT:
        return None

    offset = 1
    if route_type in (0, 3):  # TRANSPORT_FLOOD / TRANSPORT_DIRECT
        offset += 4
    if offset >= len(raw):
        return None

    path_byte = raw[offset]
    offset += 1
    hash_size = ((path_byte >> 6) & 0x03) + 1
    hash_count = path_byte & 0x3F
    path_bytes = hash_size * hash_count
    if hash_size == 4 or path_bytes > 64:
        return None
    offset += path_bytes
    if offset > len(raw):
        return None
    return payload_type, raw[offset:]


def try_decrypt_channel_message(
    raw_hex: str, channel_name: str = "#meshtastic"
) -> ChannelPlaintext | None:
    """Decrypt a MeshCore raw hex packet if it is GRP_TXT for channel_name."""
    try:
        raw = bytes.fromhex(raw_hex.replace(" ", "").replace("\n", ""))
    except ValueError:
        return None

    extracted = extract_grp_txt_payload(raw)
    if extracted is None:
        return None
    _, payload = extracted
    if len(payload) < 3:
        return None

    key = derive_channel_key(channel_name)
    expected_hash = channel_hash(key)
    if payload[0] != expected_hash:
        # Still try decrypt — hash mismatch usually means wrong channel
        logger.debug(
            "Channel hash mismatch: got %02X want %02X for %s",
            payload[0],
            expected_hash,
            channel_name,
        )
        return None

    mac = payload[1:3]
    ciphertext = payload[3:]
    plain = decrypt_grp_txt(key, mac, ciphertext)
    if plain is None:
        return None
    parsed = parse_plaintext(plain)
    if parsed is None:
        return None
    timestamp, sender, message = parsed
    return ChannelPlaintext(
        timestamp=timestamp,
        sender=sender,
        message=message,
        channel_name=channel_name,
    )


def content_hash(raw_hex: str) -> str:
    """Path-independent content hash (first 16 hex chars), CoreScope-compatible."""
    try:
        buf = bytes.fromhex(raw_hex.replace(" ", "").replace("\n", ""))
    except ValueError:
        return hashlib.sha256(raw_hex.encode()).hexdigest()[:16]
    if len(buf) < 2:
        return hashlib.sha256(buf).hexdigest()[:16]

    header = buf[0]
    offset = 1
    if (header & 0x03) in (0, 3):
        offset += 4
    if offset >= len(buf):
        return hashlib.sha256(buf).hexdigest()[:16]
    path_byte = buf[offset]
    offset += 1
    hash_size = ((path_byte >> 6) & 0x03) + 1
    hash_count = path_byte & 0x3F
    payload_start = offset + hash_size * hash_count
    if payload_start > len(buf):
        return hashlib.sha256(buf).hexdigest()[:16]
    payload = buf[payload_start:]
    payload_type = (header >> 2) & 0x0F
    to_hash = bytes([payload_type]) + payload
    return hashlib.sha256(to_hash).hexdigest()[:16]
