"""Meshtastic AES-256-CTR decryption helpers (mirrors malla capture)."""

from __future__ import annotations

import base64
import hashlib
import logging

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

logger = logging.getLogger(__name__)


def derive_key(channel_name: str, key_base64: str) -> bytes:
    """Derive AES key from base64 PSK and optional channel name."""
    key_bytes = base64.b64decode(key_base64)
    if channel_name:
        hasher = hashlib.sha256()
        hasher.update(key_bytes)
        hasher.update(channel_name.encode("utf-8"))
        return hasher.digest()
    if len(key_bytes) == 32:
        return key_bytes
    # Pad short keys to 32 bytes the same way Meshtastic often expects
    return key_bytes.ljust(32, b"\x00")[:32]


def decrypt_payload(
    encrypted_payload: bytes, packet_id: int, sender_id: int, key: bytes
) -> bytes:
    """Decrypt Meshtastic packet payload with AES-256-CTR."""
    if not encrypted_payload:
        return b""
    nonce = packet_id.to_bytes(8, "little") + sender_id.to_bytes(8, "little")
    cipher = Cipher(algorithms.AES(key), modes.CTR(nonce), backend=default_backend())
    decryptor = cipher.decryptor()
    return decryptor.update(encrypted_payload) + decryptor.finalize()


def try_decrypt_keys(
    encrypted_payload: bytes,
    packet_id: int,
    sender_id: int,
    key_base64: str,
    channel_name: str,
) -> bytes | None:
    """
    Try decrypting with key as-is and with channel-name derivation.

    Returns decrypted bytes on first successful Data protobuf parse attempt
    is left to the caller; here we return raw decrypted bytes if non-empty.
    """
    candidates: list[bytes] = []
    # Prefer channel-name derivation for named secondary channels, then raw key.
    if channel_name:
        candidates.append(derive_key(channel_name, key_base64))
    candidates.append(derive_key("", key_base64))

    seen: set[bytes] = set()
    for key in candidates:
        if key in seen:
            continue
        seen.add(key)
        try:
            plain = decrypt_payload(encrypted_payload, packet_id, sender_id, key)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Decrypt attempt failed: %s", exc)
            continue
        if plain:
            return plain
    return None
