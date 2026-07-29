import hashlib
import hmac
import struct

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from meshbridge.meshcore_crypto import (
    PAYLOAD_GRP_TXT,
    channel_hash,
    content_hash,
    derive_channel_key,
    parse_plaintext,
    try_decrypt_channel_message,
)


def _aes_ecb_encrypt(key: bytes, plaintext: bytes) -> bytes:
    # PKCS-less: pad with zeros to 16
    if len(plaintext) % 16:
        plaintext = plaintext + b"\x00" * (16 - (len(plaintext) % 16))
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
    enc = cipher.encryptor()
    return enc.update(plaintext) + enc.finalize()


def _build_grp_txt_packet(channel_name: str, sender: str, message: str) -> str:
    key = derive_channel_key(channel_name)
    ch = channel_hash(key)
    timestamp = 1_700_000_000
    plain = struct.pack("<I", timestamp) + bytes([0]) + f"{sender}: {message}".encode()
    ciphertext = _aes_ecb_encrypt(key, plain)
    mac = hmac.new(key + b"\x00" * 16, ciphertext, hashlib.sha256).digest()[:2]

    # header: route FLOOD=1, payload GRP_TXT=5, version 0 → (5<<2)|1 = 0x15
    header = bytes([(PAYLOAD_GRP_TXT << 2) | 0x01])
    path_byte = bytes([0x00])  # zero-hop
    payload = bytes([ch]) + mac + ciphertext
    return (header + path_byte + payload).hex()


def test_derive_key_and_hash_match_corescope():
    key = derive_channel_key("#meshtastic")
    assert len(key) == 16
    assert key == hashlib.sha256(b"#meshtastic").digest()[:16]
    assert channel_hash(key) == hashlib.sha256(key).digest()[0]


def test_parse_plaintext_sender_message():
    plain = struct.pack("<I", 123) + b"\x00" + b"NodeA: hello world\x00extra"
    ts, sender, msg = parse_plaintext(plain)
    assert ts == 123
    assert sender == "NodeA"
    assert msg == "hello world"


def test_roundtrip_decrypt_channel_message():
    raw_hex = _build_grp_txt_packet("#meshtastic", "Relay1", "bridge test")
    result = try_decrypt_channel_message(raw_hex, "#meshtastic")
    assert result is not None
    assert result.sender == "Relay1"
    assert result.message == "bridge test"


def test_wrong_channel_rejected():
    raw_hex = _build_grp_txt_packet("#meshtastic", "Relay1", "bridge test")
    assert try_decrypt_channel_message(raw_hex, "#other") is None


def test_content_hash_stable_across_path():
    # Same payload type + payload should hash equal; here just ensure length/format
    raw_hex = _build_grp_txt_packet("#meshtastic", "A", "B")
    h = content_hash(raw_hex)
    assert len(h) == 16
    assert h == content_hash(raw_hex)
