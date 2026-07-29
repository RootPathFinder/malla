import base64

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from meshbridge.meshtastic_crypto import decrypt_payload, derive_key, try_decrypt_keys


def test_derive_key_with_channel_name():
    key_b64 = base64.b64encode(b"\x01" * 32).decode()
    k1 = derive_key("MeshCore", key_b64)
    k2 = derive_key("MeshCore", key_b64)
    k3 = derive_key("", key_b64)
    assert k1 == k2
    assert len(k1) == 32
    assert k1 != k3


def test_decrypt_roundtrip():
    key = b"\x42" * 32
    packet_id = 0x12345678
    sender_id = 0xAABBCCDD
    plain = b"hello from meshtastic"
    nonce = packet_id.to_bytes(8, "little") + sender_id.to_bytes(8, "little")
    cipher = Cipher(algorithms.AES(key), modes.CTR(nonce), backend=default_backend())
    enc = cipher.encryptor()
    encrypted = enc.update(plain) + enc.finalize()

    assert decrypt_payload(encrypted, packet_id, sender_id, key) == plain


def test_try_decrypt_keys_raw_psk():
    key_bytes = b"\x11" * 32
    key_b64 = base64.b64encode(key_bytes).decode()
    packet_id = 99
    sender_id = 100
    plain = b"payload-bytes-here"
    nonce = packet_id.to_bytes(8, "little") + sender_id.to_bytes(8, "little")
    cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
    enc = cipher.encryptor()
    encrypted = enc.update(plain) + enc.finalize()

    out = try_decrypt_keys(encrypted, packet_id, sender_id, key_b64, channel_name="")
    assert out == plain
