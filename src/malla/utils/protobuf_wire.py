"""Minimal protobuf wire helpers for custom firmware fields.

Stock ``meshtastic`` Python bindings lag custom firmware ModuleConfig fields.
Unknown fields round-trip on serialize with the upb backend, but are not
readable via ``UnknownFields()``. These helpers encode/decode uint32 fields
by field number so admin GET/SET can support firmware-only keys.
"""

from __future__ import annotations

from typing import Any


def encode_varint(value: int) -> bytes:
    """Encode an unsigned protobuf varint."""
    if value < 0:
        raise ValueError("varint must be non-negative")
    out = bytearray()
    n = int(value)
    while True:
        bits = n & 0x7F
        n >>= 7
        out.append(bits | (0x80 if n else 0))
        if not n:
            return bytes(out)


def decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Decode a varint; return ``(value, new_offset)``."""
    result = 0
    shift = 0
    pos = offset
    while pos < len(data):
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")
    raise ValueError("truncated varint")


def encode_uint32_field(field_number: int, value: int) -> bytes:
    """Encode a protobuf uint32 field (wire type 0)."""
    tag = (int(field_number) << 3) | 0
    return encode_varint(tag) + encode_varint(int(value) & 0xFFFFFFFF)


def strip_field(data: bytes, field_number: int) -> bytes:
    """Return ``data`` with all occurrences of ``field_number`` removed."""
    out = bytearray()
    pos = 0
    target = int(field_number)
    while pos < len(data):
        tag, next_pos = decode_varint(data, pos)
        field_no = tag >> 3
        wire_type = tag & 0x07
        start = pos
        pos = next_pos
        if wire_type == 0:  # varint
            _, pos = decode_varint(data, pos)
        elif wire_type == 1:  # 64-bit
            pos += 8
        elif wire_type == 2:  # length-delimited
            length, pos = decode_varint(data, pos)
            pos += length
        elif wire_type == 5:  # 32-bit
            pos += 4
        else:
            # Unsupported / group wire types — stop rather than corrupt.
            out.extend(data[start:])
            break
        if field_no != target:
            out.extend(data[start:pos])
    return bytes(out)


def read_uint32_field(data: bytes, field_number: int) -> int | None:
    """Return the last uint32/varint value for ``field_number``, if present."""
    pos = 0
    target = int(field_number)
    found: int | None = None
    while pos < len(data):
        tag, next_pos = decode_varint(data, pos)
        field_no = tag >> 3
        wire_type = tag & 0x07
        pos = next_pos
        if wire_type == 0:
            value, pos = decode_varint(data, pos)
            if field_no == target:
                found = int(value)
        elif wire_type == 1:
            pos += 8
        elif wire_type == 2:
            length, pos = decode_varint(data, pos)
            pos += length
        elif wire_type == 5:
            pos += 4
        else:
            break
    return found


def get_message_uint32(message: Any, field_number: int, attr_name: str | None = None) -> int | None:
    """Read a uint32 from a known attr or from serialized unknown field bytes."""
    if attr_name and hasattr(message, attr_name):
        try:
            return int(getattr(message, attr_name))
        except (TypeError, ValueError):
            pass
    try:
        raw = message.SerializeToString()
    except Exception:
        return None
    return read_uint32_field(raw, field_number)


def set_message_uint32_field(message: Any, field_number: int, value: int, attr_name: str | None = None) -> None:
    """
    Set a uint32 on ``message``.

    Uses the generated attribute when present; otherwise injects the field into
    the serialized form so it is preserved as an unknown field on send.
    """
    if attr_name and hasattr(message, attr_name):
        setattr(message, attr_name, int(value))
        return
    raw = strip_field(message.SerializeToString(), field_number)
    raw += encode_uint32_field(field_number, int(value))
    message.Clear()
    message.ParseFromString(raw)
