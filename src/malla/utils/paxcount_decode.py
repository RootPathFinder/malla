"""Decode Meshtastic PAXCOUNTER_APP payloads, including optional MAC/BSSID sightings.

Firmware with ``ModuleConfig.paxcounter.report_ids`` may attach ``PaxSighting``
entries (WiFi client MACs and AP BSSIDs). Stock ``meshtastic`` Python bindings
do not yet know these fields, so we decode with a vendored protobuf.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from malla.vendor.meshtastic import paxcount_pb2

KIND_WIFI_CLIENT = "wifi_client"
KIND_WIFI_AP = "wifi_ap"

_KIND_LABELS = {
    paxcount_pb2.PaxSighting.WIFI_CLIENT: KIND_WIFI_CLIENT,
    paxcount_pb2.PaxSighting.WIFI_AP: KIND_WIFI_AP,
}


def format_mac(mac: bytes | bytearray | memoryview | str | None) -> str | None:
    """Format a 6-byte MAC/BSSID as ``aa:bb:cc:dd:ee:ff`` (lowercase)."""
    if mac is None:
        return None
    if isinstance(mac, str):
        cleaned = mac.replace(":", "").replace("-", "").strip()
        if len(cleaned) != 12:
            return mac.lower() if mac else None
        try:
            raw = bytes.fromhex(cleaned)
        except ValueError:
            return mac.lower()
    else:
        raw = bytes(mac)
    if len(raw) != 6:
        return raw.hex()
    return ":".join(f"{b:02x}" for b in raw)


def _kind_name(kind: int) -> str:
    return _KIND_LABELS.get(int(kind), f"unknown_{int(kind)}")


def decode_paxcount_payload(raw_payload: bytes | bytearray | memoryview | None) -> dict[str, Any] | None:
    """
    Decode a PAXCOUNTER_APP payload into a JSON-friendly dict.

    Returns ``None`` when the payload is empty or unparseable.
    """
    if not raw_payload:
        return None
    try:
        payload = bytes(raw_payload)
        msg = paxcount_pb2.Paxcount()
        msg.ParseFromString(payload)
    except Exception:
        return None

    sightings: list[dict[str, Any]] = []
    for s in msg.sightings:
        mac = format_mac(s.mac)
        if not mac:
            continue
        sightings.append(
            {
                "mac": mac,
                "kind": _kind_name(s.kind),
                "kind_value": int(s.kind),
                "rssi": int(s.rssi),
            }
        )

    return {
        "wifi": int(msg.wifi or 0),
        "ble": int(msg.ble or 0),
        "uptime": int(msg.uptime or 0),
        "sightings": sightings,
        "sighting_count": int(msg.sighting_count or 0),
        "chunk_index": int(msg.chunk_index or 0),
        "chunk_total": int(msg.chunk_total or 0),
    }


def aggregate_mac_hits(
    readings: list[dict[str, Any]] | None,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """
    Aggregate unique MAC/BSSID hit counts across decoded readings.

    Each reading may include a ``sightings`` list (from ``decode_paxcount_payload``).
    Returns rows sorted by hit count descending, then MAC ascending.
    """
    if not readings:
        return []

    counts: Counter[tuple[str, str]] = Counter()
    best_rssi: dict[tuple[str, str], int] = {}
    last_seen: dict[tuple[str, str], float] = {}
    nodes: dict[tuple[str, str], set[str]] = {}

    for reading in readings:
        sightings = reading.get("sightings") or []
        if not isinstance(sightings, list):
            continue
        ts = reading.get("timestamp")
        node_hex = reading.get("from_node_hex") or reading.get("node_hex")
        for s in sightings:
            if not isinstance(s, dict):
                continue
            mac = format_mac(s.get("mac"))
            if not mac:
                continue
            kind = str(s.get("kind") or KIND_WIFI_CLIENT)
            key = (mac, kind)
            counts[key] += 1
            rssi = s.get("rssi")
            if rssi is not None:
                try:
                    rssi_i = int(rssi)
                except (TypeError, ValueError):
                    rssi_i = None
                if rssi_i is not None:
                    prev = best_rssi.get(key)
                    if prev is None or rssi_i > prev:
                        best_rssi[key] = rssi_i
            if ts is not None:
                try:
                    ts_f = float(ts)
                except (TypeError, ValueError):
                    ts_f = None
                if ts_f is not None:
                    prev_ts = last_seen.get(key)
                    if prev_ts is None or ts_f > prev_ts:
                        last_seen[key] = ts_f
            if node_hex:
                nodes.setdefault(key, set()).add(str(node_hex))

    rows: list[dict[str, Any]] = []
    for (mac, kind), hits in counts.items():
        rows.append(
            {
                "mac": mac,
                "kind": kind,
                "hits": hits,
                "best_rssi": best_rssi.get((mac, kind)),
                "last_seen": last_seen.get((mac, kind)),
                "node_count": len(nodes.get((mac, kind), ())),
            }
        )

    rows.sort(key=lambda r: (-int(r["hits"]), r["mac"], r["kind"]))
    if limit and limit > 0:
        return rows[: int(limit)]
    return rows
