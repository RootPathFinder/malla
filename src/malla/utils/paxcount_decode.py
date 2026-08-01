"""Decode Meshtastic PAXCOUNTER_APP payloads, including optional MAC/BSSID sightings.

Firmware with ``ModuleConfig.paxcounter.report_ids`` may attach ``PaxSighting``
entries (WiFi client MACs, AP BSSIDs, and BLE addresses, including Apple/Android
classification and soft fingerprints for rotating BLE random addresses).
Stock ``meshtastic`` Python bindings do not yet know these fields, so we decode
with a vendored protobuf.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from malla.vendor.meshtastic import paxcount_pb2

KIND_WIFI_CLIENT = "wifi_client"
KIND_WIFI_AP = "wifi_ap"
KIND_BLE = "ble"
KIND_BLE_APPLE = "ble_apple"
KIND_BLE_ANDROID = "ble_android"

_KIND_LABELS = {
    paxcount_pb2.PaxSighting.WIFI_CLIENT: KIND_WIFI_CLIENT,
    paxcount_pb2.PaxSighting.WIFI_AP: KIND_WIFI_AP,
    paxcount_pb2.PaxSighting.BLE: KIND_BLE,
    paxcount_pb2.PaxSighting.BLE_APPLE: KIND_BLE_APPLE,
    paxcount_pb2.PaxSighting.BLE_ANDROID: KIND_BLE_ANDROID,
}


def format_mac(mac: bytes | bytearray | memoryview | str | None) -> str | None:
    """Format a 6-byte MAC/BSSID as ``aa:bb:cc:dd:ee:ff`` (lowercase)."""
    if mac is None:
        return None
    if isinstance(mac, str):
        cleaned = mac.replace(":", "").replace("-", "").strip()
        if len(cleaned) != 12:
            return None
        try:
            raw = bytes.fromhex(cleaned)
        except ValueError:
            return None
    else:
        raw = bytes(mac)
    if len(raw) != 6:
        return None
    return ":".join(f"{b:02x}" for b in raw)


def format_fingerprint(
    fingerprint: bytes | bytearray | memoryview | str | None,
) -> str | None:
    """Format a soft BLE fingerprint as lowercase hex (1–4 bytes → 2–8 chars)."""
    if fingerprint is None or fingerprint == "":
        return None
    if isinstance(fingerprint, str):
        cleaned = fingerprint.strip().lower()
        if cleaned.startswith("fp:"):
            cleaned = cleaned[3:]
        cleaned = cleaned.replace(":", "").replace("-", "")
        if not cleaned or len(cleaned) > 8 or len(cleaned) % 2:
            return None
        try:
            raw = bytes.fromhex(cleaned)
        except ValueError:
            return None
    else:
        raw = bytes(fingerprint)
    if not raw or len(raw) > 4:
        return None
    return raw.hex()


def stable_sighting_id(
    mac: str | None = None,
    fingerprint: str | None = None,
) -> str | None:
    """
    Return a stable profile/directory id for a sighting.

    Prefer ``fp:<hex>`` when a soft BLE fingerprint is present (survives MAC
    randomization); otherwise the normalized MAC.
    """
    fp = format_fingerprint(fingerprint)
    if fp:
        return f"fp:{fp}"
    return format_mac(mac)


def normalize_profile_id(value: str | None) -> str | None:
    """Normalize a profile key (MAC or ``fp:<hex>`` fingerprint id)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lower = text.lower()
    if lower.startswith("fp:"):
        fp = format_fingerprint(lower)
        return f"fp:{fp}" if fp else None
    # Bare short hex without separators → fingerprint id (not a 6-byte MAC).
    if ":" not in lower and "-" not in lower:
        cleaned = lower
        if cleaned and len(cleaned) <= 8 and len(cleaned) % 2 == 0:
            fp = format_fingerprint(cleaned)
            if fp:
                return f"fp:{fp}"
    return format_mac(text)


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
        fingerprint = format_fingerprint(s.fingerprint) if s.fingerprint else None
        sightings.append(
            {
                "mac": mac,
                "kind": _kind_name(s.kind),
                "kind_value": int(s.kind),
                "rssi": int(s.rssi),
                "fingerprint": fingerprint,
                "stable_id": stable_sighting_id(mac, fingerprint),
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
    first_seen: dict[tuple[str, str], float] = {}
    last_seen: dict[tuple[str, str], float] = {}
    nodes: dict[tuple[str, str], set[str]] = {}
    fingerprints: dict[tuple[str, str], str] = {}
    stable_ids: dict[tuple[str, str], str] = {}

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
            fp = format_fingerprint(s.get("fingerprint"))
            if fp:
                fingerprints[key] = fp
            sid = s.get("stable_id") or stable_sighting_id(mac, fp)
            if sid:
                stable_ids[key] = sid
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
                    prev_first = first_seen.get(key)
                    if prev_first is None or ts_f < prev_first:
                        first_seen[key] = ts_f
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
                "first_seen": first_seen.get((mac, kind)),
                "last_seen": last_seen.get((mac, kind)),
                "node_count": len(nodes.get((mac, kind), ())),
                "fingerprint": fingerprints.get((mac, kind)),
                "stable_id": stable_ids.get((mac, kind), mac),
            }
        )

    rows.sort(key=lambda r: (-int(r["hits"]), r["mac"], r["kind"]))
    if limit and limit > 0:
        return rows[: int(limit)]
    return rows


def build_id_directory(
    mac_hits: list[dict[str, Any]] | None,
    profiles: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """
    Collapse per-kind mac_hits into per-stable-ID rows with kind breakdown + profile.

    When a BLE fingerprint is present, rotating MACs that share it are grouped
    under ``fp:<hex>`` so nicknames survive address randomization.
    """
    profiles = profiles or {}
    by_id: dict[str, dict[str, Any]] = {}

    for hit in mac_hits or []:
        if not isinstance(hit, dict):
            continue
        mac = format_mac(hit.get("mac"))
        if not mac:
            continue
        kind = str(hit.get("kind") or KIND_WIFI_CLIENT)
        hits = int(hit.get("hits") or 0)
        best_rssi = hit.get("best_rssi")
        first = hit.get("first_seen")
        last = hit.get("last_seen")
        node_count = int(hit.get("node_count") or 0)
        fingerprint = format_fingerprint(hit.get("fingerprint"))
        stable_id = hit.get("stable_id") or stable_sighting_id(mac, fingerprint)
        if not stable_id:
            continue

        entry = by_id.get(stable_id)
        if entry is None:
            entry = {
                "id": stable_id,
                "mac": mac if not stable_id.startswith("fp:") else None,
                "macs": [],
                "fingerprint": fingerprint,
                "hits": 0,
                "kinds": [],
                "kind_hits": {},
                "best_rssi": None,
                "first_seen": None,
                "last_seen": None,
                "node_count": 0,
                "nickname": None,
                "notes": None,
                "has_profile": False,
            }
            by_id[stable_id] = entry

        entry["hits"] += hits
        entry["kind_hits"][kind] = entry["kind_hits"].get(kind, 0) + hits
        if kind not in entry["kinds"]:
            entry["kinds"].append(kind)
        if mac not in entry["macs"]:
            entry["macs"].append(mac)
        # Prefer a concrete MAC display when not fingerprint-keyed
        if not stable_id.startswith("fp:"):
            entry["mac"] = mac
        elif entry["mac"] is None:
            entry["mac"] = mac
        if fingerprint and not entry.get("fingerprint"):
            entry["fingerprint"] = fingerprint
        if best_rssi is not None:
            try:
                rssi_i = int(best_rssi)
            except (TypeError, ValueError):
                rssi_i = None
            if rssi_i is not None and (
                entry["best_rssi"] is None or rssi_i > entry["best_rssi"]
            ):
                entry["best_rssi"] = rssi_i
        if first is not None:
            try:
                first_f = float(first)
            except (TypeError, ValueError):
                first_f = None
            if first_f is not None and (
                entry["first_seen"] is None or first_f < entry["first_seen"]
            ):
                entry["first_seen"] = first_f
        if last is not None:
            try:
                last_f = float(last)
            except (TypeError, ValueError):
                last_f = None
            if last_f is not None and (
                entry["last_seen"] is None or last_f > entry["last_seen"]
            ):
                entry["last_seen"] = last_f
        entry["node_count"] = max(entry["node_count"], node_count)

    for key, profile in profiles.items():
        stable_id = normalize_profile_id(key) or profile.get("mac") or key
        stable_id = normalize_profile_id(stable_id) or str(stable_id)
        entry = by_id.get(stable_id)
        if entry is None:
            entry = {
                "id": stable_id,
                "mac": format_mac(stable_id) if not str(stable_id).startswith("fp:") else None,
                "macs": [],
                "fingerprint": format_fingerprint(stable_id),
                "hits": 0,
                "kinds": [],
                "kind_hits": {},
                "best_rssi": None,
                "first_seen": None,
                "last_seen": None,
                "node_count": 0,
                "nickname": None,
                "notes": None,
                "has_profile": False,
            }
            by_id[stable_id] = entry
        entry["nickname"] = profile.get("nickname")
        entry["notes"] = profile.get("notes")
        entry["has_profile"] = True
        entry["profile_updated_at"] = profile.get("updated_at")

    # Back-compat: expose mac field used by older UI as display id
    for entry in by_id.values():
        if not entry.get("mac"):
            entry["mac"] = entry["id"]

    rows = list(by_id.values())
    rows.sort(
        key=lambda r: (
            0 if r.get("nickname") else 1,
            str(r.get("nickname") or "").lower(),
            -int(r.get("hits") or 0),
            str(r.get("id") or r.get("mac") or ""),
        )
    )
    return rows
