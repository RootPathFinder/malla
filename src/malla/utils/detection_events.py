"""Join firmware trip + cleared detection packets into one logical event."""

from __future__ import annotations

from typing import Any


def _sensor_key(event: dict[str, Any]) -> tuple[Any, str]:
    name = (event.get("detection_name") or "").strip().lower()
    return (event.get("from_node_id"), name)


def _merge_trip_and_cleared(trip: dict[str, Any] | None, cleared: dict[str, Any]) -> dict[str, Any]:
    """Build a single completed event from an optional trip + cleared packet."""
    merged = dict(cleared)
    merged["event_kind"] = "complete"
    merged["trip_id"] = trip.get("id") if trip else None
    merged["clear_id"] = cleared.get("id")
    merged["started_at"] = trip.get("timestamp") if trip else None
    merged["started_at_iso"] = trip.get("timestamp_iso") if trip else None
    merged["ended_at"] = cleared.get("timestamp")
    # Prefer clear timing/duration; keep trip dwell if present.
    if trip and trip.get("dwell_ms") is not None and merged.get("dwell_ms") is None:
        merged["dwell_ms"] = trip.get("dwell_ms")
    # Stable display name from either side.
    if not merged.get("detection_name") and trip:
        merged["detection_name"] = trip.get("detection_name")
    return merged


def join_detection_events(
    events: list[dict[str, Any]],
    *,
    max_pair_secs: float = 120.0,
) -> list[dict[str, Any]]:
    """Coalesce trip+cleared pairs from the same node/sensor into one event.

    Firmware sends an immediate ``detected`` (trip) and a later ``cleared`` with
    duration. For the UI log we want one record when the burst ends.

    Args:
        events: Detection events, any order.
        max_pair_secs: Max age from trip→cleared to allow pairing.

    Returns:
        Newest-first list. Paired events use ``event_kind='complete'``.
        Unpaired trips (still in progress) stay ``trip``. State/unknown pass through.
    """
    if not events:
        return []

    chronological = sorted(
        events,
        key=lambda e: (float(e.get("timestamp") or 0), int(e.get("id") or 0)),
    )
    open_trips: dict[tuple[Any, str], dict[str, Any]] = {}
    out: list[dict[str, Any]] = []

    for ev in chronological:
        kind = ev.get("event_kind") or "unknown"
        key = _sensor_key(ev)

        if kind == "trip":
            prior = open_trips.get(key)
            if prior is not None:
                out.append(prior)
            open_trips[key] = ev
            continue

        if kind == "cleared":
            trip = open_trips.get(key)
            if trip is not None:
                gap = float(ev.get("timestamp") or 0) - float(trip.get("timestamp") or 0)
                if 0 <= gap <= max_pair_secs:
                    open_trips.pop(key, None)
                    out.append(_merge_trip_and_cleared(trip, ev))
                    continue
            # Orphan clear — still one finished record with durations.
            out.append(_merge_trip_and_cleared(None, ev))
            continue

        out.append(ev)

    out.extend(open_trips.values())
    out.sort(
        key=lambda e: (float(e.get("timestamp") or 0), int(e.get("id") or 0)),
        reverse=True,
    )
    return out
