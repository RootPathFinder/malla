"""
Geographic utility functions for Meshtastic Mesh Health Web UI
"""

import math


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees) using the Haversine formula.

    Args:
        lat1: Latitude of first point in decimal degrees
        lon1: Longitude of first point in decimal degrees
        lat2: Latitude of second point in decimal degrees
        lon2: Longitude of second point in decimal degrees

    Returns:
        Distance in kilometers
    """
    # Earth's radius in km
    R = 6371.0

    # Convert decimal degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance


def calculate_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the initial bearing from point 1 to point 2.

    Args:
        lat1: Latitude of first point in decimal degrees
        lon1: Longitude of first point in decimal degrees
        lat2: Latitude of second point in decimal degrees
        lon2: Longitude of second point in decimal degrees

    Returns:
        Bearing in degrees (0-360)
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon_rad = math.radians(lon2 - lon1)

    y = math.sin(dlon_rad) * math.cos(lat2_rad)
    x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(
        lat2_rad
    ) * math.cos(dlon_rad)

    bearing_rad = math.atan2(y, x)
    bearing_deg = math.degrees(bearing_rad)

    # Normalize to 0-360 degrees
    return (bearing_deg + 360) % 360


def format_distance_for_unit(
    distance_km: float | None,
    unit: str = "C",
    *,
    decimals: int = 1,
) -> str:
    """Format a kilometer distance using C→metric / F→imperial preference.

    The web UI temperature toggle stores ``C`` or ``F``. Metric (C) uses
    kilometers/meters; imperial (F) uses miles/feet.
    """
    if distance_km is None:
        return "N/A"
    try:
        km = float(distance_km)
    except (TypeError, ValueError):
        return "N/A"
    if math.isnan(km) or math.isinf(km) or km < 0:
        return "N/A"

    preferred = (unit or "C").upper()
    if preferred == "F":
        miles = km * 0.621371192237334
        if miles < 1:
            feet = int(round(km * 1000 * 3.280839895))
            return f"{feet} ft"
        return f"{miles:.{decimals}f} mi"

    if km < 1:
        return f"{int(round(km * 1000))} m"
    return f"{km:.{decimals}f} km"
