"""Type stubs for vendored Paxcount protobuf (with PaxSighting)."""

from typing import Any, Iterable

class PaxSighting:
    class Kind:
        WIFI_CLIENT: int
        WIFI_AP: int
        BLE: int

    WIFI_CLIENT: int
    WIFI_AP: int
    BLE: int

    mac: bytes
    kind: int
    rssi: int

    def __init__(
        self,
        *,
        mac: bytes = ...,
        kind: int = ...,
        rssi: int = ...,
    ) -> None: ...
    def ClearField(self, field_name: str) -> None: ...

class Paxcount:
    wifi: int
    ble: int
    uptime: int
    sightings: Any
    sighting_count: int
    chunk_index: int
    chunk_total: int

    def __init__(
        self,
        *,
        wifi: int = ...,
        ble: int = ...,
        uptime: int = ...,
        sightings: Iterable[PaxSighting] | None = ...,
        sighting_count: int = ...,
        chunk_index: int = ...,
        chunk_total: int = ...,
    ) -> None: ...
    def ParseFromString(self, data: bytes) -> int: ...
    def SerializeToString(self, *, deterministic: bool = ...) -> bytes: ...
    def ClearField(self, field_name: str) -> None: ...
