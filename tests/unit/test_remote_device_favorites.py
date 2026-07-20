"""Unit tests for on-device favorites list management."""

from unittest.mock import MagicMock, patch

import pytest

from src.malla.database.admin_repository import AdminRepository, init_admin_tables
from src.malla.database.connection import get_db_connection
from src.malla.services.admin_service import AdminConnectionType, AdminService


@pytest.fixture
def admin_db(tmp_path, monkeypatch):
    db_path = tmp_path / "admin_favorites.db"
    cfg = MagicMock()
    cfg.database_file = str(db_path)
    monkeypatch.setattr("malla.database.connection.get_config", lambda: cfg)
    monkeypatch.setattr(
        "src.malla.database.connection.get_config", lambda: cfg
    )
    init_admin_tables()
    conn = get_db_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS node_info (
            node_id INTEGER PRIMARY KEY,
            hex_id TEXT,
            long_name TEXT,
            short_name TEXT,
            hw_model TEXT
        )
        """
    )
    conn.commit()
    conn.close()
    return db_path


class TestRemoteDeviceFavoritesRepository:
    @pytest.mark.unit
    def test_upsert_list_and_remove(self, admin_db):
        AdminRepository.upsert_remote_device_favorite(0x1111, 0xAAAA, source="managed")
        AdminRepository.upsert_remote_device_favorite(0x1111, 0xBBBB, source="device")

        rows = AdminRepository.list_remote_device_favorites(0x1111)
        ids = {int(r["node_id"]) for r in rows}
        assert ids == {0xAAAA, 0xBBBB}
        assert all(r["hex_id"].startswith("!") for r in rows)

        assert AdminRepository.remove_remote_device_favorite(0x1111, 0xAAAA) is True
        remaining = AdminRepository.list_remote_device_favorites(0x1111)
        assert {int(r["node_id"]) for r in remaining} == {0xBBBB}

    @pytest.mark.unit
    def test_replace_favorites(self, admin_db):
        AdminRepository.upsert_remote_device_favorite(0x2222, 0x1, source="managed")
        AdminRepository.replace_remote_device_favorites(
            0x2222, [0x2, 0x3], source="device"
        )
        rows = AdminRepository.list_remote_device_favorites(0x2222)
        assert {int(r["node_id"]) for r in rows} == {0x2, 0x3}


class TestGetDeviceFavorites:
    @pytest.fixture
    def service(self, admin_db):
        AdminService._instance = None
        service = AdminService()
        service._connection_type = AdminConnectionType.TCP
        return service

    @pytest.mark.unit
    def test_remote_returns_tracked_list(self, service, admin_db):
        target = 0xABCDEF01
        fav = 0x12345678
        AdminRepository.upsert_remote_device_favorite(target, fav, source="managed")

        service.gateway_node_id = 0x99999999
        result = service.get_device_favorites(target)

        assert result["success"] is True
        assert result["source"] == "tracked"
        assert result["is_local"] is False
        assert result["count"] == 1
        assert int(result["favorites"][0]["node_id"]) == fav
        assert "cannot return" in result["note"].lower()

    @pytest.mark.unit
    def test_local_reads_nodedb_favorites(self, service, admin_db):
        gateway = 0x11111111
        fav_id = 0x22222222
        publisher = MagicMock()
        publisher._interface.nodesByNum = {
            fav_id: {
                "num": fav_id,
                "isFavorite": True,
                "user": {
                    "longName": "Hill Top Roof",
                    "shortName": "Hill",
                },
            },
            0x33333333: {
                "num": 0x33333333,
                "isFavorite": False,
                "user": {"longName": "Other", "shortName": "Othr"},
            },
        }

        service.gateway_node_id = gateway
        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.get_device_favorites(gateway)

        assert result["success"] is True
        assert result["source"] == "device"
        assert result["is_local"] is True
        assert result["count"] == 1
        fav = result["favorites"][0]
        assert int(fav["node_id"]) == fav_id
        assert fav["long_name"] == "Hill Top Roof"
