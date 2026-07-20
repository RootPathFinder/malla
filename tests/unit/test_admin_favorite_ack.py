"""Unit tests for ACK waiting on remote favorite admin writes."""

from unittest.mock import MagicMock, patch

import pytest

from src.malla.database.admin_repository import AdminRepository, init_admin_tables
from src.malla.database.connection import get_db_connection
from src.malla.services.admin_service import AdminConnectionType, AdminService


@pytest.fixture
def admin_db(tmp_path, monkeypatch):
    db_path = tmp_path / "admin_favorite_ack.db"
    cfg = MagicMock()
    cfg.database_file = str(db_path)
    # Env var takes precedence over get_config() — isolate per test for xdist.
    monkeypatch.setenv("MALLA_DATABASE_FILE", str(db_path))
    monkeypatch.setattr("malla.database.connection.get_config", lambda: cfg)
    monkeypatch.setattr("src.malla.database.connection.get_config", lambda: cfg)
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


@pytest.fixture
def service(admin_db):
    AdminService._instance = None
    svc = AdminService()
    svc._connection_type = AdminConnectionType.TCP
    svc._gateway_node_id = 0x11111111
    return svc


class TestFavoriteWriteAck:
    @pytest.mark.unit
    def test_set_favorite_ack_success(self, service, admin_db):
        publisher = MagicMock()
        publisher.send_set_favorite_node.return_value = 0xABCD
        publisher.get_response.return_value = {
            "is_ack": True,
            "is_nak": False,
            "error_reason": "NONE",
        }

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.set_favorite_node(0x22222222, 0x33333333)

        assert result.success is True
        assert result.response["acknowledged"] is True
        assert "ACK received" in result.response["message"]
        publisher.get_response.assert_called_once_with(0xABCD, timeout=10.0)

        tracked = AdminRepository.list_remote_device_favorites(0x22222222)
        assert {int(r["node_id"]) for r in tracked} == {0x33333333}

    @pytest.mark.unit
    def test_set_favorite_nak_fails_and_does_not_track(self, service, admin_db):
        publisher = MagicMock()
        publisher.send_set_favorite_node.return_value = 0xABCD
        publisher.get_response.return_value = {
            "is_ack": False,
            "is_nak": True,
            "error_reason": "NO_RESPONSE",
        }

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.set_favorite_node(0x22222222, 0x33333333)

        assert result.success is False
        assert "NO_RESPONSE" in (result.error or "")
        assert AdminRepository.list_remote_device_favorites(0x22222222) == []

    @pytest.mark.unit
    def test_set_favorite_timeout_sent_unacked(self, service, admin_db):
        publisher = MagicMock()
        publisher.send_set_favorite_node.return_value = 0xABCD
        publisher.get_response.return_value = None

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.set_favorite_node(0x22222222, 0x33333333)

        assert result.success is True
        assert result.response["acknowledged"] is False
        assert "no ACK" in result.response["message"]

        tracked = AdminRepository.list_remote_device_favorites(0x22222222)
        assert {int(r["node_id"]) for r in tracked} == {0x33333333}

    @pytest.mark.unit
    def test_remove_favorite_ack_success(self, service, admin_db):
        AdminRepository.upsert_remote_device_favorite(
            0x22222222, 0x33333333, source="managed"
        )
        publisher = MagicMock()
        publisher.send_remove_favorite_node.return_value = 0xBEEF
        publisher.get_response.return_value = {
            "is_ack": True,
            "is_nak": False,
            "error_reason": "NONE",
        }

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.remove_favorite_node(0x22222222, 0x33333333)

        assert result.success is True
        assert result.response["acknowledged"] is True
        assert AdminRepository.list_remote_device_favorites(0x22222222) == []

    @pytest.mark.unit
    def test_remove_favorite_nak_keeps_tracked(self, service, admin_db):
        AdminRepository.upsert_remote_device_favorite(
            0x22222222, 0x33333333, source="managed"
        )
        publisher = MagicMock()
        publisher.send_remove_favorite_node.return_value = 0xBEEF
        publisher.get_response.return_value = {
            "is_ack": False,
            "is_nak": True,
            "error_reason": "NO_CHANNEL",
        }

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.remove_favorite_node(0x22222222, 0x33333333)

        assert result.success is False
        tracked = AdminRepository.list_remote_device_favorites(0x22222222)
        assert {int(r["node_id"]) for r in tracked} == {0x33333333}


    @pytest.mark.unit
    def test_set_favorite_fetches_passkey_when_missing(self, service, admin_db):
        publisher = MagicMock()
        publisher.has_session_passkey.return_value = False
        publisher.refresh_session_passkey.return_value = True
        publisher.send_set_favorite_node.return_value = 0xABCD
        publisher.get_response.return_value = {
            "is_ack": True,
            "is_nak": False,
            "error_reason": "NONE",
        }

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.set_favorite_node(0x22222222, 0x33333333)

        assert result.success is True
        publisher.refresh_session_passkey.assert_called_once_with(
            0x22222222, timeout=30.0
        )
        assert result.retry_info is not None
        assert any(
            "No session passkey" in line
            for line in result.retry_info[0]["recovery_log"]
        )


class TestFavoriteSessionKeyRecovery:
    @pytest.mark.unit
    def test_set_favorite_retries_after_bad_session_key(self, service, admin_db):
        publisher = MagicMock()
        publisher.send_set_favorite_node.side_effect = [0xABCD, 0xDCBA]
        publisher.refresh_session_passkey.return_value = True
        publisher.get_response.side_effect = [
            {
                "is_ack": False,
                "is_nak": True,
                "error_reason": "ADMIN_BAD_SESSION_KEY",
            },
            {
                "is_ack": True,
                "is_nak": False,
                "error_reason": "NONE",
            },
        ]

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.set_favorite_node(0x22222222, 0x33333333)

        assert result.success is True
        assert result.response["acknowledged"] is True
        assert result.response["recovered"] is True
        assert "session key refreshed" in result.response["message"]
        publisher.refresh_session_passkey.assert_called_once_with(
            0x22222222, timeout=30.0
        )
        assert publisher.send_set_favorite_node.call_count == 2
        assert publisher.get_response.call_count == 2

        tracked = AdminRepository.list_remote_device_favorites(0x22222222)
        assert {int(r["node_id"]) for r in tracked} == {0x33333333}

    @pytest.mark.unit
    def test_set_favorite_retries_up_to_three_attempts(self, service, admin_db):
        publisher = MagicMock()
        publisher.send_set_favorite_node.side_effect = [0xA1, 0xA2, 0xA3]
        publisher.refresh_session_passkey.return_value = True
        publisher.get_response.side_effect = [
            {
                "is_ack": False,
                "is_nak": True,
                "error_reason": "ADMIN_BAD_SESSION_KEY",
            },
            {
                "is_ack": False,
                "is_nak": True,
                "error_reason": "ADMIN_BAD_SESSION_KEY",
            },
            {
                "is_ack": True,
                "is_nak": False,
                "error_reason": "NONE",
            },
        ]

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.set_favorite_node(0x22222222, 0x33333333)

        assert result.success is True
        assert result.response["recovered"] is True
        assert publisher.send_set_favorite_node.call_count == 3
        assert publisher.refresh_session_passkey.call_count == 2

    @pytest.mark.unit
    def test_set_favorite_fails_after_three_bad_session_keys(self, service, admin_db):
        publisher = MagicMock()
        publisher.send_set_favorite_node.side_effect = [0xA1, 0xA2, 0xA3]
        publisher.refresh_session_passkey.return_value = True
        bad = {
            "is_ack": False,
            "is_nak": True,
            "error_reason": "ADMIN_BAD_SESSION_KEY",
        }
        publisher.get_response.side_effect = [bad, bad, bad]

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.set_favorite_node(0x22222222, 0x33333333)

        assert result.success is False
        assert "ADMIN_BAD_SESSION_KEY" in (result.error or "")
        assert publisher.send_set_favorite_node.call_count == 3
        assert publisher.refresh_session_passkey.call_count == 2
        assert AdminRepository.list_remote_device_favorites(0x22222222) == []

    @pytest.mark.unit
    def test_set_favorite_fails_when_session_refresh_fails(self, service, admin_db):
        publisher = MagicMock()
        publisher.send_set_favorite_node.return_value = 0xABCD
        publisher.refresh_session_passkey.return_value = False
        publisher.get_response.return_value = {
            "is_ack": False,
            "is_nak": True,
            "error_reason": "ADMIN_BAD_SESSION_KEY",
        }

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.set_favorite_node(0x22222222, 0x33333333)

        assert result.success is False
        assert "ADMIN_BAD_SESSION_KEY" in (result.error or "")
        assert "failed to refresh session passkey" in (result.error or "")
        publisher.send_set_favorite_node.assert_called_once()
        assert AdminRepository.list_remote_device_favorites(0x22222222) == []

    @pytest.mark.unit
    def test_remove_favorite_retries_after_bad_session_key(self, service, admin_db):
        AdminRepository.upsert_remote_device_favorite(
            0x22222222, 0x33333333, source="managed"
        )
        publisher = MagicMock()
        publisher.send_remove_favorite_node.side_effect = [0xBEEF, 0xCAFE]
        publisher.refresh_session_passkey.return_value = True
        publisher.get_response.side_effect = [
            {
                "is_ack": False,
                "is_nak": True,
                "error_reason": "ADMIN_BAD_SESSION_KEY",
            },
            {
                "is_ack": True,
                "is_nak": False,
                "error_reason": "NONE",
            },
        ]

        with patch.object(service, "_get_publisher", return_value=publisher):
            result = service.remove_favorite_node(0x22222222, 0x33333333)

        assert result.success is True
        assert result.response["recovered"] is True
        assert AdminRepository.list_remote_device_favorites(0x22222222) == []


class TestTcpPublisherAckCorrelation:
    @pytest.mark.unit
    def test_routing_ack_matches_decoded_request_id(self):
        from src.malla.services.tcp_publisher import TCPPublisher

        publisher = TCPPublisher.__new__(TCPPublisher)
        publisher._pending_responses = {}
        publisher._response_events = {}
        publisher._response_lock = __import__("threading").Lock()
        publisher._last_admin_response = None
        publisher._admin_response_event = __import__("threading").Event()
        publisher._interface = MagicMock()
        publisher._pending_telemetry_lock = __import__("threading").Lock()
        publisher._pending_telemetry_requests = {}

        packet_id = 0x12345678
        publisher._pending_responses[packet_id] = {}
        event = __import__("threading").Event()
        publisher._response_events[packet_id] = event

        packet = {
            "decoded": {
                "portnum": "ROUTING_APP",
                "requestId": packet_id,
                "routing": {"errorReason": "NONE"},
            },
            "fromId": "!aabbccdd",
            "from": 0xAABBCCDD,
        }
        publisher._on_receive(packet)

        assert event.is_set()
        assert publisher._pending_responses[packet_id]["is_ack"] is True
        assert publisher._pending_responses[packet_id]["is_nak"] is False
