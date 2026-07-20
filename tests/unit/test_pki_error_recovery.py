"""
Unit tests for PKI error code handling and recovery functionality.
"""

from unittest.mock import MagicMock, patch

from meshtastic import admin_pb2

from malla.services.tcp_publisher import PKIErrorCodes, TCPPublisher


class TestPKIErrorCodes:
    """Tests for PKIErrorCodes classification."""

    def test_recoverable_session_errors_contains_expected_codes(self):
        """Test that ADMIN_BAD_SESSION_KEY and PKI_FAILED are recoverable."""
        assert "ADMIN_BAD_SESSION_KEY" in PKIErrorCodes.RECOVERABLE_SESSION_ERRORS
        assert "PKI_FAILED" in PKIErrorCodes.RECOVERABLE_SESSION_ERRORS

    def test_key_configuration_errors_contains_expected_codes(self):
        """Test that key config errors are correctly classified."""
        assert "PKI_UNKNOWN_PUBKEY" in PKIErrorCodes.REQUIRES_KEY_CONFIGURATION
        assert (
            "ADMIN_PUBLIC_KEY_UNAUTHORIZED" in PKIErrorCodes.REQUIRES_KEY_CONFIGURATION
        )
        assert "NOT_AUTHORIZED" in PKIErrorCodes.REQUIRES_KEY_CONFIGURATION

    def test_is_recoverable_session_error_true_cases(self):
        """Test is_recoverable_session_error returns True for recoverable errors."""
        assert PKIErrorCodes.is_recoverable_session_error("ADMIN_BAD_SESSION_KEY")
        assert PKIErrorCodes.is_recoverable_session_error("PKI_FAILED")

    def test_is_recoverable_session_error_false_cases(self):
        """Test is_recoverable_session_error returns False for non-recoverable errors."""
        assert not PKIErrorCodes.is_recoverable_session_error("NONE")
        assert not PKIErrorCodes.is_recoverable_session_error("PKI_UNKNOWN_PUBKEY")
        assert not PKIErrorCodes.is_recoverable_session_error("NOT_AUTHORIZED")
        assert not PKIErrorCodes.is_recoverable_session_error("TIMEOUT")

    def test_is_key_configuration_error_true_cases(self):
        """Test is_key_configuration_error returns True for config-required errors."""
        assert PKIErrorCodes.is_key_configuration_error("PKI_UNKNOWN_PUBKEY")
        assert PKIErrorCodes.is_key_configuration_error("ADMIN_PUBLIC_KEY_UNAUTHORIZED")
        assert PKIErrorCodes.is_key_configuration_error("NOT_AUTHORIZED")

    def test_is_key_configuration_error_false_cases(self):
        """Test is_key_configuration_error returns False for other errors."""
        assert not PKIErrorCodes.is_key_configuration_error("NONE")
        assert not PKIErrorCodes.is_key_configuration_error("ADMIN_BAD_SESSION_KEY")
        assert not PKIErrorCodes.is_key_configuration_error("PKI_FAILED")
        assert not PKIErrorCodes.is_key_configuration_error("TIMEOUT")

    def test_is_pki_related_true_cases(self):
        """Test is_pki_related returns True for all PKI-related errors."""
        # Recoverable session errors
        assert PKIErrorCodes.is_pki_related("ADMIN_BAD_SESSION_KEY")
        assert PKIErrorCodes.is_pki_related("PKI_FAILED")
        # Key configuration errors
        assert PKIErrorCodes.is_pki_related("PKI_UNKNOWN_PUBKEY")
        assert PKIErrorCodes.is_pki_related("ADMIN_PUBLIC_KEY_UNAUTHORIZED")
        assert PKIErrorCodes.is_pki_related("NOT_AUTHORIZED")

    def test_is_pki_related_false_cases(self):
        """Test is_pki_related returns False for non-PKI errors."""
        assert not PKIErrorCodes.is_pki_related("NONE")
        assert not PKIErrorCodes.is_pki_related("TIMEOUT")
        assert not PKIErrorCodes.is_pki_related("NO_ROUTE")
        assert not PKIErrorCodes.is_pki_related("MAX_RETRANSMIT")

    def test_error_code_constants(self):
        """Test that error code constants are defined correctly."""
        assert PKIErrorCodes.NONE == "NONE"
        assert PKIErrorCodes.NOT_AUTHORIZED == "NOT_AUTHORIZED"
        assert PKIErrorCodes.PKI_FAILED == "PKI_FAILED"
        assert PKIErrorCodes.PKI_UNKNOWN_PUBKEY == "PKI_UNKNOWN_PUBKEY"
        assert PKIErrorCodes.ADMIN_BAD_SESSION_KEY == "ADMIN_BAD_SESSION_KEY"
        assert (
            PKIErrorCodes.ADMIN_PUBLIC_KEY_UNAUTHORIZED
            == "ADMIN_PUBLIC_KEY_UNAUTHORIZED"
        )

    def test_no_overlap_between_recoverable_and_config_errors(self):
        """Test that recoverable and config-required error sets don't overlap."""
        overlap = PKIErrorCodes.RECOVERABLE_SESSION_ERRORS.intersection(
            PKIErrorCodes.REQUIRES_KEY_CONFIGURATION
        )
        assert len(overlap) == 0, f"Unexpected overlap: {overlap}"


class TestSessionPasskeyRefresh:
    """Tests for refreshing session passkeys after ADMIN_BAD_SESSION_KEY."""

    def _publisher(self) -> TCPPublisher:
        publisher = TCPPublisher.__new__(TCPPublisher)
        publisher._session_passkeys = {0x22222222: b"\x01" * 8}
        publisher._session_passkey_lock = __import__("threading").Lock()
        return publisher

    def test_refresh_session_passkey_clears_and_stores_new_key(self):
        publisher = self._publisher()

        def fake_get_metadata(target_node_id: int):
            publisher._session_passkeys[target_node_id] = b"\x02" * 8
            return 0xAAAA

        with (
            patch.object(
                publisher, "send_get_device_metadata", side_effect=fake_get_metadata
            ) as send_get,
            patch.object(
                publisher,
                "get_response",
                return_value={"is_nak": False, "error_reason": "NONE"},
            ),
        ):
            assert publisher.refresh_session_passkey(0x22222222) is True

        send_get.assert_called_once_with(0x22222222)
        assert publisher._session_passkeys[0x22222222] == b"\x02" * 8

    def test_refresh_session_passkey_fails_on_timeout(self):
        publisher = self._publisher()
        with (
            patch.object(publisher, "send_get_device_metadata", return_value=0xAAAA),
            patch.object(publisher, "get_response", return_value=None),
        ):
            assert publisher.refresh_session_passkey(0x22222222) is False
        assert 0x22222222 not in publisher._session_passkeys

    def test_send_admin_with_recovery_refreshes_passkey_before_retry(self):
        publisher = self._publisher()
        admin_msg = admin_pb2.AdminMessage()
        admin_msg.get_device_metadata_request = True
        admin_msg.session_passkey = b"\x01" * 8

        responses = [
            {
                "is_nak": True,
                "error_reason": "ADMIN_BAD_SESSION_KEY",
            },
            {
                "is_nak": False,
                "error_reason": "NONE",
                "admin_message": admin_msg,
            },
        ]

        with (
            patch.object(
                publisher, "send_admin_message", side_effect=[0x1111, 0x2222]
            ) as send_admin,
            patch.object(publisher, "get_response", side_effect=responses),
            patch.object(
                publisher, "refresh_session_passkey", return_value=True
            ) as refresh,
        ):
            result = publisher.send_admin_with_recovery(
                target_node_id=0x22222222,
                admin_message=admin_msg,
                timeout=5.0,
            )

        assert result["success"] is True
        assert result["recovered"] is True
        refresh.assert_called_once_with(0x22222222, timeout=5.0)
        assert send_admin.call_count == 2

    def test_execute_with_session_recovery_retries_up_to_three_times(self):
        publisher = self._publisher()
        responses = [
            {"is_nak": True, "error_reason": "ADMIN_BAD_SESSION_KEY"},
            {"is_nak": True, "error_reason": "ADMIN_BAD_SESSION_KEY"},
            {"is_nak": False, "error_reason": "NONE"},
        ]

        with (
            patch.object(
                publisher, "get_response", side_effect=responses
            ),
            patch.object(
                publisher, "refresh_session_passkey", return_value=True
            ) as refresh,
        ):
            result = publisher.execute_with_session_recovery(
                target_node_id=0x22222222,
                send_fn=MagicMock(side_effect=[0x1, 0x2, 0x3]),
                timeout=5.0,
            )

        assert result["success"] is True
        assert result["recovered"] is True
        assert result["attempts"] == 3
        assert refresh.call_count == 2

    def test_execute_with_session_recovery_stops_after_max_attempts(self):
        publisher = self._publisher()
        bad = {"is_nak": True, "error_reason": "ADMIN_BAD_SESSION_KEY"}

        with (
            patch.object(publisher, "get_response", return_value=bad),
            patch.object(
                publisher, "refresh_session_passkey", return_value=True
            ) as refresh,
        ):
            result = publisher.execute_with_session_recovery(
                target_node_id=0x22222222,
                send_fn=MagicMock(side_effect=[0x1, 0x2, 0x3]),
                timeout=5.0,
            )

        assert result["success"] is False
        assert result["attempts"] == 3
        assert refresh.call_count == 2
        assert "ADMIN_BAD_SESSION_KEY" in (result["error"] or "")
