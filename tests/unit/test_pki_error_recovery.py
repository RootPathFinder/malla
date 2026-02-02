"""
Unit tests for PKI error code handling and recovery functionality.
"""

from malla.services.tcp_publisher import PKIErrorCodes


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
