"""Unit tests for job confirmation / success-level helpers."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.malla.services.admin_service import AdminCommandResult
from src.malla.services.job_service import (
    CONFIRMATION_ACKED,
    CONFIRMATION_FAILED,
    CONFIRMATION_MIXED,
    CONFIRMATION_SKIPPED,
    CONFIRMATION_UNACKED,
    CONFIRMATION_VERIFIED,
    JobService,
    confirmation_counts,
    confirmation_from_admin_result,
    summarize_confirmations,
)


class TestConfirmationFromAdminResult:
    @pytest.mark.unit
    def test_failed_result(self):
        result = SimpleNamespace(success=False, response=None, error="NAK")
        assert confirmation_from_admin_result(result) == CONFIRMATION_FAILED

    @pytest.mark.unit
    def test_acked(self):
        result = SimpleNamespace(
            success=True, response={"acknowledged": True, "message": "ok"}
        )
        assert confirmation_from_admin_result(result) == CONFIRMATION_ACKED

    @pytest.mark.unit
    def test_unacked(self):
        result = SimpleNamespace(
            success=True, response={"acknowledged": False, "message": "sent"}
        )
        assert confirmation_from_admin_result(result) == CONFIRMATION_UNACKED

    @pytest.mark.unit
    def test_success_without_flag_is_unacked(self):
        result = SimpleNamespace(success=True, response={"message": "ok"})
        assert confirmation_from_admin_result(result) == CONFIRMATION_UNACKED


class TestSummarizeConfirmations:
    @pytest.mark.unit
    def test_all_acked(self):
        assert (
            summarize_confirmations([CONFIRMATION_ACKED, CONFIRMATION_ACKED])
            == CONFIRMATION_ACKED
        )

    @pytest.mark.unit
    def test_verified(self):
        assert (
            summarize_confirmations([CONFIRMATION_VERIFIED, CONFIRMATION_VERIFIED])
            == CONFIRMATION_VERIFIED
        )

    @pytest.mark.unit
    def test_mixed_ack_and_unacked(self):
        assert (
            summarize_confirmations([CONFIRMATION_ACKED, CONFIRMATION_UNACKED])
            == CONFIRMATION_MIXED
        )

    @pytest.mark.unit
    def test_skips_ignored_when_others_present(self):
        assert (
            summarize_confirmations(
                [CONFIRMATION_SKIPPED, CONFIRMATION_ACKED, CONFIRMATION_ACKED]
            )
            == CONFIRMATION_ACKED
        )

    @pytest.mark.unit
    def test_only_skipped(self):
        assert (
            summarize_confirmations([CONFIRMATION_SKIPPED, CONFIRMATION_SKIPPED])
            == CONFIRMATION_SKIPPED
        )

    @pytest.mark.unit
    def test_failed_with_success_is_mixed(self):
        assert (
            summarize_confirmations([CONFIRMATION_ACKED, CONFIRMATION_FAILED])
            == CONFIRMATION_MIXED
        )

    @pytest.mark.unit
    def test_counts(self):
        assert confirmation_counts(
            [CONFIRMATION_ACKED, CONFIRMATION_ACKED, CONFIRMATION_UNACKED]
        ) == {CONFIRMATION_ACKED: 2, CONFIRMATION_UNACKED: 1}


class TestConfigDeployJobConfirmation:
    @pytest.mark.unit
    def test_config_deploy_includes_ack_confirmation(self, monkeypatch):
        service = JobService.__new__(JobService)
        progress = MagicMock()
        admin = MagicMock()
        admin.set_config.return_value = AdminCommandResult(
            success=True,
            log_id=1,
            response={"message": "Config updated - ACK received", "acknowledged": True},
        )

        monkeypatch.setattr(
            "src.malla.services.admin_service.get_admin_service", lambda: admin
        )

        result = service._execute_config_deploy_job(
            {
                "target_node_id": 0x1111,
                "job_data": {"config_type": "DEVICE", "config_data": {"role": 1}},
            },
            progress,
        )

        assert result["success"] is True
        assert result["data"]["confirmation"] == CONFIRMATION_ACKED
        assert result["data"]["acknowledged"] is True

    @pytest.mark.unit
    def test_config_deploy_unacked(self, monkeypatch):
        service = JobService.__new__(JobService)
        progress = MagicMock()
        admin = MagicMock()
        admin.set_config.return_value = AdminCommandResult(
            success=True,
            log_id=2,
            response={
                "message": "Config sent - no ACK received (likely applied)",
                "acknowledged": False,
            },
        )

        monkeypatch.setattr(
            "src.malla.services.admin_service.get_admin_service", lambda: admin
        )

        result = service._execute_config_deploy_job(
            {
                "target_node_id": 0x2222,
                "job_data": {"config_type": "DEVICE", "config_data": {"role": 2}},
            },
            progress,
        )

        assert result["success"] is True
        assert result["data"]["confirmation"] == CONFIRMATION_UNACKED
        assert result["data"]["acknowledged"] is False
