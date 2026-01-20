"""
Unit tests for change_registry service.

Tests the ChangeRegistry class that tracks pending configuration changes
following the Meshtastic web client protocol.
"""

import pytest

from malla.services.change_registry import (
    ChangeRegistry,
    ChangeStatus,
    ChangeType,
    get_change_registry,
)

pytestmark = pytest.mark.unit


class TestChangeRegistry:
    """Tests for ChangeRegistry class."""

    def test_begin_transaction(self):
        """Test starting a new transaction."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        assert tx is not None
        assert tx.node_id == 0x12345678
        assert tx.is_active is True
        assert tx.begin_sent is False
        assert tx.commit_sent is False
        assert len(tx.changes) == 0

    def test_unique_transaction_ids(self):
        """Test that transaction IDs are unique."""
        registry = ChangeRegistry()
        tx1 = registry.begin_transaction(0x12345678)
        tx2 = registry.begin_transaction(0x12345678)

        assert tx1.transaction_id != tx2.transaction_id

    def test_get_transaction(self):
        """Test retrieving a transaction by ID."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        retrieved = registry.get_transaction(tx.transaction_id)
        assert retrieved == tx

    def test_get_nonexistent_transaction(self):
        """Test retrieving a non-existent transaction."""
        registry = ChangeRegistry()
        result = registry.get_transaction("nonexistent")
        assert result is None

    def test_register_change(self):
        """Test registering a pending change."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        change = registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value={"role": 0},
            new_value={"role": 1},
        )

        assert change is not None
        assert change.status == ChangeStatus.PENDING
        assert change.config_key == "device"
        assert len(tx.changes) == 1

    def test_register_unchanged_config_skipped(self):
        """Test that unchanged configs are skipped."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        # Same config - should be skipped
        change = registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value={"role": 1},
            new_value={"role": 1},
            skip_if_unchanged=True,
        )

        assert change is not None
        assert change.status == ChangeStatus.SKIPPED

    def test_register_changed_config_not_skipped(self):
        """Test that changed configs are not skipped."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        change = registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value={"role": 0},
            new_value={"role": 1},
            skip_if_unchanged=True,
        )

        assert change is not None
        assert change.status == ChangeStatus.PENDING

    def test_register_change_without_original(self):
        """Test registering a change without original value."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        change = registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value=None,
            new_value={"role": 1},
        )

        assert change is not None
        assert change.status == ChangeStatus.PENDING  # Can't skip without original

    def test_has_pending_changes_true(self):
        """Test has_pending_changes returns True when changes exist."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value=None,
            new_value={"role": 1},
        )

        assert registry.has_pending_changes(tx.transaction_id) is True

    def test_has_pending_changes_false_when_skipped(self):
        """Test has_pending_changes returns False when all changes are skipped."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        # Same config - will be skipped
        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value={"role": 1},
            new_value={"role": 1},
            skip_if_unchanged=True,
        )

        assert registry.has_pending_changes(tx.transaction_id) is False

    def test_get_pending_changes(self):
        """Test getting only pending changes."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        # Pending change
        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value=None,
            new_value={"role": 1},
        )

        # Skipped change
        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="lora",
            original_value={"region": 1},
            new_value={"region": 1},
            skip_if_unchanged=True,
        )

        pending = registry.get_pending_changes(tx.transaction_id)
        assert len(pending) == 1
        assert pending[0].config_key == "device"

    def test_mark_change_applied(self):
        """Test marking a change as applied."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value=None,
            new_value={"role": 1},
        )

        success = registry.mark_change_applied(
            tx.transaction_id, "device", packet_id=12345
        )

        assert success is True
        assert tx.changes[0].status == ChangeStatus.APPLIED
        assert tx.changes[0].packet_id == 12345

    def test_mark_change_failed(self):
        """Test marking a change as failed."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value=None,
            new_value={"role": 1},
        )

        success = registry.mark_change_failed(
            tx.transaction_id, "device", "Connection timeout"
        )

        assert success is True
        assert tx.changes[0].status == ChangeStatus.FAILED
        assert tx.changes[0].error_message == "Connection timeout"

    def test_mark_begin_sent(self):
        """Test marking beginEditSettings as sent."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        registry.mark_begin_sent(tx.transaction_id)

        assert tx.begin_sent is True

    def test_mark_commit_sent(self):
        """Test marking commitEditSettings as sent."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        registry.mark_commit_sent(tx.transaction_id)

        assert tx.commit_sent is True

    def test_complete_transaction(self):
        """Test completing a transaction."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        # Add and apply a change
        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value=None,
            new_value={"role": 1},
        )
        registry.mark_change_applied(tx.transaction_id, "device")

        # Add and skip a change
        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="lora",
            original_value={"region": 1},
            new_value={"region": 1},
            skip_if_unchanged=True,
        )

        summary = registry.complete_transaction(tx.transaction_id)

        assert summary["applied"] == 1
        assert summary["skipped"] == 1
        assert summary["failed"] == 0
        assert summary["total_changes"] == 2
        assert tx.is_active is False

    def test_abort_transaction(self):
        """Test aborting a transaction."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value=None,
            new_value={"role": 1},
        )

        success = registry.abort_transaction(tx.transaction_id)

        assert success is True
        assert tx.is_active is False
        assert tx.changes[0].status == ChangeStatus.FAILED
        assert "aborted" in tx.changes[0].error_message.lower()

    def test_get_revert_data(self):
        """Test getting revert data for applied changes."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CONFIG,
            node_id=0x12345678,
            config_key="device",
            original_value={"role": 0},
            new_value={"role": 1},
        )
        registry.mark_change_applied(tx.transaction_id, "device")

        revert_data = registry.get_revert_data(tx.transaction_id)

        assert len(revert_data) == 1
        assert revert_data[0]["config_key"] == "device"
        assert revert_data[0]["original_value"] == {"role": 0}

    def test_cleanup_old_transactions(self):
        """Test cleaning up old inactive transactions."""
        registry = ChangeRegistry()
        tx1 = registry.begin_transaction(0x12345678)
        tx2 = registry.begin_transaction(0x12345678)

        # Complete tx1 (makes it inactive)
        registry.complete_transaction(tx1.transaction_id)

        removed = registry.cleanup_old_transactions()

        assert removed == 1
        assert registry.get_transaction(tx1.transaction_id) is None
        assert registry.get_transaction(tx2.transaction_id) is not None

    def test_channel_change_type(self):
        """Test registering channel changes."""
        registry = ChangeRegistry()
        tx = registry.begin_transaction(0x12345678)

        change = registry.register_change(
            transaction_id=tx.transaction_id,
            change_type=ChangeType.CHANNEL,
            node_id=0x12345678,
            config_key="channel_0",
            original_value={"role": 1, "name": "LongFast"},
            new_value={"role": 1, "name": "Admin"},
        )

        assert change.change_type == ChangeType.CHANNEL


class TestGlobalRegistry:
    """Tests for the global registry instance."""

    def test_get_change_registry_returns_singleton(self):
        """Test that get_change_registry returns the same instance."""
        registry1 = get_change_registry()
        registry2 = get_change_registry()

        assert registry1 is registry2

    def test_global_registry_is_functional(self):
        """Test that the global registry works correctly."""
        registry = get_change_registry()
        tx = registry.begin_transaction(0xDEADBEEF)

        assert tx is not None
        assert tx.node_id == 0xDEADBEEF
