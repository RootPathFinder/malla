"""
Change Registry service following Meshtastic web client protocols.

This module provides change tracking for pending configuration modifications,
matching the behavior of the official web client's ChangeRegistry pattern.

Key features:
- Track pending config changes with original values
- Support diff/revert operations
- Transaction-aware change batching
- Skip-if-unchanged detection
"""

import logging
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ..utils.config_compare import (
    configs_are_equal,
    deep_compare_config,
    get_config_diff_summary,
)

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Type of configuration change."""

    CONFIG = "config"
    MODULE_CONFIG = "module_config"
    CHANNEL = "channel"
    OWNER = "owner"


class ChangeStatus(Enum):
    """Status of a pending change."""

    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"
    SKIPPED = "skipped"  # Skipped because unchanged


@dataclass
class PendingChange:
    """Represents a pending configuration change."""

    change_type: ChangeType
    node_id: int
    config_key: str  # e.g., "device", "lora", "channel_0"
    original_value: dict | None
    new_value: dict
    status: ChangeStatus = ChangeStatus.PENDING
    error_message: str | None = None
    packet_id: int | None = None


@dataclass
class ChangeTransaction:
    """Represents a transaction grouping multiple changes."""

    transaction_id: str
    node_id: int
    changes: list[PendingChange] = field(default_factory=list)
    is_active: bool = True
    begin_sent: bool = False
    commit_sent: bool = False


class ChangeRegistry:
    """
    Registry for tracking pending configuration changes.

    This implements the change tracking pattern from the Meshtastic web client,
    allowing detection of unchanged configs and proper transaction management.

    Usage:
        registry = ChangeRegistry()

        # Start a transaction
        tx = registry.begin_transaction(node_id)

        # Register changes
        registry.register_change(
            tx.transaction_id,
            ChangeType.CONFIG,
            node_id,
            "device",
            original_config,
            new_config,
        )

        # Check if any changes are needed
        if registry.has_pending_changes(tx.transaction_id):
            # Apply changes...
            registry.mark_change_applied(tx.transaction_id, "device", packet_id)

        # Complete transaction
        registry.complete_transaction(tx.transaction_id)
    """

    def __init__(self) -> None:
        """Initialize the change registry."""
        self._transactions: dict[str, ChangeTransaction] = {}
        self._transaction_counter = 0
        self._lock = threading.Lock()

    def _generate_transaction_id(self, node_id: int) -> str:
        """Generate a unique transaction ID."""
        with self._lock:
            self._transaction_counter += 1
            return f"tx_{node_id:08x}_{self._transaction_counter}"

    def begin_transaction(self, node_id: int) -> ChangeTransaction:
        """
        Begin a new change transaction for a node.

        Args:
            node_id: The target node ID

        Returns:
            ChangeTransaction object to track changes
        """
        tx_id = self._generate_transaction_id(node_id)
        transaction = ChangeTransaction(
            transaction_id=tx_id,
            node_id=node_id,
        )
        self._transactions[tx_id] = transaction
        logger.debug(f"Started transaction {tx_id} for node {node_id:08x}")
        return transaction

    def get_transaction(self, transaction_id: str) -> ChangeTransaction | None:
        """Get a transaction by ID."""
        return self._transactions.get(transaction_id)

    def register_change(
        self,
        transaction_id: str,
        change_type: ChangeType,
        node_id: int,
        config_key: str,
        original_value: dict | None,
        new_value: dict,
        skip_if_unchanged: bool = True,
    ) -> PendingChange | None:
        """
        Register a pending configuration change.

        Args:
            transaction_id: Transaction to add change to
            change_type: Type of change (config, channel, etc.)
            node_id: Target node ID
            config_key: Config type key (e.g., "device", "lora", "channel_0")
            original_value: Current value from node (None if unknown)
            new_value: New value to apply
            skip_if_unchanged: If True, skip registration if configs are equal

        Returns:
            PendingChange object or None if skipped due to being unchanged
        """
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            logger.error(f"Transaction {transaction_id} not found")
            return None

        if not transaction.is_active:
            logger.error(f"Transaction {transaction_id} is not active")
            return None

        # Check if change is actually needed
        if skip_if_unchanged and original_value is not None:
            if configs_are_equal(new_value, original_value, config_key):
                logger.debug(
                    f"Skipping change for {config_key} on node {node_id:08x} - unchanged"
                )
                skipped_change = PendingChange(
                    change_type=change_type,
                    node_id=node_id,
                    config_key=config_key,
                    original_value=original_value,
                    new_value=new_value,
                    status=ChangeStatus.SKIPPED,
                )
                transaction.changes.append(skipped_change)
                return skipped_change

        # Register the change
        change = PendingChange(
            change_type=change_type,
            node_id=node_id,
            config_key=config_key,
            original_value=original_value,
            new_value=new_value,
            status=ChangeStatus.PENDING,
        )
        transaction.changes.append(change)

        logger.debug(
            f"Registered {change_type.value} change for {config_key} on node {node_id:08x}"
        )
        if original_value is not None:
            diff = deep_compare_config(new_value, original_value, config_key)
            if diff:
                logger.debug(f"Change diff: {get_config_diff_summary(diff)}")

        return change

    def has_pending_changes(self, transaction_id: str) -> bool:
        """Check if transaction has any pending changes that need to be applied."""
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return False

        return any(
            change.status == ChangeStatus.PENDING for change in transaction.changes
        )

    def get_pending_changes(self, transaction_id: str) -> list[PendingChange]:
        """Get all pending changes for a transaction."""
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return []

        return [
            change
            for change in transaction.changes
            if change.status == ChangeStatus.PENDING
        ]

    def get_all_changes(self, transaction_id: str) -> list[PendingChange]:
        """Get all changes for a transaction including skipped."""
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return []

        return transaction.changes.copy()

    def mark_change_applied(
        self,
        transaction_id: str,
        config_key: str,
        packet_id: int | None = None,
    ) -> bool:
        """
        Mark a change as successfully applied.

        Args:
            transaction_id: Transaction ID
            config_key: Config key of the applied change
            packet_id: Optional packet ID from the send operation

        Returns:
            True if change was found and marked
        """
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return False

        for change in transaction.changes:
            if (
                change.config_key == config_key
                and change.status == ChangeStatus.PENDING
            ):
                change.status = ChangeStatus.APPLIED
                change.packet_id = packet_id
                logger.debug(
                    f"Marked change {config_key} as applied in transaction {transaction_id}"
                )
                return True

        return False

    def mark_change_failed(
        self,
        transaction_id: str,
        config_key: str,
        error_message: str,
    ) -> bool:
        """
        Mark a change as failed.

        Args:
            transaction_id: Transaction ID
            config_key: Config key of the failed change
            error_message: Error description

        Returns:
            True if change was found and marked
        """
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return False

        for change in transaction.changes:
            if (
                change.config_key == config_key
                and change.status == ChangeStatus.PENDING
            ):
                change.status = ChangeStatus.FAILED
                change.error_message = error_message
                logger.debug(
                    f"Marked change {config_key} as failed in transaction {transaction_id}: {error_message}"
                )
                return True

        return False

    def mark_begin_sent(self, transaction_id: str) -> bool:
        """Mark that beginEditSettings has been sent."""
        transaction = self._transactions.get(transaction_id)
        if transaction:
            transaction.begin_sent = True
            return True
        return False

    def mark_commit_sent(self, transaction_id: str) -> bool:
        """Mark that commitEditSettings has been sent."""
        transaction = self._transactions.get(transaction_id)
        if transaction:
            transaction.commit_sent = True
            return True
        return False

    def complete_transaction(self, transaction_id: str) -> dict[str, Any]:
        """
        Complete a transaction and return summary.

        Args:
            transaction_id: Transaction to complete

        Returns:
            Summary dict with counts of applied, skipped, failed changes
        """
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return {"error": "Transaction not found"}

        transaction.is_active = False

        applied = sum(
            1 for c in transaction.changes if c.status == ChangeStatus.APPLIED
        )
        skipped = sum(
            1 for c in transaction.changes if c.status == ChangeStatus.SKIPPED
        )
        failed = sum(1 for c in transaction.changes if c.status == ChangeStatus.FAILED)
        pending = sum(
            1 for c in transaction.changes if c.status == ChangeStatus.PENDING
        )

        summary = {
            "transaction_id": transaction_id,
            "node_id": transaction.node_id,
            "total_changes": len(transaction.changes),
            "applied": applied,
            "skipped": skipped,
            "failed": failed,
            "pending": pending,
            "begin_sent": transaction.begin_sent,
            "commit_sent": transaction.commit_sent,
        }

        logger.info(
            f"Completed transaction {transaction_id}: "
            f"applied={applied}, skipped={skipped}, failed={failed}, pending={pending}"
        )

        return summary

    def abort_transaction(self, transaction_id: str) -> bool:
        """
        Abort a transaction, marking all pending changes as failed.

        Args:
            transaction_id: Transaction to abort

        Returns:
            True if transaction was found and aborted
        """
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return False

        transaction.is_active = False
        for change in transaction.changes:
            if change.status == ChangeStatus.PENDING:
                change.status = ChangeStatus.FAILED
                change.error_message = "Transaction aborted"

        logger.info(f"Aborted transaction {transaction_id}")
        return True

    def get_revert_data(self, transaction_id: str) -> list[dict]:
        """
        Get data needed to revert applied changes.

        Returns a list of dicts with original values for all applied changes.

        Args:
            transaction_id: Transaction to get revert data for

        Returns:
            List of dicts with change_type, config_key, and original_value
        """
        transaction = self._transactions.get(transaction_id)
        if not transaction:
            return []

        return [
            {
                "change_type": change.change_type.value,
                "config_key": change.config_key,
                "original_value": change.original_value,
                "node_id": change.node_id,
            }
            for change in transaction.changes
            if change.status == ChangeStatus.APPLIED
            and change.original_value is not None
        ]

    def cleanup_old_transactions(self, max_age_seconds: int = 3600) -> int:
        """
        Remove old inactive transactions.

        Args:
            max_age_seconds: Maximum age for keeping transactions (default 1 hour)

        Returns:
            Number of transactions removed
        """
        # For now, just remove all inactive transactions
        # In a real implementation, we'd track timestamps
        to_remove = [
            tx_id for tx_id, tx in self._transactions.items() if not tx.is_active
        ]
        for tx_id in to_remove:
            del self._transactions[tx_id]

        if to_remove:
            logger.debug(f"Cleaned up {len(to_remove)} old transactions")

        return len(to_remove)


# Global registry instance
_change_registry: ChangeRegistry | None = None


def get_change_registry() -> ChangeRegistry:
    """Get the global change registry instance."""
    global _change_registry
    if _change_registry is None:
        _change_registry = ChangeRegistry()
    return _change_registry
