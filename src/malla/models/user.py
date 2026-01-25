"""
User model for authentication.

Provides Flask-Login integration and password hashing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from flask_login import UserMixin

logger = logging.getLogger(__name__)


class UserRole(str, Enum):
    """User roles for access control."""

    VIEWER = "viewer"  # Read-only access to all data
    OPERATOR = "operator"  # Can use admin features on allowed nodes
    ADMIN = "admin"  # Full access including user management


@dataclass
class User(UserMixin):
    """User model with Flask-Login integration."""

    id: int
    username: str
    password_hash: str
    role: UserRole
    created_at: float
    last_login: float | None = None
    active: bool = True

    @property
    def is_active(self) -> bool:  # type: ignore[override]
        """Flask-Login requires this property."""
        return self.active

    def get_id(self) -> str:
        """Return user ID as string for Flask-Login."""
        return str(self.id)

    def has_role(self, role: UserRole | str) -> bool:
        """Check if user has at least the specified role level."""
        role_hierarchy = {
            UserRole.VIEWER: 0,
            UserRole.OPERATOR: 1,
            UserRole.ADMIN: 2,
        }
        if isinstance(role, str):
            role = UserRole(role)

        return role_hierarchy.get(self.role, 0) >= role_hierarchy.get(role, 0)

    def is_admin(self) -> bool:
        """Check if user has admin role."""
        return self.role == UserRole.ADMIN

    def is_operator(self) -> bool:
        """Check if user has operator or higher role."""
        return self.has_role(UserRole.OPERATOR)

    def to_dict(self) -> dict[str, Any]:
        """Convert user to dictionary (without password hash)."""
        return {
            "id": self.id,
            "username": self.username,
            "role": self.role.value,
            "created_at": self.created_at,
            "last_login": self.last_login,
            "is_active": self.is_active,
        }

    @classmethod
    def from_row(cls, row) -> User:
        """Create User from database row."""
        return cls(
            id=row["id"],
            username=row["username"],
            password_hash=row["password_hash"],
            role=UserRole(row["role"]),
            created_at=row["created_at"],
            last_login=row["last_login"],
            active=bool(row["is_active"]),
        )
