"""
Authentication service for user management.

Handles user creation, authentication, password hashing, and session management.
"""

from __future__ import annotations

import json
import logging
import secrets
import time
from typing import TYPE_CHECKING

import bcrypt
from flask_login import LoginManager

from ..database.connection import get_db_connection
from ..models.user import User, UserRole

if TYPE_CHECKING:
    from flask import Flask

logger = logging.getLogger(__name__)

# Flask-Login manager instance
login_manager = LoginManager()
login_manager.login_view = "auth.login"  # type: ignore[assignment]
login_manager.login_message = "Please log in to access this page."
login_manager.login_message_category = "info"


def init_auth(app: Flask) -> None:
    """Initialize authentication for the Flask app."""
    login_manager.init_app(app)
    _ensure_users_table()
    _ensure_default_admin()


@login_manager.user_loader
def load_user(user_id: str) -> User | None:
    """Load user by ID for Flask-Login."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM users WHERE id = ? AND is_active = 1", (int(user_id),)
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            return User.from_row(row)
        return None
    except Exception as e:
        logger.error(f"Error loading user {user_id}: {e}")
        return None


class AuthService:
    """Service for authentication operations."""

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password using bcrypt."""
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

    @staticmethod
    def verify_password(password: str, password_hash: str) -> bool:
        """Verify a password against its hash."""
        try:
            return bcrypt.checkpw(
                password.encode("utf-8"), password_hash.encode("utf-8")
            )
        except Exception as e:
            logger.error(f"Password verification error: {e}")
            return False

    @staticmethod
    def authenticate(username: str, password: str) -> User | None:
        """
        Authenticate a user by username and password.

        Returns User object if authentication succeeds, None otherwise.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM users WHERE username = ? AND is_active = 1",
                (username.lower(),),
            )
            row = cursor.fetchone()

            if row and AuthService.verify_password(password, row["password_hash"]):
                # Update last login time
                cursor.execute(
                    "UPDATE users SET last_login = ? WHERE id = ?",
                    (time.time(), row["id"]),
                )
                conn.commit()
                conn.close()

                user = User.from_row(row)
                user.last_login = time.time()
                logger.info(f"User '{username}' authenticated successfully")
                return user

            conn.close()
            logger.warning(f"Failed authentication attempt for user '{username}'")
            return None
        except Exception as e:
            logger.error(f"Authentication error for user '{username}': {e}")
            return None

    @staticmethod
    def create_user(
        username: str,
        password: str,
        role: UserRole = UserRole.VIEWER,
    ) -> User | None:
        """
        Create a new user.

        Returns the created User object or None if creation fails.
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Check if username already exists
            cursor.execute(
                "SELECT id FROM users WHERE username = ?", (username.lower(),)
            )
            if cursor.fetchone():
                logger.warning(f"User '{username}' already exists")
                conn.close()
                return None

            password_hash = AuthService.hash_password(password)
            created_at = time.time()

            cursor.execute(
                """
                INSERT INTO users (username, password_hash, role, created_at, is_active)
                VALUES (?, ?, ?, ?, 1)
                """,
                (username.lower(), password_hash, role.value, created_at),
            )
            conn.commit()

            user_id = cursor.lastrowid
            conn.close()

            if user_id is None:
                logger.error(f"Failed to get user ID after creating user '{username}'")
                return None

            logger.info(f"Created user '{username}' with role '{role.value}'")
            return User(
                id=user_id,
                username=username.lower(),
                password_hash=password_hash,
                role=role,
                created_at=created_at,
            )
        except Exception as e:
            logger.error(f"Error creating user '{username}': {e}")
            return None

    @staticmethod
    def update_password(user_id: int, new_password: str) -> bool:
        """Update a user's password."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            password_hash = AuthService.hash_password(new_password)
            cursor.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (password_hash, user_id),
            )
            conn.commit()
            conn.close()

            logger.info(f"Password updated for user ID {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating password for user ID {user_id}: {e}")
            return False

    @staticmethod
    def update_role(user_id: int, new_role: UserRole) -> bool:
        """Update a user's role."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE users SET role = ? WHERE id = ?",
                (new_role.value, user_id),
            )
            conn.commit()
            conn.close()

            logger.info(f"Role updated to '{new_role.value}' for user ID {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating role for user ID {user_id}: {e}")
            return False

    @staticmethod
    def get_user_preferences(user_id: int) -> dict:
        """
        Get user preferences from database.

        Args:
            user_id: User ID

        Returns:
            dict: User preferences (empty dict if none set)
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT preferences FROM users WHERE id = ?", (user_id,))
            row = cursor.fetchone()
            conn.close()

            if row and row["preferences"]:
                return json.loads(row["preferences"])
            return {}
        except Exception as e:
            logger.error(f"Error getting preferences for user {user_id}: {e}")
            return {}

    @staticmethod
    def set_user_preferences(user_id: int, preferences: dict) -> bool:
        """
        Set user preferences in database.

        Args:
            user_id: User ID
            preferences: Preferences dictionary

        Returns:
            bool: True if successful
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET preferences = ? WHERE id = ?",
                (json.dumps(preferences), user_id),
            )
            conn.commit()
            conn.close()

            logger.debug(f"Preferences updated for user ID {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error setting preferences for user {user_id}: {e}")
            return False

    @staticmethod
    def update_user_preference(user_id: int, key: str, value) -> bool:
        """
        Update a single user preference.

        Args:
            user_id: User ID
            key: Preference key
            value: Preference value

        Returns:
            bool: True if successful
        """
        try:
            prefs = AuthService.get_user_preferences(user_id)
            prefs[key] = value
            return AuthService.set_user_preferences(user_id, prefs)
        except Exception as e:
            logger.error(f"Error updating preference {key} for user {user_id}: {e}")
            return False

    @staticmethod
    def deactivate_user(user_id: int) -> bool:
        """Deactivate a user (soft delete)."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("UPDATE users SET is_active = 0 WHERE id = ?", (user_id,))
            conn.commit()
            conn.close()

            logger.info(f"Deactivated user ID {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error deactivating user ID {user_id}: {e}")
            return False

    @staticmethod
    def get_user(user_id: int) -> User | None:
        """Get a user by ID."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                return User.from_row(row)
            return None
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None

    @staticmethod
    def get_user_by_username(username: str) -> User | None:
        """Get a user by username."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM users WHERE username = ?", (username.lower(),)
            )
            row = cursor.fetchone()
            conn.close()

            if row:
                return User.from_row(row)
            return None
        except Exception as e:
            logger.error(f"Error getting user '{username}': {e}")
            return None

    @staticmethod
    def list_users(include_inactive: bool = False) -> list[User]:
        """List all users."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            if include_inactive:
                cursor.execute("SELECT * FROM users ORDER BY username")
            else:
                cursor.execute(
                    "SELECT * FROM users WHERE is_active = 1 ORDER BY username"
                )

            rows = cursor.fetchall()
            conn.close()

            return [User.from_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Error listing users: {e}")
            return []

    @staticmethod
    def count_admins() -> int:
        """Count the number of active admin users."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) as count FROM users WHERE role = ? AND is_active = 1",
                (UserRole.ADMIN.value,),
            )
            result = cursor.fetchone()
            conn.close()
            return result["count"] if result else 0
        except Exception as e:
            logger.error(f"Error counting admins: {e}")
            return 0


def _ensure_users_table() -> None:
    """Ensure the users table exists in the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'viewer',
                created_at REAL NOT NULL,
                last_login REAL,
                is_active INTEGER DEFAULT 1,
                preferences TEXT
            )
        """)

        # Add preferences column if it doesn't exist (for existing databases)
        cursor.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in cursor.fetchall()]
        if "preferences" not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN preferences TEXT")
            logger.info("Added preferences column to users table")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")

        conn.commit()
        conn.close()
        logger.debug("Users table verified/created")
    except Exception as e:
        logger.error(f"Error ensuring users table: {e}")
        raise


def _ensure_default_admin() -> None:
    """Ensure a default admin user exists if no users exist."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) as count FROM users")
        result = cursor.fetchone()
        conn.close()

        if result and result["count"] == 0:
            # Generate a random password for the default admin
            default_password = secrets.token_urlsafe(16)

            admin = AuthService.create_user(
                username="admin",
                password=default_password,
                role=UserRole.ADMIN,
            )

            if admin:
                logger.warning("=" * 60)
                logger.warning("DEFAULT ADMIN USER CREATED")
                logger.warning("  Username: admin")
                logger.warning(f"  Password: {default_password}")
                logger.warning("  Please change this password immediately!")
                logger.warning("=" * 60)

                # Also print to stdout for visibility
                print("=" * 60)
                print("DEFAULT ADMIN USER CREATED")
                print("  Username: admin")
                print(f"  Password: {default_password}")
                print("  Please change this password immediately!")
                print("=" * 60)
    except Exception as e:
        logger.warning(f"Error checking for default admin: {e}")
