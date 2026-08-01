"""
PAX ID profile repository — nicknames/notes for WiFi/BLE addresses seen by Paxcounter.

Profiles are keyed by normalized MAC/BSSID/BLE address (aa:bb:cc:dd:ee:ff).
Kind (wifi_client / wifi_ap / ble) is not part of the key; the same ID can be
seen under different radio kinds over time.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from ..utils.paxcount_decode import format_mac
from .connection import get_db_connection

logger = logging.getLogger(__name__)


def _ensure_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pax_mac_profiles (
            mac TEXT PRIMARY KEY,
            nickname TEXT,
            notes TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pax_mac_profiles_nickname
        ON pax_mac_profiles(nickname COLLATE NOCASE)
        """
    )


def init_pax_profile_tables() -> None:
    """Create PAX profile tables if needed."""
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.warning("Could not initialize pax profile tables: %s", e)
        return
    try:
        cursor = conn.cursor()
        _ensure_table(cursor)
        conn.commit()
    finally:
        conn.close()
    logger.info("PAX profile tables initialized")


def _row_to_profile(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "mac": row["mac"],
        "nickname": row["nickname"],
        "notes": row["notes"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


class PaxProfileRepository:
    """CRUD for operator-assigned nicknames on Paxcounter sighting IDs."""

    @staticmethod
    def list_profiles(*, q: str | None = None) -> list[dict[str, Any]]:
        """Return all profiles, optionally filtered by mac/nickname/notes search."""
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            _ensure_table(cursor)
            if q and q.strip():
                like = f"%{q.strip()}%"
                cursor.execute(
                    """
                    SELECT mac, nickname, notes, created_at, updated_at
                    FROM pax_mac_profiles
                    WHERE mac LIKE ? COLLATE NOCASE
                       OR IFNULL(nickname, '') LIKE ? COLLATE NOCASE
                       OR IFNULL(notes, '') LIKE ? COLLATE NOCASE
                    ORDER BY
                        CASE WHEN nickname IS NULL OR nickname = '' THEN 1 ELSE 0 END,
                        nickname COLLATE NOCASE,
                        mac
                    """,
                    (like, like, like),
                )
            else:
                cursor.execute(
                    """
                    SELECT mac, nickname, notes, created_at, updated_at
                    FROM pax_mac_profiles
                    ORDER BY
                        CASE WHEN nickname IS NULL OR nickname = '' THEN 1 ELSE 0 END,
                        nickname COLLATE NOCASE,
                        mac
                    """
                )
            return [_row_to_profile(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    @staticmethod
    def get_profiles_by_macs(macs: list[str]) -> dict[str, dict[str, Any]]:
        """Return ``{mac: profile}`` for the given normalized MAC list."""
        normalized = [m for m in (format_mac(x) for x in macs) if m]
        if not normalized:
            return {}
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            _ensure_table(cursor)
            placeholders = ",".join("?" for _ in normalized)
            cursor.execute(
                f"""
                SELECT mac, nickname, notes, created_at, updated_at
                FROM pax_mac_profiles
                WHERE mac IN ({placeholders})
                """,
                normalized,
            )
            return {row["mac"]: _row_to_profile(row) for row in cursor.fetchall()}
        finally:
            conn.close()

    @staticmethod
    def get_profile(mac: str) -> dict[str, Any] | None:
        normalized = format_mac(mac)
        if not normalized:
            return None
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            _ensure_table(cursor)
            cursor.execute(
                """
                SELECT mac, nickname, notes, created_at, updated_at
                FROM pax_mac_profiles
                WHERE mac = ?
                """,
                (normalized,),
            )
            row = cursor.fetchone()
            return _row_to_profile(row) if row else None
        finally:
            conn.close()

    @staticmethod
    def upsert_profile(
        mac: str,
        *,
        nickname: str | None = None,
        notes: str | None = None,
    ) -> dict[str, Any]:
        """
        Create or update a profile.

        Empty nickname + empty notes deletes the profile row.
        """
        normalized = format_mac(mac)
        if not normalized:
            return {"success": False, "error": "Invalid MAC / ID"}

        nick = (nickname or "").strip() or None
        note = (notes or "").strip() or None
        now = time.time()

        if nick is None and note is None:
            deleted = PaxProfileRepository.delete_profile(normalized)
            return {
                "success": True,
                "deleted": deleted,
                "profile": None,
            }

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            _ensure_table(cursor)
            cursor.execute(
                """
                INSERT INTO pax_mac_profiles (mac, nickname, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(mac) DO UPDATE SET
                    nickname = excluded.nickname,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (normalized, nick, note, now, now),
            )
            conn.commit()
            conn.close()
            profile = PaxProfileRepository.get_profile(normalized)
            return {"success": True, "deleted": False, "profile": profile}
        except Exception as e:
            logger.exception("Failed to upsert pax profile for %s", normalized)
            return {"success": False, "error": str(e)}

    @staticmethod
    def delete_profile(mac: str) -> bool:
        normalized = format_mac(mac)
        if not normalized:
            return False
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            _ensure_table(cursor)
            cursor.execute("DELETE FROM pax_mac_profiles WHERE mac = ?", (normalized,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
