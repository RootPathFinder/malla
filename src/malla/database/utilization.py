"""Channel utilization analytics from telemetry_data + packet_history."""

from __future__ import annotations

import logging
import time
from typing import Any

from ..utils.formatting import format_node_display_name
from .connection import get_db_connection

logger = logging.getLogger(__name__)

# User-facing threshold: unexplained util above this is worth investigating.
DEFAULT_HIGH_UTIL_PCT = 30.0


def _clamp_hours(hours: int | float | None, default: int = 24) -> int:
    try:
        h = int(hours if hours is not None else default)
    except (TypeError, ValueError):
        h = default
    return min(max(h, 1), 168)


def _table_exists(cursor: Any, name: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    )
    return cursor.fetchone() is not None


def _node_label(node_id: int, long_name: str | None, short_name: str | None) -> str:
    return format_node_display_name(
        node_id,
        long_name=long_name,
        short_name=short_name,
        hex_id=f"!{int(node_id):08x}",
    )


class UtilizationRepository:
    """Queries for mesh channel utilization investigation."""

    @staticmethod
    def get_summary(
        hours: int = 24, high_util_pct: float = DEFAULT_HIGH_UTIL_PCT
    ) -> dict[str, Any]:
        hours = _clamp_hours(hours)
        cutoff = time.time() - hours * 3600
        empty = {
            "hours": hours,
            "high_util_pct": high_util_pct,
            "avg_channel_utilization": None,
            "max_channel_utilization": None,
            "avg_air_util_tx": None,
            "max_air_util_tx": None,
            "reporting_nodes": 0,
            "nodes_over_threshold": 0,
            "packet_count": 0,
            "packets_per_hour": 0.0,
            "has_telemetry": False,
            "has_packets": False,
        }
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            has_telem = _table_exists(cursor, "telemetry_data")
            has_packets = _table_exists(cursor, "packet_history")
            empty["has_telemetry"] = has_telem
            empty["has_packets"] = has_packets

            if has_telem:
                cursor.execute(
                    """
                    SELECT
                        AVG(channel_utilization) AS avg_ch,
                        MAX(channel_utilization) AS max_ch,
                        AVG(air_util_tx) AS avg_tx,
                        MAX(air_util_tx) AS max_tx,
                        COUNT(DISTINCT node_id) AS node_cnt
                    FROM telemetry_data
                    WHERE timestamp > ?
                      AND channel_utilization IS NOT NULL
                    """,
                    (cutoff,),
                )
                row = cursor.fetchone()
                if row:
                    empty["avg_channel_utilization"] = row["avg_ch"]
                    empty["max_channel_utilization"] = row["max_ch"]
                    empty["avg_air_util_tx"] = row["avg_tx"]
                    empty["max_air_util_tx"] = row["max_tx"]
                    empty["reporting_nodes"] = int(row["node_cnt"] or 0)

                # Latest reading per node over threshold
                cursor.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM (
                        SELECT t.node_id, t.channel_utilization
                        FROM telemetry_data t
                        INNER JOIN (
                            SELECT node_id, MAX(timestamp) AS max_ts
                            FROM telemetry_data
                            WHERE timestamp > ?
                              AND channel_utilization IS NOT NULL
                            GROUP BY node_id
                        ) latest
                          ON t.node_id = latest.node_id
                         AND t.timestamp = latest.max_ts
                        WHERE t.channel_utilization >= ?
                    )
                    """,
                    (cutoff, float(high_util_pct)),
                )
                over = cursor.fetchone()
                empty["nodes_over_threshold"] = int((over["cnt"] if over else 0) or 0)

            if has_packets:
                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM packet_history WHERE timestamp > ?",
                    (cutoff,),
                )
                pkt = cursor.fetchone()
                count = int((pkt["cnt"] if pkt else 0) or 0)
                empty["packet_count"] = count
                empty["packets_per_hour"] = round(count / float(hours), 1)

            conn.close()
            return empty
        except Exception as e:
            logger.error("utilization summary failed: %s", e, exc_info=True)
            return empty

    @staticmethod
    def get_nodes(
        hours: int = 24, high_util_pct: float = DEFAULT_HIGH_UTIL_PCT
    ) -> list[dict[str, Any]]:
        hours = _clamp_hours(hours)
        cutoff = time.time() - hours * 3600
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            if not _table_exists(cursor, "telemetry_data"):
                conn.close()
                return []

            cursor.execute(
                """
                SELECT
                    t.node_id,
                    ni.long_name,
                    ni.short_name,
                    ni.role,
                    AVG(t.channel_utilization) AS avg_chutil,
                    MAX(t.channel_utilization) AS max_chutil,
                    AVG(t.air_util_tx) AS avg_air_tx,
                    MAX(t.air_util_tx) AS max_air_tx,
                    COUNT(*) AS sample_count,
                    MAX(t.timestamp) AS last_seen
                FROM telemetry_data t
                LEFT JOIN node_info ni ON t.node_id = ni.node_id
                WHERE t.timestamp > ?
                  AND t.channel_utilization IS NOT NULL
                GROUP BY t.node_id
                ORDER BY max_chutil DESC, avg_chutil DESC
                """,
                (cutoff,),
            )
            rows = cursor.fetchall()

            # Latest reading per node
            cursor.execute(
                """
                SELECT t.node_id, t.channel_utilization, t.air_util_tx, t.timestamp
                FROM telemetry_data t
                INNER JOIN (
                    SELECT node_id, MAX(timestamp) AS max_ts
                    FROM telemetry_data
                    WHERE timestamp > ?
                      AND channel_utilization IS NOT NULL
                    GROUP BY node_id
                ) latest
                  ON t.node_id = latest.node_id AND t.timestamp = latest.max_ts
                """,
                (cutoff,),
            )
            latest_by_node = {
                int(r["node_id"]): {
                    "latest_chutil": r["channel_utilization"],
                    "latest_air_tx": r["air_util_tx"],
                    "latest_ts": r["timestamp"],
                }
                for r in cursor.fetchall()
            }

            packet_counts: dict[int, int] = {}
            if _table_exists(cursor, "packet_history"):
                cursor.execute(
                    """
                    SELECT from_node_id AS node_id, COUNT(*) AS cnt
                    FROM packet_history
                    WHERE timestamp > ? AND from_node_id IS NOT NULL
                    GROUP BY from_node_id
                    """,
                    (cutoff,),
                )
                packet_counts = {
                    int(r["node_id"]): int(r["cnt"] or 0) for r in cursor.fetchall()
                }

            conn.close()

            nodes: list[dict[str, Any]] = []
            for row in rows:
                node_id = int(row["node_id"])
                latest = latest_by_node.get(node_id, {})
                avg_ch = row["avg_chutil"]
                max_ch = row["max_chutil"]
                latest_ch = latest.get("latest_chutil")
                nodes.append(
                    {
                        "node_id": node_id,
                        "node_hex": f"!{node_id:08x}",
                        "name": _node_label(
                            node_id, row["long_name"], row["short_name"]
                        ),
                        "long_name": row["long_name"],
                        "short_name": row["short_name"],
                        "role": row["role"],
                        "avg_channel_utilization": avg_ch,
                        "max_channel_utilization": max_ch,
                        "latest_channel_utilization": latest_ch,
                        "avg_air_util_tx": row["avg_air_tx"],
                        "max_air_util_tx": row["max_air_tx"],
                        "latest_air_util_tx": latest.get("latest_air_tx"),
                        "sample_count": int(row["sample_count"] or 0),
                        "packet_count": packet_counts.get(node_id, 0),
                        "last_seen": row["last_seen"],
                        "over_threshold": (
                            (latest_ch is not None and latest_ch >= high_util_pct)
                            or (max_ch is not None and max_ch >= high_util_pct)
                        ),
                    }
                )
            return nodes
        except Exception as e:
            logger.error("utilization nodes failed: %s", e, exc_info=True)
            return []

    @staticmethod
    def get_timeline(hours: int = 24, bucket_mins: int | None = None) -> dict[str, Any]:
        hours = _clamp_hours(hours)
        if bucket_mins is None:
            if hours <= 6:
                bucket_mins = 5
            elif hours <= 24:
                bucket_mins = 15
            elif hours <= 72:
                bucket_mins = 30
            else:
                bucket_mins = 60
        bucket_mins = min(max(int(bucket_mins), 1), 360)
        bucket_secs = bucket_mins * 60
        cutoff = time.time() - hours * 3600

        result: dict[str, Any] = {
            "hours": hours,
            "bucket_mins": bucket_mins,
            "buckets": [],
        }
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            util_by_bucket: dict[int, dict[str, Any]] = {}

            if _table_exists(cursor, "telemetry_data"):
                cursor.execute(
                    """
                    SELECT
                        CAST(timestamp / ? AS INTEGER) AS bucket,
                        AVG(channel_utilization) AS avg_ch,
                        MAX(channel_utilization) AS max_ch,
                        AVG(air_util_tx) AS avg_tx,
                        COUNT(DISTINCT node_id) AS node_cnt
                    FROM telemetry_data
                    WHERE timestamp > ?
                      AND channel_utilization IS NOT NULL
                    GROUP BY bucket
                    ORDER BY bucket
                    """,
                    (bucket_secs, cutoff),
                )
                for row in cursor.fetchall():
                    b = int(row["bucket"])
                    util_by_bucket[b] = {
                        "bucket": b,
                        "timestamp": b * bucket_secs,
                        "avg_channel_utilization": row["avg_ch"],
                        "max_channel_utilization": row["max_ch"],
                        "avg_air_util_tx": row["avg_tx"],
                        "reporting_nodes": int(row["node_cnt"] or 0),
                        "packet_count": 0,
                    }

            if _table_exists(cursor, "packet_history"):
                cursor.execute(
                    """
                    SELECT
                        CAST(timestamp / ? AS INTEGER) AS bucket,
                        COUNT(*) AS cnt
                    FROM packet_history
                    WHERE timestamp > ?
                    GROUP BY bucket
                    ORDER BY bucket
                    """,
                    (bucket_secs, cutoff),
                )
                for row in cursor.fetchall():
                    b = int(row["bucket"])
                    entry = util_by_bucket.get(b)
                    if entry is None:
                        entry = {
                            "bucket": b,
                            "timestamp": b * bucket_secs,
                            "avg_channel_utilization": None,
                            "max_channel_utilization": None,
                            "avg_air_util_tx": None,
                            "reporting_nodes": 0,
                            "packet_count": 0,
                        }
                        util_by_bucket[b] = entry
                    entry["packet_count"] = int(row["cnt"] or 0)

            conn.close()
            result["buckets"] = [
                util_by_bucket[k] for k in sorted(util_by_bucket.keys())
            ]
            return result
        except Exception as e:
            logger.error("utilization timeline failed: %s", e, exc_info=True)
            return result

    @staticmethod
    def get_talkers(hours: int = 24, limit: int = 25) -> dict[str, Any]:
        """Top packet sources and portnum breakdown — correlates with util spikes."""
        hours = _clamp_hours(hours)
        limit = min(max(int(limit), 1), 100)
        cutoff = time.time() - hours * 3600
        result: dict[str, Any] = {
            "hours": hours,
            "top_nodes": [],
            "by_portnum": [],
            "total_packets": 0,
        }
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            if not _table_exists(cursor, "packet_history"):
                conn.close()
                return result

            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM packet_history WHERE timestamp > ?",
                (cutoff,),
            )
            result["total_packets"] = int(cursor.fetchone()["cnt"] or 0)

            cursor.execute(
                """
                SELECT
                    ph.from_node_id AS node_id,
                    ni.long_name,
                    ni.short_name,
                    ni.role,
                    COUNT(*) AS packet_count
                FROM packet_history ph
                LEFT JOIN node_info ni ON ph.from_node_id = ni.node_id
                WHERE ph.timestamp > ?
                  AND ph.from_node_id IS NOT NULL
                GROUP BY ph.from_node_id
                ORDER BY packet_count DESC
                LIMIT ?
                """,
                (cutoff, limit),
            )
            top = []
            for row in cursor.fetchall():
                node_id = int(row["node_id"])
                top.append(
                    {
                        "node_id": node_id,
                        "node_hex": f"!{node_id:08x}",
                        "name": _node_label(
                            node_id, row["long_name"], row["short_name"]
                        ),
                        "role": row["role"],
                        "packet_count": int(row["packet_count"] or 0),
                    }
                )
            result["top_nodes"] = top

            cursor.execute(
                """
                SELECT
                    COALESCE(NULLIF(portnum_name, ''), CAST(portnum AS TEXT), 'UNKNOWN')
                        AS port_name,
                    COUNT(*) AS packet_count
                FROM packet_history
                WHERE timestamp > ?
                GROUP BY port_name
                ORDER BY packet_count DESC
                LIMIT 20
                """,
                (cutoff,),
            )
            result["by_portnum"] = [
                {
                    "portnum_name": r["port_name"],
                    "packet_count": int(r["packet_count"] or 0),
                }
                for r in cursor.fetchall()
            ]

            conn.close()
            return result
        except Exception as e:
            logger.error("utilization talkers failed: %s", e, exc_info=True)
            return result
