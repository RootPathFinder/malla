"""
Repository for background job database operations.

Handles job tracking, queuing, and status management for long-running admin operations.
"""

import json
import logging
import time
from enum import Enum
from typing import Any

from .connection import get_db_connection

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Status of a background job."""

    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(str, Enum):
    """Type of background job."""

    BACKUP = "backup"
    RESTORE = "restore"
    BULK_COMMAND = "bulk_command"
    CONFIG_DEPLOY = "config_deploy"


# Job types that conflict with each other (same node)
CONFLICTING_JOB_TYPES = {
    JobType.BACKUP: [JobType.BACKUP, JobType.RESTORE, JobType.CONFIG_DEPLOY],
    JobType.RESTORE: [JobType.BACKUP, JobType.RESTORE, JobType.CONFIG_DEPLOY],
    JobType.CONFIG_DEPLOY: [JobType.BACKUP, JobType.RESTORE, JobType.CONFIG_DEPLOY],
    JobType.BULK_COMMAND: [JobType.BULK_COMMAND],
}


def init_job_tables() -> None:
    """Initialize job-related database tables."""
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.warning(f"Could not initialize job tables: {e}")
        return

    cursor = conn.cursor()

    # Main jobs table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS background_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            target_node_id INTEGER,
            job_name TEXT NOT NULL,
            job_data TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            progress INTEGER DEFAULT 0,
            progress_message TEXT,
            progress_phase TEXT,
            progress_current INTEGER DEFAULT 0,
            progress_total INTEGER DEFAULT 0,
            result_data TEXT,
            error_message TEXT,
            cancel_requested INTEGER DEFAULT 0,
            created_at REAL NOT NULL,
            started_at REAL,
            completed_at REAL,
            updated_at REAL NOT NULL
        )
    """)

    # Add cancel_requested column if it doesn't exist (for existing databases)
    try:
        cursor.execute(
            "ALTER TABLE background_jobs ADD COLUMN cancel_requested INTEGER DEFAULT 0"
        )
        conn.commit()
    except Exception:
        pass  # Column already exists

    # Job progress log for detailed history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_progress_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            timestamp REAL NOT NULL,
            progress INTEGER,
            message TEXT,
            phase TEXT,
            details TEXT,
            FOREIGN KEY (job_id) REFERENCES background_jobs(id) ON DELETE CASCADE
        )
    """)

    # Indexes for efficient queries
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_status ON background_jobs(status, created_at DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_node ON background_jobs(target_node_id, created_at DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_type ON background_jobs(job_type, status)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_job_progress_job ON job_progress_log(job_id, timestamp DESC)"
    )

    conn.commit()
    conn.close()
    logger.info("Job tables initialized")


class JobRepository:
    """Repository for background job database operations."""

    @staticmethod
    def create_job(
        job_type: JobType,
        job_name: str,
        job_data: dict[str, Any],
        target_node_id: int | None = None,
    ) -> int:
        """
        Create a new background job.

        Args:
            job_type: Type of job (backup, restore, etc.)
            job_name: Human-readable name for the job
            job_data: JSON-serializable job parameters
            target_node_id: Optional target node ID for node-specific jobs

        Returns:
            The ID of the created job
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()
        cursor.execute(
            """
            INSERT INTO background_jobs
            (job_type, target_node_id, job_name, job_data, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_type.value,
                target_node_id,
                job_name,
                json.dumps(job_data),
                JobStatus.QUEUED.value,
                now,
                now,
            ),
        )

        job_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(f"Created job {job_id}: {job_type.value} - {job_name}")
        assert job_id is not None
        return job_id

    @staticmethod
    def get_job(job_id: int) -> dict[str, Any] | None:
        """Get a job by ID."""
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, job_type, target_node_id, job_name, job_data, status,
                   progress, progress_message, progress_phase, progress_current,
                   progress_total, result_data, error_message, created_at,
                   started_at, completed_at, updated_at
            FROM background_jobs
            WHERE id = ?
            """,
            (job_id,),
        )

        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        return {
            "id": row[0],
            "job_type": row[1],
            "target_node_id": row[2],
            "job_name": row[3],
            "job_data": json.loads(row[4]) if row[4] else {},
            "status": row[5],
            "progress": row[6],
            "progress_message": row[7],
            "progress_phase": row[8],
            "progress_current": row[9],
            "progress_total": row[10],
            "result_data": json.loads(row[11]) if row[11] else None,
            "error_message": row[12],
            "created_at": row[13],
            "started_at": row[14],
            "completed_at": row[15],
            "updated_at": row[16],
        }

    @staticmethod
    def get_jobs(
        status: JobStatus | None = None,
        job_type: JobType | None = None,
        target_node_id: int | None = None,
        limit: int = 50,
        include_completed: bool = True,
    ) -> list[dict[str, Any]]:
        """Get jobs with optional filtering."""
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT id, job_type, target_node_id, job_name, job_data, status,
                   progress, progress_message, progress_phase, progress_current,
                   progress_total, result_data, error_message, created_at,
                   started_at, completed_at, updated_at
            FROM background_jobs
            WHERE 1=1
        """
        params: list[Any] = []

        if status:
            query += " AND status = ?"
            params.append(status.value)
        elif not include_completed:
            query += " AND status NOT IN (?, ?)"
            params.extend([JobStatus.COMPLETED.value, JobStatus.FAILED.value])

        if job_type:
            query += " AND job_type = ?"
            params.append(job_type.value)

        if target_node_id:
            query += " AND target_node_id = ?"
            params.append(target_node_id)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        jobs = []
        for row in rows:
            jobs.append(
                {
                    "id": row[0],
                    "job_type": row[1],
                    "target_node_id": row[2],
                    "job_name": row[3],
                    "job_data": json.loads(row[4]) if row[4] else {},
                    "status": row[5],
                    "progress": row[6],
                    "progress_message": row[7],
                    "progress_phase": row[8],
                    "progress_current": row[9],
                    "progress_total": row[10],
                    "result_data": json.loads(row[11]) if row[11] else None,
                    "error_message": row[12],
                    "created_at": row[13],
                    "started_at": row[14],
                    "completed_at": row[15],
                    "updated_at": row[16],
                }
            )

        return jobs

    @staticmethod
    def get_active_jobs(target_node_id: int | None = None) -> list[dict[str, Any]]:
        """Get all queued or running jobs, optionally for a specific node."""
        conn = get_db_connection()
        cursor = conn.cursor()

        if target_node_id:
            cursor.execute(
                """
                SELECT id, job_type, target_node_id, job_name, status, progress,
                       progress_message, created_at
                FROM background_jobs
                WHERE status IN (?, ?) AND target_node_id = ?
                ORDER BY created_at ASC
                """,
                (JobStatus.QUEUED.value, JobStatus.RUNNING.value, target_node_id),
            )
        else:
            cursor.execute(
                """
                SELECT id, job_type, target_node_id, job_name, status, progress,
                       progress_message, created_at
                FROM background_jobs
                WHERE status IN (?, ?)
                ORDER BY created_at ASC
                """,
                (JobStatus.QUEUED.value, JobStatus.RUNNING.value),
            )

        rows = cursor.fetchall()
        conn.close()

        return [
            {
                "id": row[0],
                "job_type": row[1],
                "target_node_id": row[2],
                "job_name": row[3],
                "status": row[4],
                "progress": row[5],
                "progress_message": row[6],
                "created_at": row[7],
            }
            for row in rows
        ]

    @staticmethod
    def has_conflicting_job(
        job_type: JobType,
        target_node_id: int | None,
    ) -> dict[str, Any] | None:
        """
        Check if there's a conflicting job already running or queued.

        Returns the conflicting job if one exists, None otherwise.
        """
        if target_node_id is None:
            return None

        conflicting_types = CONFLICTING_JOB_TYPES.get(job_type, [])
        if not conflicting_types:
            return None

        conn = get_db_connection()
        cursor = conn.cursor()

        placeholders = ",".join("?" * len(conflicting_types))
        cursor.execute(
            f"""
            SELECT id, job_type, job_name, status, created_at
            FROM background_jobs
            WHERE target_node_id = ?
              AND status IN (?, ?)
              AND job_type IN ({placeholders})
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (
                target_node_id,
                JobStatus.QUEUED.value,
                JobStatus.RUNNING.value,
                *[t.value for t in conflicting_types],
            ),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                "id": row[0],
                "job_type": row[1],
                "job_name": row[2],
                "status": row[3],
                "created_at": row[4],
            }
        return None

    @staticmethod
    def get_next_queued_job() -> dict[str, Any] | None:
        """Get the next queued job that's ready to run (no conflicts)."""
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all queued jobs ordered by creation time
        cursor.execute(
            """
            SELECT id, job_type, target_node_id, job_name, job_data
            FROM background_jobs
            WHERE status = ?
            ORDER BY created_at ASC
            """,
            (JobStatus.QUEUED.value,),
        )

        queued_jobs = cursor.fetchall()

        # Get all running jobs
        cursor.execute(
            """
            SELECT job_type, target_node_id
            FROM background_jobs
            WHERE status = ?
            """,
            (JobStatus.RUNNING.value,),
        )

        running_jobs = cursor.fetchall()
        conn.close()

        # Build set of (node_id, conflicting_types) for running jobs
        running_conflicts: dict[int, set[str]] = {}
        for job_type_str, node_id in running_jobs:
            if node_id is not None:
                try:
                    job_type = JobType(job_type_str)
                    conflicting = CONFLICTING_JOB_TYPES.get(job_type, [])
                    if node_id not in running_conflicts:
                        running_conflicts[node_id] = set()
                    running_conflicts[node_id].update(t.value for t in conflicting)
                except ValueError:
                    pass

        # Find first queued job that doesn't conflict
        for job_id, job_type_str, node_id, job_name, job_data in queued_jobs:
            if node_id is None:
                # Jobs without target node don't conflict
                return {
                    "id": job_id,
                    "job_type": job_type_str,
                    "target_node_id": node_id,
                    "job_name": job_name,
                    "job_data": json.loads(job_data) if job_data else {},
                }

            # Check if this job's type conflicts with any running job on same node
            if node_id in running_conflicts:
                if job_type_str in running_conflicts[node_id]:
                    continue  # Skip this job, it conflicts

            return {
                "id": job_id,
                "job_type": job_type_str,
                "target_node_id": node_id,
                "job_name": job_name,
                "job_data": json.loads(job_data) if job_data else {},
            }

        return None

    @staticmethod
    def update_job_status(
        job_id: int,
        status: JobStatus,
        error_message: str | None = None,
    ) -> None:
        """Update job status."""
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()
        if status == JobStatus.RUNNING:
            cursor.execute(
                """
                UPDATE background_jobs
                SET status = ?, started_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (status.value, now, now, job_id),
            )
        elif status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            cursor.execute(
                """
                UPDATE background_jobs
                SET status = ?, completed_at = ?, updated_at = ?, error_message = ?
                WHERE id = ?
                """,
                (status.value, now, now, error_message, job_id),
            )
        else:
            cursor.execute(
                """
                UPDATE background_jobs
                SET status = ?, updated_at = ?
                WHERE id = ?
                """,
                (status.value, now, job_id),
            )

        conn.commit()
        conn.close()
        logger.info(f"Updated job {job_id} status to {status.value}")

    @staticmethod
    def update_job_progress(
        job_id: int,
        progress: int,
        message: str | None = None,
        phase: str | None = None,
        current: int | None = None,
        total: int | None = None,
        log_entry: bool = True,
    ) -> None:
        """Update job progress."""
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()
        cursor.execute(
            """
            UPDATE background_jobs
            SET progress = ?, progress_message = ?, progress_phase = ?,
                progress_current = COALESCE(?, progress_current),
                progress_total = COALESCE(?, progress_total),
                updated_at = ?
            WHERE id = ?
            """,
            (progress, message, phase, current, total, now, job_id),
        )

        # Log progress entry
        if log_entry:
            cursor.execute(
                """
                INSERT INTO job_progress_log (job_id, timestamp, progress, message, phase)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, now, progress, message, phase),
            )

        conn.commit()
        conn.close()

    @staticmethod
    def complete_job(
        job_id: int,
        success: bool,
        result_data: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        """Mark a job as completed or failed."""
        conn = get_db_connection()
        cursor = conn.cursor()

        now = time.time()
        status = JobStatus.COMPLETED if success else JobStatus.FAILED

        cursor.execute(
            """
            UPDATE background_jobs
            SET status = ?, progress = ?, completed_at = ?, updated_at = ?,
                result_data = ?, error_message = ?
            WHERE id = ?
            """,
            (
                status.value,
                100 if success else None,
                now,
                now,
                json.dumps(result_data) if result_data else None,
                error_message,
                job_id,
            ),
        )

        # Add final log entry
        cursor.execute(
            """
            INSERT INTO job_progress_log (job_id, timestamp, progress, message, phase)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                job_id,
                now,
                100 if success else None,
                "Completed successfully" if success else f"Failed: {error_message}",
                "complete",
            ),
        )

        conn.commit()
        conn.close()
        logger.info(
            f"Job {job_id} {'completed' if success else 'failed'}"
            + (f": {error_message}" if error_message else "")
        )

    @staticmethod
    def cancel_job(job_id: int) -> bool:
        """
        Cancel a queued job.

        Returns True if the job was cancelled, False if it was already running/completed.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Only cancel if queued
        cursor.execute(
            """
            UPDATE background_jobs
            SET status = ?, completed_at = ?, updated_at = ?, error_message = ?
            WHERE id = ? AND status = ?
            """,
            (
                JobStatus.CANCELLED.value,
                time.time(),
                time.time(),
                "Cancelled by user",
                job_id,
                JobStatus.QUEUED.value,
            ),
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Job {job_id} cancelled")
            return True
        return False

    @staticmethod
    def request_cancel_running_job(job_id: int) -> bool:
        """
        Request cancellation of a running job.

        Sets the cancel_requested flag so the job handler can check and stop.
        Returns True if the request was set, False if job is not running.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Only request cancel if running
        cursor.execute(
            """
            UPDATE background_jobs
            SET cancel_requested = 1, updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (
                time.time(),
                job_id,
                JobStatus.RUNNING.value,
            ),
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Cancellation requested for running job {job_id}")
            return True
        return False

    @staticmethod
    def force_cancel_job(job_id: int) -> bool:
        """
        Force cancel a job regardless of its status.

        This is useful for orphaned jobs that are stuck in 'running' state
        after a server restart. Directly sets the job to cancelled status.

        Returns True if the job was cancelled, False if not found.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Force cancel any job that's not already completed/cancelled/failed
        cursor.execute(
            """
            UPDATE background_jobs
            SET status = ?, completed_at = ?, updated_at = ?,
                error_message = ?, cancel_requested = 1
            WHERE id = ? AND status IN (?, ?, ?)
            """,
            (
                JobStatus.CANCELLED.value,
                time.time(),
                time.time(),
                "Force cancelled (job was orphaned or unresponsive)",
                job_id,
                JobStatus.QUEUED.value,
                JobStatus.RUNNING.value,
                JobStatus.PAUSED.value,
            ),
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Job {job_id} force cancelled")
            return True
        return False

    @staticmethod
    def cleanup_orphaned_jobs(max_running_time_seconds: int = 3600) -> int:
        """
        Clean up jobs that have been running for too long (likely orphaned).

        This should be called on server startup to clean up any jobs that
        were running when the server was stopped.

        Args:
            max_running_time_seconds: Jobs running longer than this are considered orphaned

        Returns:
            Number of jobs cleaned up
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cutoff_time = time.time() - max_running_time_seconds

        # Find orphaned running jobs (running for too long)
        cursor.execute(
            """
            UPDATE background_jobs
            SET status = ?, completed_at = ?, updated_at = ?,
                error_message = ?
            WHERE status = ? AND started_at < ?
            """,
            (
                JobStatus.FAILED.value,
                time.time(),
                time.time(),
                "Job failed: Server was restarted while job was running",
                JobStatus.RUNNING.value,
                cutoff_time,
            ),
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Cleaned up {rows_affected} orphaned running jobs")

        return rows_affected

    @staticmethod
    def is_cancel_requested(job_id: int) -> bool:
        """
        Check if cancellation has been requested for a job.

        Job handlers should call this periodically to check for cancellation.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT cancel_requested FROM background_jobs WHERE id = ?
            """,
            (job_id,),
        )

        row = cursor.fetchone()
        conn.close()

        return bool(row and row[0])

    @staticmethod
    def pause_job(job_id: int) -> bool:
        """
        Pause a queued job.

        Returns True if the job was paused, False otherwise.
        Running jobs cannot be paused - only queued jobs.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Only pause if queued
        cursor.execute(
            """
            UPDATE background_jobs
            SET status = ?, updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (
                JobStatus.PAUSED.value,
                time.time(),
                job_id,
                JobStatus.QUEUED.value,
            ),
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Job {job_id} paused")
            return True
        return False

    @staticmethod
    def resume_job(job_id: int) -> bool:
        """
        Resume a paused job (put it back in queue).

        Returns True if the job was resumed, False otherwise.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        # Only resume if paused
        cursor.execute(
            """
            UPDATE background_jobs
            SET status = ?, updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (
                JobStatus.QUEUED.value,
                time.time(),
                job_id,
                JobStatus.PAUSED.value,
            ),
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Job {job_id} resumed")
            return True
        return False

    @staticmethod
    def get_job_progress_log(job_id: int, limit: int = 100) -> list[dict[str, Any]]:
        """Get progress log entries for a job."""
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, timestamp, progress, message, phase, details
            FROM job_progress_log
            WHERE job_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (job_id, limit),
        )

        rows = cursor.fetchall()
        conn.close()

        return [
            {
                "id": row[0],
                "timestamp": row[1],
                "progress": row[2],
                "message": row[3],
                "phase": row[4],
                "details": json.loads(row[5]) if row[5] else None,
            }
            for row in rows
        ]

    @staticmethod
    def cleanup_old_jobs(max_age_days: int = 30) -> int:
        """
        Delete completed/failed jobs older than max_age_days.

        Returns the number of jobs deleted.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        cutoff = time.time() - (max_age_days * 86400)

        # Delete progress logs first (foreign key)
        cursor.execute(
            """
            DELETE FROM job_progress_log
            WHERE job_id IN (
                SELECT id FROM background_jobs
                WHERE status IN (?, ?, ?)
                  AND completed_at < ?
            )
            """,
            (
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
                cutoff,
            ),
        )

        # Delete jobs
        cursor.execute(
            """
            DELETE FROM background_jobs
            WHERE status IN (?, ?, ?)
              AND completed_at < ?
            """,
            (
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
                cutoff,
            ),
        )

        deleted = cursor.rowcount
        conn.commit()
        conn.close()

        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old jobs")

        return deleted

    @staticmethod
    def get_queue_position(job_id: int) -> int | None:
        """Get the queue position of a job (1-based). Returns None if not queued."""
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get the job's created_at
        cursor.execute(
            "SELECT created_at, status FROM background_jobs WHERE id = ?", (job_id,)
        )
        row = cursor.fetchone()

        if not row or row[1] != JobStatus.QUEUED.value:
            conn.close()
            return None

        created_at = row[0]

        # Count jobs ahead in queue
        cursor.execute(
            """
            SELECT COUNT(*) FROM background_jobs
            WHERE status = ? AND created_at < ?
            """,
            (JobStatus.QUEUED.value, created_at),
        )

        count = cursor.fetchone()[0]
        conn.close()

        return count + 1  # 1-based position
