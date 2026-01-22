"""
Job routes for background job management.

Provides REST API endpoints for querying, monitoring, and managing background jobs.
"""

import logging

from flask import Blueprint, jsonify, request

from ..database.job_repository import JobRepository, JobStatus, JobType
from ..database.repositories import get_db_connection
from ..services.job_service import get_job_service
from ..utils.node_utils import convert_node_id

logger = logging.getLogger(__name__)

job_bp = Blueprint("jobs", __name__)


def _get_node_names_for_ids(node_ids: set) -> dict:
    """Look up node names for a set of node IDs."""
    if not node_ids:
        return {}

    node_names = {}
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT node_id, long_name, short_name FROM node_info")
        for row in cursor.fetchall():
            if row["node_id"] in node_ids:
                node_names[row["node_id"]] = {
                    "long_name": row["long_name"],
                    "short_name": row["short_name"],
                }
        conn.close()
    except Exception:
        pass  # Ignore lookup errors
    return node_names


def _enrich_job_with_node_name(job: dict) -> dict:
    """Add target_node_name and target_node_hex to a job dict."""
    if job and job.get("target_node_id"):
        node_id = job["target_node_id"]
        # Add hex representation
        job["target_node_hex"] = f"!{(node_id & 0xFFFFFFFF):08x}"
        # Look up node name
        node_names = _get_node_names_for_ids({node_id})
        if node_id in node_names:
            node_info = node_names[node_id]
            job["target_node_name"] = (
                node_info.get("long_name") or node_info.get("short_name") or None
            )
    return job


def _enrich_jobs_with_node_names(jobs: list) -> list:
    """Add target_node_name and target_node_hex to a list of job dicts."""
    # Collect unique node IDs
    node_ids = {j["target_node_id"] for j in jobs if j.get("target_node_id")}
    if not node_ids:
        return jobs

    # Batch lookup node names
    node_names = _get_node_names_for_ids(node_ids)

    # Enrich jobs
    for job in jobs:
        if job.get("target_node_id"):
            node_id = job["target_node_id"]
            job["target_node_hex"] = f"!{(node_id & 0xFFFFFFFF):08x}"
            if node_id in node_names:
                node_info = node_names[node_id]
                job["target_node_name"] = (
                    node_info.get("long_name") or node_info.get("short_name") or None
                )

    return jobs


# ============================================================================
# API Routes - Job Management
# ============================================================================


@job_bp.route("/api/jobs")
def api_get_jobs():
    """
    Get list of jobs with optional filtering.

    Query parameters:
        status: Filter by status (queued, running, completed, failed, cancelled)
        job_type: Filter by type (backup, restore, bulk_command, config_deploy)
        node_id: Filter by target node ID
        limit: Maximum number of jobs to return (default 50)
        include_completed: Include completed/failed jobs (default true)
    """
    try:
        status_str = request.args.get("status")
        job_type_str = request.args.get("job_type")
        node_id_str = request.args.get("node_id")
        limit = request.args.get("limit", 50, type=int)
        include_completed = request.args.get("include_completed", "true") == "true"

        status = JobStatus(status_str) if status_str else None
        job_type = JobType(job_type_str) if job_type_str else None
        node_id = convert_node_id(node_id_str) if node_id_str else None

        job_service = get_job_service()
        jobs = job_service.get_jobs(
            status=status,
            job_type=job_type,
            target_node_id=node_id,
            limit=limit,
            include_completed=include_completed,
        )

        # Enrich with node names
        jobs = _enrich_jobs_with_node_names(jobs)

        return jsonify({"jobs": jobs, "count": len(jobs)})

    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {e}"}), 400
    except Exception as e:
        logger.error(f"Error getting jobs: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/active")
def api_get_active_jobs():
    """
    Get all active (queued or running) jobs.

    Query parameters:
        node_id: Filter by target node ID
    """
    try:
        node_id_str = request.args.get("node_id")
        node_id = convert_node_id(node_id_str) if node_id_str else None

        job_service = get_job_service()
        jobs = job_service.get_active_jobs(target_node_id=node_id)

        # Enrich with node names
        jobs = _enrich_jobs_with_node_names(jobs)

        return jsonify({"jobs": jobs, "count": len(jobs)})

    except Exception as e:
        logger.error(f"Error getting active jobs: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/<int:job_id>")
def api_get_job(job_id: int):
    """Get details for a specific job."""
    try:
        job_service = get_job_service()
        job = job_service.get_job(job_id)

        if not job:
            return jsonify({"error": "Job not found"}), 404

        # Add queue position if queued
        if job["status"] == JobStatus.QUEUED.value:
            job["queue_position"] = JobRepository.get_queue_position(job_id)

        # Enrich with node name
        job = _enrich_job_with_node_name(job)

        # Calculate duration if job has timing info
        if job.get("started_at") and job.get("completed_at"):
            job["duration_seconds"] = job["completed_at"] - job["started_at"]
        elif job.get("started_at"):
            import time

            job["elapsed_seconds"] = time.time() - job["started_at"]

        return jsonify(job)

    except Exception as e:
        logger.error(f"Error getting job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/<int:job_id>/progress")
def api_get_job_progress(job_id: int):
    """
    Get progress log for a specific job.

    Query parameters:
        limit: Maximum number of log entries (default 100)
    """
    try:
        limit = request.args.get("limit", 100, type=int)

        job_service = get_job_service()
        job = job_service.get_job(job_id)

        if not job:
            return jsonify({"error": "Job not found"}), 404

        log_entries = job_service.get_job_progress_log(job_id, limit)

        return jsonify(
            {
                "job_id": job_id,
                "status": job["status"],
                "progress": job["progress"],
                "progress_message": job["progress_message"],
                "log_entries": log_entries,
            }
        )

    except Exception as e:
        logger.error(f"Error getting job progress {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/<int:job_id>/cancel", methods=["POST"])
def api_cancel_job(job_id: int):
    """Cancel a queued or running job.

    Query parameters:
        force: If 'true', force cancel even if running (for orphaned jobs)
    """
    try:
        force = request.args.get("force", "false").lower() == "true"
        job_service = get_job_service()
        result = job_service.cancel_job(job_id, force=force)

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 400

    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/<int:job_id>/pause", methods=["POST"])
def api_pause_job(job_id: int):
    """Pause a queued job."""
    try:
        job_service = get_job_service()
        result = job_service.pause_job(job_id)

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 400

    except Exception as e:
        logger.error(f"Error pausing job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/<int:job_id>/resume", methods=["POST"])
def api_resume_job(job_id: int):
    """Resume a paused job."""
    try:
        job_service = get_job_service()
        result = job_service.resume_job(job_id)

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 400

    except Exception as e:
        logger.error(f"Error resuming job {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/status")
def api_job_service_status():
    """Get job service status and summary."""
    try:
        job_service = get_job_service()

        # Get counts by status
        active_jobs = job_service.get_active_jobs()
        running = [j for j in active_jobs if j["status"] == JobStatus.RUNNING.value]
        queued = [j for j in active_jobs if j["status"] == JobStatus.QUEUED.value]

        return jsonify(
            {
                "service_running": job_service.is_running(),
                "running_jobs": len(running),
                "queued_jobs": len(queued),
                "active_jobs": active_jobs,
            }
        )

    except Exception as e:
        logger.error(f"Error getting job service status: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# API Routes - Job Creation
# ============================================================================


@job_bp.route("/api/jobs/backup", methods=["POST"])
def api_queue_backup_job():
    """
    Queue a backup job.

    Request body:
        node_id: Target node ID (required)
        backup_name: Name for the backup (required)
        description: Optional description
    """
    try:
        data = request.get_json() or {}

        node_id_str = data.get("node_id")
        backup_name = data.get("backup_name")
        description = data.get("description", "")

        if not node_id_str:
            return jsonify({"error": "node_id is required"}), 400
        if not backup_name:
            return jsonify({"error": "backup_name is required"}), 400

        node_id = convert_node_id(node_id_str)

        job_service = get_job_service()
        result = job_service.queue_job(
            job_type=JobType.BACKUP,
            job_name=f"Backup: {backup_name}",
            job_data={
                "backup_name": backup_name,
                "description": description,
            },
            target_node_id=node_id,
        )

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 409  # Conflict

    except ValueError as e:
        return jsonify({"error": f"Invalid node_id: {e}"}), 400
    except Exception as e:
        logger.error(f"Error queuing backup job: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/restore", methods=["POST"])
def api_queue_restore_job():
    """
    Queue a restore job.

    Request body:
        backup_id: ID of backup to restore (required)
        target_node_id: Node to restore to (required)
        skip_primary_channel: Skip restoring primary channel (default true)
        skip_lora: Skip restoring LoRa config (default false)
        skip_security: Skip restoring security config (default true)
        reboot_after: Reboot node after restore (default false)
    """
    try:
        data = request.get_json() or {}

        backup_id = data.get("backup_id")
        target_node_str = data.get("target_node_id")
        skip_primary_channel = data.get("skip_primary_channel", True)
        skip_lora = data.get("skip_lora", False)
        skip_security = data.get("skip_security", True)
        reboot_after = data.get("reboot_after", False)

        if not backup_id:
            return jsonify({"error": "backup_id is required"}), 400
        if not target_node_str:
            return jsonify({"error": "target_node_id is required"}), 400

        target_node_id = convert_node_id(target_node_str)

        # Get backup info for job name
        from ..database.admin_repository import AdminRepository

        backup = AdminRepository.get_backup(backup_id)
        if not backup:
            return jsonify({"error": f"Backup {backup_id} not found"}), 404

        backup_name = backup.get("backup_name", f"Backup #{backup_id}")

        job_service = get_job_service()
        result = job_service.queue_job(
            job_type=JobType.RESTORE,
            job_name=f"Restore: {backup_name}",
            job_data={
                "backup_id": backup_id,
                "skip_primary_channel": skip_primary_channel,
                "skip_lora": skip_lora,
                "skip_security": skip_security,
                "reboot_after": reboot_after,
            },
            target_node_id=target_node_id,
        )

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 409  # Conflict

    except ValueError as e:
        return jsonify({"error": f"Invalid target_node_id: {e}"}), 400
    except Exception as e:
        logger.error(f"Error queuing restore job: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/bulk-command", methods=["POST"])
def api_queue_bulk_command_job():
    """
    Queue a bulk command job.

    Request body:
        command: Command to execute (reboot, shutdown)
        node_ids: List of node IDs to target
    """
    try:
        data = request.get_json() or {}

        command = data.get("command")
        node_ids_raw = data.get("node_ids", [])

        if not command:
            return jsonify({"error": "command is required"}), 400
        if not node_ids_raw:
            return jsonify({"error": "node_ids is required"}), 400

        valid_commands = ["reboot", "shutdown"]
        if command not in valid_commands:
            return (
                jsonify(
                    {"error": f"Invalid command. Must be one of: {valid_commands}"}
                ),
                400,
            )

        node_ids = [convert_node_id(n) for n in node_ids_raw]

        job_service = get_job_service()
        result = job_service.queue_job(
            job_type=JobType.BULK_COMMAND,
            job_name=f"Bulk {command}: {len(node_ids)} nodes",
            job_data={
                "command": command,
                "node_ids": node_ids,
            },
            target_node_id=None,  # Bulk commands don't target a single node
        )

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 409  # Conflict

    except ValueError as e:
        return jsonify({"error": f"Invalid node_id: {e}"}), 400
    except Exception as e:
        logger.error(f"Error queuing bulk command job: {e}")
        return jsonify({"error": str(e)}), 500


@job_bp.route("/api/jobs/config-deploy", methods=["POST"])
def api_queue_config_deploy_job():
    """
    Queue a configuration deployment job.

    Request body:
        target_node_id: Node to deploy config to (required)
        config_type: Type of config (device, position, power, etc.)
        config_data: Configuration data to deploy
    """
    try:
        data = request.get_json() or {}

        target_node_str = data.get("target_node_id")
        config_type = data.get("config_type")
        config_data = data.get("config_data")

        if not target_node_str:
            return jsonify({"error": "target_node_id is required"}), 400
        if not config_type:
            return jsonify({"error": "config_type is required"}), 400
        if not config_data:
            return jsonify({"error": "config_data is required"}), 400

        target_node_id = convert_node_id(target_node_str)

        job_service = get_job_service()
        result = job_service.queue_job(
            job_type=JobType.CONFIG_DEPLOY,
            job_name=f"Deploy {config_type} config",
            job_data={
                "config_type": config_type,
                "config_data": config_data,
            },
            target_node_id=target_node_id,
        )

        if result["success"]:
            return jsonify(result)
        else:
            return jsonify(result), 409  # Conflict

    except ValueError as e:
        return jsonify({"error": f"Invalid target_node_id: {e}"}), 400
    except Exception as e:
        logger.error(f"Error queuing config deploy job: {e}")
        return jsonify({"error": str(e)}), 500
