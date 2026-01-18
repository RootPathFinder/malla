"""
Background job service for running long-running admin operations.

This service manages the execution of background jobs like backups and restores,
allowing users to start operations and check back later for results.
"""

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from ..database.job_repository import (
    JobRepository,
    JobStatus,
    JobType,
)

logger = logging.getLogger(__name__)


class JobProgressCallback:
    """Callback handler for job progress updates."""

    def __init__(self, job_id: int):
        self.job_id = job_id

    def update(
        self,
        progress: int,
        message: str | None = None,
        phase: str | None = None,
        current: int | None = None,
        total: int | None = None,
    ) -> None:
        """Update job progress."""
        JobRepository.update_job_progress(
            job_id=self.job_id,
            progress=progress,
            message=message,
            phase=phase,
            current=current,
            total=total,
        )


class JobService:
    """
    Service for managing background jobs.

    Jobs are queued and executed one at a time for each node to prevent
    conflicts. The service runs a background thread that processes queued jobs.
    """

    _instance: "JobService | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "JobService":
        """Singleton pattern for job service."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize the job service."""
        if self._initialized:
            return

        self._initialized = True
        self._worker_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._job_handlers: dict[JobType, Callable] = {}
        self._running = False

        # Register default job handlers
        self._register_handlers()

        logger.info("JobService initialized")

    def _register_handlers(self) -> None:
        """Register job type handlers."""
        self._job_handlers = {
            JobType.BACKUP: self._execute_backup_job,
            JobType.RESTORE: self._execute_restore_job,
            JobType.BULK_COMMAND: self._execute_bulk_command_job,
            JobType.CONFIG_DEPLOY: self._execute_config_deploy_job,
        }

    def start(self) -> None:
        """Start the job worker thread."""
        if self._running:
            return

        self._stop_event.clear()
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            name="JobWorker",
            daemon=True,
        )
        self._worker_thread.start()
        self._running = True
        logger.info("Job worker thread started")

    def stop(self) -> None:
        """Stop the job worker thread."""
        if not self._running:
            return

        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
        self._running = False
        logger.info("Job worker thread stopped")

    def is_running(self) -> bool:
        """Check if the job service is running."""
        return self._running

    def _worker_loop(self) -> None:
        """Main worker loop that processes queued jobs."""
        logger.info("Job worker loop started")

        while not self._stop_event.is_set():
            try:
                # Get next job that's ready to run
                job = JobRepository.get_next_queued_job()

                if job:
                    self._process_job(job)
                else:
                    # No jobs ready, wait a bit
                    self._stop_event.wait(timeout=2.0)

            except Exception as e:
                logger.error(f"Error in job worker loop: {e}", exc_info=True)
                self._stop_event.wait(timeout=5.0)

        logger.info("Job worker loop stopped")

    def _process_job(self, job: dict[str, Any]) -> None:
        """Process a single job."""
        job_id = job["id"]
        job_type_str = job["job_type"]
        job_name = job["job_name"]

        logger.info(f"Starting job {job_id}: {job_type_str} - {job_name}")

        # Mark as running
        JobRepository.update_job_status(job_id, JobStatus.RUNNING)

        try:
            job_type = JobType(job_type_str)
            handler = self._job_handlers.get(job_type)

            if not handler:
                raise ValueError(f"No handler for job type: {job_type_str}")

            # Create progress callback
            progress = JobProgressCallback(job_id)

            # Execute the job
            result = handler(job, progress)

            # Mark as completed
            JobRepository.complete_job(
                job_id=job_id,
                success=result.get("success", False),
                result_data=result.get("data"),
                error_message=result.get("error"),
            )

            logger.info(
                f"Job {job_id} completed: "
                f"{'success' if result.get('success') else 'failed'}"
            )

        except Exception as e:
            logger.error(f"Job {job_id} failed with exception: {e}", exc_info=True)
            JobRepository.complete_job(
                job_id=job_id,
                success=False,
                error_message=str(e),
            )

    def queue_job(
        self,
        job_type: JobType,
        job_name: str,
        job_data: dict[str, Any],
        target_node_id: int | None = None,
    ) -> dict[str, Any]:
        """
        Queue a new job for background execution.

        Args:
            job_type: Type of job to queue
            job_name: Human-readable name for the job
            job_data: Parameters for the job
            target_node_id: Optional target node ID

        Returns:
            Dict with job_id and queue position, or error if conflicting
        """
        # Check for conflicting jobs
        conflicting = JobRepository.has_conflicting_job(job_type, target_node_id)
        if conflicting:
            return {
                "success": False,
                "error": f"A {conflicting['job_type']} job is already "
                f"{'running' if conflicting['status'] == 'running' else 'queued'} "
                f"for this node: {conflicting['job_name']}",
                "conflicting_job": conflicting,
            }

        # Create the job
        job_id = JobRepository.create_job(
            job_type=job_type,
            job_name=job_name,
            job_data=job_data,
            target_node_id=target_node_id,
        )

        # Get queue position
        queue_position = JobRepository.get_queue_position(job_id)

        # Make sure worker is running
        if not self._running:
            self.start()

        return {
            "success": True,
            "job_id": job_id,
            "queue_position": queue_position,
            "status": JobStatus.QUEUED.value,
        }

    def get_job(self, job_id: int) -> dict[str, Any] | None:
        """Get job details by ID."""
        return JobRepository.get_job(job_id)

    def get_jobs(
        self,
        status: JobStatus | None = None,
        job_type: JobType | None = None,
        target_node_id: int | None = None,
        limit: int = 50,
        include_completed: bool = True,
    ) -> list[dict[str, Any]]:
        """Get jobs with optional filtering."""
        return JobRepository.get_jobs(
            status=status,
            job_type=job_type,
            target_node_id=target_node_id,
            limit=limit,
            include_completed=include_completed,
        )

    def get_active_jobs(
        self, target_node_id: int | None = None
    ) -> list[dict[str, Any]]:
        """Get all queued or running jobs."""
        return JobRepository.get_active_jobs(target_node_id)

    def cancel_job(self, job_id: int) -> dict[str, Any]:
        """Cancel a queued job."""
        if JobRepository.cancel_job(job_id):
            return {"success": True, "message": "Job cancelled"}
        else:
            return {
                "success": False,
                "error": "Job cannot be cancelled (already running or completed)",
            }

    def pause_job(self, job_id: int) -> dict[str, Any]:
        """Pause a queued job."""
        if JobRepository.pause_job(job_id):
            return {"success": True, "message": "Job paused"}
        else:
            return {
                "success": False,
                "error": "Job cannot be paused (only queued jobs can be paused)",
            }

    def resume_job(self, job_id: int) -> dict[str, Any]:
        """Resume a paused job."""
        if JobRepository.resume_job(job_id):
            return {"success": True, "message": "Job resumed"}
        else:
            return {
                "success": False,
                "error": "Job cannot be resumed (not in paused state)",
            }

    def get_job_progress_log(
        self, job_id: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Get progress log for a job."""
        return JobRepository.get_job_progress_log(job_id, limit)

    # =========================================================================
    # Job Handlers
    # =========================================================================

    def _execute_backup_job(
        self, job: dict[str, Any], progress: JobProgressCallback
    ) -> dict[str, Any]:
        """Execute a backup job."""
        from .admin_service import ConfigType, ModuleConfigType, get_admin_service

        job_data = job["job_data"]
        node_id = job["target_node_id"]
        backup_name = job_data.get("backup_name", "Backup")
        description = job_data.get("description", "")

        admin_service = get_admin_service()

        # Define all items to fetch
        core_configs = [
            ("DEVICE", ConfigType.DEVICE),
            ("POSITION", ConfigType.POSITION),
            ("POWER", ConfigType.POWER),
            ("NETWORK", ConfigType.NETWORK),
            ("DISPLAY", ConfigType.DISPLAY),
            ("LORA", ConfigType.LORA),
            ("BLUETOOTH", ConfigType.BLUETOOTH),
            ("SECURITY", ConfigType.SECURITY),
        ]

        module_configs = [
            ("MQTT", ModuleConfigType.MQTT),
            ("SERIAL", ModuleConfigType.SERIAL),
            ("EXTNOTIF", ModuleConfigType.EXTNOTIF),
            ("STOREFORWARD", ModuleConfigType.STOREFORWARD),
            ("RANGETEST", ModuleConfigType.RANGETEST),
            ("TELEMETRY", ModuleConfigType.TELEMETRY),
            ("CANNEDMSG", ModuleConfigType.CANNEDMSG),
            ("AUDIO", ModuleConfigType.AUDIO),
            ("REMOTEHARDWARE", ModuleConfigType.REMOTEHARDWARE),
            ("NEIGHBORINFO", ModuleConfigType.NEIGHBORINFO),
            ("AMBIENTLIGHTING", ModuleConfigType.AMBIENTLIGHTING),
            ("DETECTIONSENSOR", ModuleConfigType.DETECTIONSENSOR),
            ("PAXCOUNTER", ModuleConfigType.PAXCOUNTER),
        ]

        channels = list(range(8))

        total_items = len(core_configs) + len(module_configs) + len(channels)
        current_item = 0

        backup_data: dict[str, Any] = {
            "backup_version": 1,
            "target_node_id": node_id,
            "created_at": time.time(),
            "core_configs": {},
            "module_configs": {},
            "channels": {},
        }

        errors: list[str] = []
        successful_configs: list[str] = []

        # Fetch core configs
        progress.update(0, "Fetching core configurations...", "core", 0, total_items)

        for name, config_type in core_configs:
            current_item += 1
            prog_pct = int((current_item / total_items) * 100)

            progress.update(
                prog_pct,
                f"Fetching {name} config...",
                "core",
                current_item,
                total_items,
            )

            result = admin_service.get_config(
                target_node_id=node_id,
                config_type=config_type,
                max_retries=3,
                retry_delay=2.0,
                timeout=30.0,
            )

            if result.success and result.response:
                backup_data["core_configs"][name.lower()] = result.response
                successful_configs.append(f"core:{name}")
                progress.update(
                    prog_pct,
                    f"✓ {name} config retrieved",
                    "core",
                    current_item,
                    total_items,
                )
            else:
                errors.append(f"core:{name}: {result.error or 'Unknown error'}")
                progress.update(
                    prog_pct,
                    f"✗ {name} config failed",
                    "core",
                    current_item,
                    total_items,
                )

        # Fetch module configs
        progress.update(
            int((current_item / total_items) * 100),
            "Fetching module configurations...",
            "module",
            current_item,
            total_items,
        )

        for name, module_type in module_configs:
            current_item += 1
            prog_pct = int((current_item / total_items) * 100)

            progress.update(
                prog_pct,
                f"Fetching {name} module...",
                "module",
                current_item,
                total_items,
            )

            result = admin_service.get_module_config(
                target_node_id=node_id,
                module_config_type=module_type,
                max_retries=3,
                retry_delay=2.0,
                timeout=30.0,
            )

            if result.success and result.response:
                backup_data["module_configs"][name.lower()] = result.response
                successful_configs.append(f"module:{name}")
                progress.update(
                    prog_pct,
                    f"✓ {name} module retrieved",
                    "module",
                    current_item,
                    total_items,
                )
            else:
                errors.append(f"module:{name}: {result.error or 'Unknown error'}")
                progress.update(
                    prog_pct,
                    f"✗ {name} module failed",
                    "module",
                    current_item,
                    total_items,
                )

        # Fetch channels
        progress.update(
            int((current_item / total_items) * 100),
            "Fetching channel configurations...",
            "channels",
            current_item,
            total_items,
        )

        for channel_idx in channels:
            current_item += 1
            prog_pct = int((current_item / total_items) * 100)

            progress.update(
                prog_pct,
                f"Fetching Channel {channel_idx}...",
                "channels",
                current_item,
                total_items,
            )

            result = admin_service.get_channel(
                target_node_id=node_id,
                channel_index=channel_idx,
                max_retries=3,
                retry_delay=2.0,
                timeout=30.0,
            )

            if result.success and result.response:
                backup_data["channels"][str(channel_idx)] = result.response
                successful_configs.append(f"channel:{channel_idx}")
                progress.update(
                    prog_pct,
                    f"✓ Channel {channel_idx} retrieved",
                    "channels",
                    current_item,
                    total_items,
                )
            else:
                errors.append(
                    f"channel:{channel_idx}: {result.error or 'Unknown error'}"
                )
                progress.update(
                    prog_pct,
                    f"✗ Channel {channel_idx} failed",
                    "channels",
                    current_item,
                    total_items,
                )

        # Save backup if we got at least some configs
        if successful_configs:
            progress.update(98, "Saving backup to database...", "saving")

            from ..database.admin_repository import AdminRepository

            # Get node info for metadata
            node_long_name = None
            node_short_name = None
            node_hex_id = f"!{node_id:08x}"

            if "device" in backup_data["core_configs"]:
                device_config = backup_data["core_configs"]["device"]
                node_long_name = device_config.get("device", {}).get("owner", None)
                node_short_name = device_config.get("device", {}).get(
                    "owner_short", None
                )

            backup_id = AdminRepository.create_backup(
                node_id=node_id,
                backup_name=backup_name,
                backup_data=json.dumps(backup_data),
                description=description,
                node_long_name=node_long_name,
                node_short_name=node_short_name,
                node_hex_id=node_hex_id,
            )

            progress.update(100, "Backup complete!", "complete")

            return {
                "success": True,
                "data": {
                    "backup_id": backup_id,
                    "backup_name": backup_name,
                    "successful_configs": successful_configs,
                    "failed_configs": errors,
                    "total_configs": len(successful_configs) + len(errors),
                },
            }
        else:
            return {
                "success": False,
                "error": "Failed to retrieve any configuration from node",
                "data": {"failed_configs": errors},
            }

    def _execute_restore_job(
        self, job: dict[str, Any], progress: JobProgressCallback
    ) -> dict[str, Any]:
        """Execute a restore job."""
        from ..database.admin_repository import AdminRepository
        from .admin_service import ConfigType, ModuleConfigType, get_admin_service

        job_data = job["job_data"]
        target_node_id = job["target_node_id"]
        backup_id = job_data.get("backup_id")
        skip_primary_channel = job_data.get("skip_primary_channel", True)
        skip_lora = job_data.get("skip_lora", False)
        skip_security = job_data.get("skip_security", True)
        reboot_after = job_data.get("reboot_after", False)

        # Get backup data
        backup = AdminRepository.get_backup(backup_id)
        if not backup:
            return {"success": False, "error": f"Backup {backup_id} not found"}

        try:
            backup_data = json.loads(backup.get("backup_data", "{}"))
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"Invalid backup data format: {e}"}

        admin_service = get_admin_service()

        # Define what we're restoring
        core_configs = backup_data.get("core_configs", {})
        module_configs = backup_data.get("module_configs", {})
        channels = backup_data.get("channels", {})

        # Build list of items to restore
        items_to_restore: list[tuple[str, str, Any]] = []

        # Core configs
        core_config_map = {
            "device": ConfigType.DEVICE,
            "position": ConfigType.POSITION,
            "power": ConfigType.POWER,
            "network": ConfigType.NETWORK,
            "display": ConfigType.DISPLAY,
            "lora": ConfigType.LORA,
            "bluetooth": ConfigType.BLUETOOTH,
            "security": ConfigType.SECURITY,
        }

        for config_name, config_type in core_config_map.items():
            if config_name in core_configs:
                if config_name == "lora" and skip_lora:
                    continue
                if config_name == "security" and skip_security:
                    continue
                items_to_restore.append(("core", config_name, config_type))

        # Module configs
        module_config_map = {
            "mqtt": ModuleConfigType.MQTT,
            "serial": ModuleConfigType.SERIAL,
            "extnotif": ModuleConfigType.EXTNOTIF,
            "storeforward": ModuleConfigType.STOREFORWARD,
            "rangetest": ModuleConfigType.RANGETEST,
            "telemetry": ModuleConfigType.TELEMETRY,
            "cannedmsg": ModuleConfigType.CANNEDMSG,
            "audio": ModuleConfigType.AUDIO,
            "remotehardware": ModuleConfigType.REMOTEHARDWARE,
            "neighborinfo": ModuleConfigType.NEIGHBORINFO,
            "ambientlighting": ModuleConfigType.AMBIENTLIGHTING,
            "detectionsensor": ModuleConfigType.DETECTIONSENSOR,
            "paxcounter": ModuleConfigType.PAXCOUNTER,
        }

        for module_name, module_type in module_config_map.items():
            if module_name in module_configs:
                items_to_restore.append(("module", module_name, module_type))

        # Channels
        for channel_idx_str in channels.keys():
            channel_idx = int(channel_idx_str)
            if channel_idx == 0 and skip_primary_channel:
                continue
            items_to_restore.append(("channel", channel_idx_str, channel_idx))

        total_items = len(items_to_restore)
        if total_items == 0:
            return {
                "success": False,
                "error": "No configurations to restore in backup",
            }

        current_item = 0
        successful_restores: list[str] = []
        errors: list[str] = []

        progress.update(
            1,
            f"Starting restore of {total_items} configurations...",
            "restoring",
            0,
            total_items,
        )

        # Restore items
        for item_type, item_name, item_enum in items_to_restore:
            current_item += 1
            prog_pct = int((current_item / total_items) * 95) + 2

            if item_type == "core":
                progress.update(
                    prog_pct,
                    f"Restoring {item_name.upper()} config...",
                    "core",
                    current_item,
                    total_items,
                )

                config_data = core_configs.get(item_name, {})
                if item_name in config_data:
                    config_data = config_data[item_name]

                result = admin_service.set_config(
                    target_node_id=target_node_id,
                    config_type=item_enum,
                    config_data=config_data,
                )

                if result.success:
                    successful_restores.append(f"core:{item_name}")
                    progress.update(
                        prog_pct,
                        f"✓ {item_name.upper()} config restored",
                        "core",
                        current_item,
                        total_items,
                    )
                else:
                    error_msg = result.error or "Unknown error"
                    errors.append(f"core:{item_name}: {error_msg}")
                    progress.update(
                        prog_pct,
                        f"✗ {item_name.upper()} config failed",
                        "core",
                        current_item,
                        total_items,
                    )

            elif item_type == "module":
                progress.update(
                    prog_pct,
                    f"Restoring {item_name.upper()} module...",
                    "module",
                    current_item,
                    total_items,
                )

                module_data = module_configs.get(item_name, {})
                if item_name in module_data:
                    module_data = module_data[item_name]

                result = admin_service.set_module_config(
                    target_node_id=target_node_id,
                    module_config_type=item_enum,
                    module_data=module_data,
                )

                if result.success:
                    successful_restores.append(f"module:{item_name}")
                    progress.update(
                        prog_pct,
                        f"✓ {item_name.upper()} module restored",
                        "module",
                        current_item,
                        total_items,
                    )
                else:
                    error_msg = result.error or "Unknown error"
                    errors.append(f"module:{item_name}: {error_msg}")
                    progress.update(
                        prog_pct,
                        f"✗ {item_name.upper()} module failed",
                        "module",
                        current_item,
                        total_items,
                    )

            elif item_type == "channel":
                channel_idx = item_enum
                progress.update(
                    prog_pct,
                    f"Restoring Channel {channel_idx}...",
                    "channel",
                    current_item,
                    total_items,
                )

                channel_data = channels.get(item_name, {})
                result = admin_service.set_channel(
                    target_node_id=target_node_id,
                    channel_index=channel_idx,
                    channel_data=channel_data,
                )

                if result.success:
                    successful_restores.append(f"channel:{channel_idx}")
                    progress.update(
                        prog_pct,
                        f"✓ Channel {channel_idx} restored",
                        "channel",
                        current_item,
                        total_items,
                    )
                else:
                    error_msg = result.error or "Unknown error"
                    errors.append(f"channel:{channel_idx}: {error_msg}")
                    progress.update(
                        prog_pct,
                        f"✗ Channel {channel_idx} failed",
                        "channel",
                        current_item,
                        total_items,
                    )

        # Reboot if requested
        reboot_sent = False
        reboot_error = None
        if reboot_after and successful_restores:
            progress.update(97, "Sending reboot command...", "reboot")

            reboot_result = admin_service.reboot_node(target_node_id, delay_seconds=5)
            if reboot_result.success:
                reboot_sent = True
                progress.update(99, "Node will reboot in 5 seconds", "reboot")
            else:
                reboot_error = reboot_result.error
                progress.update(99, f"Reboot command failed: {reboot_error}", "reboot")

        # Final result
        if successful_restores:
            progress.update(100, "Restore complete!", "complete")

            return {
                "success": True,
                "data": {
                    "message": f"Restored {len(successful_restores)} configurations",
                    "successful_restores": successful_restores,
                    "failed_restores": errors,
                    "total_restored": len(successful_restores),
                    "total_failed": len(errors),
                    "reboot_after": reboot_after,
                    "reboot_sent": reboot_sent,
                    "reboot_error": reboot_error,
                },
            }
        else:
            return {
                "success": False,
                "error": "Failed to restore any configurations",
                "data": {"failed_restores": errors},
            }

    def _execute_bulk_command_job(
        self, job: dict[str, Any], progress: JobProgressCallback
    ) -> dict[str, Any]:
        """Execute a bulk command job."""
        from .admin_service import get_admin_service

        job_data = job["job_data"]
        command = job_data.get("command")
        node_ids = job_data.get("node_ids", [])

        if not command or not node_ids:
            return {"success": False, "error": "Missing command or node_ids"}

        admin_service = get_admin_service()
        total_nodes = len(node_ids)
        successful = []
        failed = []

        for i, node_id in enumerate(node_ids):
            prog_pct = int(((i + 1) / total_nodes) * 100)
            progress.update(
                prog_pct,
                f"Executing {command} on node {node_id}...",
                "executing",
                i + 1,
                total_nodes,
            )

            try:
                if command == "reboot":
                    result = admin_service.reboot_node(node_id)
                elif command == "shutdown":
                    result = admin_service.shutdown_node(node_id)
                else:
                    failed.append(
                        {"node_id": node_id, "error": f"Unknown command: {command}"}
                    )
                    continue

                if result.success:
                    successful.append(node_id)
                else:
                    failed.append({"node_id": node_id, "error": result.error})
            except Exception as e:
                failed.append({"node_id": node_id, "error": str(e)})

        progress.update(
            100, f"Completed {len(successful)}/{total_nodes} nodes", "complete"
        )

        return {
            "success": len(successful) > 0,
            "data": {
                "command": command,
                "successful": successful,
                "failed": failed,
                "total_nodes": total_nodes,
            },
        }

    def _execute_config_deploy_job(
        self, job: dict[str, Any], progress: JobProgressCallback
    ) -> dict[str, Any]:
        """Execute a configuration deployment job."""
        from .admin_service import ConfigType, ModuleConfigType, get_admin_service

        job_data = job["job_data"]
        target_node_id = job["target_node_id"]
        config_type = job_data.get("config_type")
        config_data = job_data.get("config_data")

        if not config_type or not config_data:
            return {"success": False, "error": "Missing config_type or config_data"}

        admin_service = get_admin_service()

        progress.update(10, f"Deploying {config_type} configuration...", "deploying")

        try:
            # Determine if it's a core or module config
            try:
                config_enum = ConfigType[config_type.upper()]
                result = admin_service.set_config(
                    target_node_id=target_node_id,
                    config_type=config_enum,
                    config_data=config_data,
                )
            except KeyError:
                try:
                    module_enum = ModuleConfigType[config_type.upper()]
                    result = admin_service.set_module_config(
                        target_node_id=target_node_id,
                        module_config_type=module_enum,
                        module_data=config_data,
                    )
                except KeyError:
                    return {
                        "success": False,
                        "error": f"Unknown config type: {config_type}",
                    }

            if result.success:
                progress.update(
                    100, f"✓ {config_type} configuration deployed", "complete"
                )
                return {
                    "success": True,
                    "data": {"config_type": config_type, "log_id": result.log_id},
                }
            else:
                return {"success": False, "error": result.error}

        except Exception as e:
            return {"success": False, "error": str(e)}


# Singleton accessor
_job_service_instance: JobService | None = None


def get_job_service() -> JobService:
    """Get the singleton job service instance."""
    global _job_service_instance
    if _job_service_instance is None:
        _job_service_instance = JobService()
    return _job_service_instance


def init_job_service() -> JobService:
    """Initialize and start the job service."""
    from ..database.job_repository import init_job_tables

    # Initialize database tables
    init_job_tables()

    # Get and start the service
    service = get_job_service()
    service.start()

    return service
