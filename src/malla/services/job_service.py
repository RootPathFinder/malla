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


class JobCancelledException(Exception):
    """Exception raised when a job is cancelled."""

    pass


class JobProgressCallback:
    """Callback handler for job progress updates."""

    def __init__(self, job_id: int):
        self.job_id = job_id
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def update(
        self,
        progress: int,
        message: str | None = None,
        phase: str | None = None,
        current: int | None = None,
        total: int | None = None,
        is_error: bool = False,
        is_warning: bool = False,
    ) -> None:
        """Update job progress."""
        # Track errors and warnings
        if message:
            if is_error:
                self.errors.append(message)
            elif is_warning:
                self.warnings.append(message)

        JobRepository.update_job_progress(
            job_id=self.job_id,
            progress=progress,
            message=message,
            phase=phase,
            current=current,
            total=total,
        )

    def check_cancelled(self) -> None:
        """Check if job was cancelled and raise exception if so."""
        if JobRepository.is_cancel_requested(self.job_id):
            raise JobCancelledException("Job cancelled by user")

    def is_cancelled(self) -> bool:
        """Check if job cancellation was requested."""
        return JobRepository.is_cancel_requested(self.job_id)

    def is_pause_requested(self) -> bool:
        """Check if job pause was requested."""
        return JobRepository.is_pause_requested(self.job_id)

    def check_paused(self) -> None:
        """
        Check if pause was requested and pause the job if so.

        This blocks until the job is resumed or cancelled.
        Should be called at safe checkpoints in job handlers.
        """
        if not self.is_pause_requested():
            return

        # Set the job to paused status
        JobRepository.set_job_paused(self.job_id)

        # Wait until job is resumed (status changes from PAUSED) or cancelled
        import time as time_module

        while True:
            # Check for cancellation first
            if JobRepository.is_cancel_requested(self.job_id):
                raise JobCancelledException("Job cancelled by user")

            # Check if job is still paused
            job = JobRepository.get_job(self.job_id)
            if not job or job.get("status") != JobStatus.PAUSED.value:
                # Job was resumed or something changed
                break

            # Wait a bit before checking again
            time_module.sleep(0.5)

    def check_cancelled_and_paused(self) -> None:
        """
        Check for both cancellation and pause requests.

        This is a convenience method that should be called at checkpoints
        in job handlers. It first checks for cancellation (throws exception)
        then checks for pause (blocks until resumed).
        """
        self.check_cancelled()
        self.check_paused()


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

        except JobCancelledException:
            logger.info(f"Job {job_id} was cancelled by user")
            JobRepository.update_job_status(
                job_id, JobStatus.CANCELLED, "Cancelled by user"
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

    def cancel_job(self, job_id: int, force: bool = False) -> dict[str, Any]:
        """Cancel a queued or running job.

        Args:
            job_id: The job ID to cancel
            force: If True, force cancel even if running (for orphaned jobs)
        """
        # If force cancel requested, do it directly
        if force:
            if JobRepository.force_cancel_job(job_id):
                return {"success": True, "message": "Job force cancelled"}
            return {
                "success": False,
                "error": "Job cannot be cancelled (already completed or not found)",
            }

        # First try to cancel if queued
        if JobRepository.cancel_job(job_id):
            return {"success": True, "message": "Job cancelled"}

        # If not queued, try to request cancellation of running job
        if JobRepository.request_cancel_running_job(job_id):
            return {
                "success": True,
                "message": "Cancellation requested - job will stop at next checkpoint",
            }

        return {
            "success": False,
            "error": "Job cannot be cancelled (already completed or failed)",
        }

    def pause_job(self, job_id: int) -> dict[str, Any]:
        """Pause a queued or running job."""
        # First try to pause a queued job (immediate pause)
        if JobRepository.pause_job(job_id):
            return {"success": True, "message": "Job paused"}

        # Try to request pause for a running job
        if JobRepository.request_pause_running_job(job_id):
            return {
                "success": True,
                "message": "Pause requested (job will pause at next checkpoint)",
            }

        # Job is neither queued nor running
        return {
            "success": False,
            "error": "Job cannot be paused (only queued or running jobs can be paused)",
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

    def _calculate_backup_delay(
        self,
        node_id: int,
        job_data: dict[str, Any],
    ) -> float:
        """
        Calculate the inter-request delay for backup operations.

        The delay is determined by:
        1. Explicit user setting (if provided)
        2. Connection type (TCP/Serial = 0, MQTT = needs delay)
        3. Estimated hop count to the node (more hops = more delay)

        Args:
            node_id: Target node ID
            job_data: Job data containing optional delay settings

        Returns:
            Delay in seconds between config requests
        """
        from .admin_service import AdminConnectionType, get_admin_service

        # Check for explicit user setting
        if "inter_request_delay" in job_data:
            return float(job_data["inter_request_delay"])

        admin_service = get_admin_service()
        conn_type = admin_service.connection_type

        # Fast mode: TCP or Serial connections are direct, no mesh delay needed
        if conn_type in (AdminConnectionType.TCP, AdminConnectionType.SERIAL):
            gateway_id = admin_service.gateway_node_id
            # If target is the gateway itself (directly connected), no delay
            if gateway_id and gateway_id == node_id:
                logger.info(
                    f"Direct connection to node {node_id:08x}, using fast mode (no delay)"
                )
                return 0.0

        # For MQTT or remote nodes, calculate delay based on hop count
        base_delay = 0.5  # Minimum delay for any mesh operation
        hop_delay = 0.5  # Additional delay per hop

        # Try to get hop count from traceroute data
        estimated_hops = self._estimate_hop_count(node_id)

        if estimated_hops is not None:
            calculated_delay = base_delay + (estimated_hops * hop_delay)
            logger.info(
                f"Estimated {estimated_hops} hops to node {node_id:08x}, "
                f"using {calculated_delay:.1f}s delay"
            )
            return calculated_delay

        # Default delay if we can't determine hop count
        default_delay = 1.5
        logger.info(
            f"Could not estimate hops to node {node_id:08x}, "
            f"using default {default_delay}s delay"
        )
        return default_delay

    def _estimate_hop_count(self, node_id: int) -> int | None:
        """
        Estimate the hop count to a node based on recent traceroute data.

        Args:
            node_id: Target node ID

        Returns:
            Estimated hop count, or None if unknown
        """
        from ..database.repositories import TracerouteRepository
        from .admin_service import get_admin_service

        admin_service = get_admin_service()
        gateway_id = admin_service.gateway_node_id

        if not gateway_id:
            return None

        try:
            # Look for recent traceroutes from gateway to target node
            # This gives us actual path information
            result = TracerouteRepository.get_traceroute_packets(
                limit=50,
                filters={
                    "from_node": gateway_id,
                    "to_node": node_id,
                    "processed_successfully_only": True,
                },
            )

            packets = result.get("packets", [])
            if packets:
                # Get the most recent successful traceroute
                # Count hops from the route_nodes in the raw payload
                from ..utils.traceroute_utils import parse_traceroute_payload

                hop_counts = []
                for packet in packets[:5]:  # Check last 5 traceroutes
                    if packet.get("raw_payload"):
                        route_data = parse_traceroute_payload(packet["raw_payload"])
                        route_nodes = route_data.get("route_nodes", [])
                        if route_nodes:
                            hop_counts.append(len(route_nodes))

                if hop_counts:
                    # Use the median hop count for stability
                    hop_counts.sort()
                    median_hops = hop_counts[len(hop_counts) // 2]
                    return median_hops

            # Try reverse direction (target to gateway)
            result = TracerouteRepository.get_traceroute_packets(
                limit=50,
                filters={
                    "from_node": node_id,
                    "to_node": gateway_id,
                    "processed_successfully_only": True,
                },
            )

            packets = result.get("packets", [])
            if packets:
                from ..utils.traceroute_utils import parse_traceroute_payload

                hop_counts = []
                for packet in packets[:5]:
                    if packet.get("raw_payload"):
                        route_data = parse_traceroute_payload(packet["raw_payload"])
                        route_nodes = route_data.get("route_nodes", [])
                        if route_nodes:
                            hop_counts.append(len(route_nodes))

                if hop_counts:
                    hop_counts.sort()
                    return hop_counts[len(hop_counts) // 2]

            return None

        except Exception as e:
            logger.warning(f"Error estimating hop count to {node_id:08x}: {e}")
            return None

    def _execute_backup_job(
        self, job: dict[str, Any], progress: JobProgressCallback
    ) -> dict[str, Any]:
        """Execute a backup job."""
        from .admin_service import ConfigType, ModuleConfigType, get_admin_service

        job_data = job["job_data"]
        node_id = job["target_node_id"]
        backup_name = job_data.get("backup_name", "Backup")
        description = job_data.get("description", "")

        # Calculate inter-request delay based on connection type and hop count
        inter_request_delay = self._calculate_backup_delay(node_id, job_data)

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
            # Check for cancellation/pause before each request
            progress.check_cancelled_and_paused()

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
                # Delay between requests to prevent mesh congestion
                if current_item < total_items:
                    time.sleep(inter_request_delay)
            else:
                error_detail = result.error or "Unknown error"
                # Check if it's a timeout
                is_timeout = "timeout" in error_detail.lower()
                error_msg = f"✗ {name} config failed: {error_detail}"
                if is_timeout:
                    error_msg = f"⏱ {name} config timeout: {error_detail}"
                errors.append(f"core:{name}: {error_detail}")
                progress.update(
                    prog_pct,
                    error_msg,
                    "core",
                    current_item,
                    total_items,
                    is_error=True,
                )
                # Still delay after failures to let mesh recover
                if current_item < total_items:
                    time.sleep(inter_request_delay)

        # Fetch module configs
        progress.update(
            int((current_item / total_items) * 100),
            "Fetching module configurations...",
            "module",
            current_item,
            total_items,
        )

        for name, module_type in module_configs:
            # Check for cancellation/pause before each request
            progress.check_cancelled_and_paused()

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
                # Delay between requests to prevent mesh congestion
                if current_item < total_items:
                    time.sleep(inter_request_delay)
            else:
                error_detail = result.error or "Unknown error"
                is_timeout = "timeout" in error_detail.lower()
                error_msg = f"✗ {name} module failed: {error_detail}"
                if is_timeout:
                    error_msg = f"⏱ {name} module timeout: {error_detail}"
                errors.append(f"module:{name}: {error_detail}")
                progress.update(
                    prog_pct,
                    error_msg,
                    "module",
                    current_item,
                    total_items,
                    is_error=True,
                )
                # Still delay after failures to let mesh recover
                if current_item < total_items:
                    time.sleep(inter_request_delay)

        # Fetch channels
        progress.update(
            int((current_item / total_items) * 100),
            "Fetching channel configurations...",
            "channels",
            current_item,
            total_items,
        )

        for channel_idx in channels:
            # Check for cancellation/pause before each request
            progress.check_cancelled_and_paused()

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
                # Delay between requests to prevent mesh congestion
                if current_item < total_items:
                    time.sleep(inter_request_delay)
            else:
                error_detail = result.error or "Unknown error"
                is_timeout = "timeout" in error_detail.lower()
                error_msg = f"✗ Channel {channel_idx} failed: {error_detail}"
                if is_timeout:
                    error_msg = f"⏱ Channel {channel_idx} timeout: {error_detail}"
                errors.append(f"channel:{channel_idx}: {error_detail}")
                progress.update(
                    prog_pct,
                    error_msg,
                    "channels",
                    current_item,
                    total_items,
                    is_error=True,
                )
                # Still delay after failures to let mesh recover
                if current_item < total_items:
                    time.sleep(inter_request_delay)

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
        skip_lora = job_data.get("skip_lora", False)
        skip_security = job_data.get("skip_security", True)
        reboot_after = job_data.get("reboot_after", False)

        # Selective restore: if lists are provided, only restore those items
        selected_core_configs = job_data.get("selected_core_configs")  # list or None
        selected_module_configs = job_data.get(
            "selected_module_configs"
        )  # list or None
        selected_channels = job_data.get("selected_channels")  # list or None

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
                # Check if selective restore is enabled and if this config is selected
                if selected_core_configs is not None:
                    if config_name not in selected_core_configs:
                        continue
                # Skip overrides even if selected
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
                # Check if selective restore is enabled and if this module is selected
                if selected_module_configs is not None:
                    if module_name not in selected_module_configs:
                        continue
                items_to_restore.append(("module", module_name, module_type))

        # Channels
        for channel_idx_str in channels.keys():
            channel_idx = int(channel_idx_str)
            # Check if selective restore is enabled and if this channel is selected
            if selected_channels is not None:
                if channel_idx_str not in selected_channels:
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
            # Check for cancellation/pause before each request
            progress.check_cancelled_and_paused()

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
                    is_timeout = "timeout" in error_msg.lower()
                    display_msg = f"✗ {item_name.upper()} config failed: {error_msg}"
                    if is_timeout:
                        display_msg = (
                            f"⏱ {item_name.upper()} config timeout: {error_msg}"
                        )
                    errors.append(f"core:{item_name}: {error_msg}")
                    progress.update(
                        prog_pct,
                        display_msg,
                        "core",
                        current_item,
                        total_items,
                        is_error=True,
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
                    is_timeout = "timeout" in error_msg.lower()
                    display_msg = f"✗ {item_name.upper()} module failed: {error_msg}"
                    if is_timeout:
                        display_msg = (
                            f"⏱ {item_name.upper()} module timeout: {error_msg}"
                        )
                    errors.append(f"module:{item_name}: {error_msg}")
                    progress.update(
                        prog_pct,
                        display_msg,
                        "module",
                        current_item,
                        total_items,
                        is_error=True,
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

                raw_channel_data = channels.get(item_name, {})
                # Flatten channel data structure for set_channel
                # Backup stores: {"index": 0, "role": 1, "settings": {"name": "...", "psk": "..."}}
                # set_channel expects: {"role": 1, "name": "...", "psk": "..."}
                channel_data = {}
                if "role" in raw_channel_data:
                    channel_data["role"] = raw_channel_data["role"]
                settings = raw_channel_data.get("settings", {})
                if "name" in settings:
                    channel_data["name"] = settings["name"]
                if "psk" in settings:
                    channel_data["psk"] = settings["psk"]
                if "module_settings" in settings:
                    module_settings = settings["module_settings"]
                    if "position_precision" in module_settings:
                        channel_data["position_precision"] = module_settings[
                            "position_precision"
                        ]

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
                    is_timeout = "timeout" in error_msg.lower()
                    display_msg = f"✗ Channel {channel_idx} failed: {error_msg}"
                    if is_timeout:
                        display_msg = f"⏱ Channel {channel_idx} timeout: {error_msg}"
                    errors.append(f"channel:{channel_idx}: {error_msg}")
                    progress.update(
                        prog_pct,
                        display_msg,
                        "channel",
                        current_item,
                        total_items,
                        is_error=True,
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
            # Check for cancellation/pause before each command
            progress.check_cancelled_and_paused()

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
                    progress.update(
                        prog_pct,
                        f"✗ Unknown command: {command}",
                        "executing",
                        i + 1,
                        total_nodes,
                        is_error=True,
                    )
                    continue

                if result.success:
                    successful.append(node_id)
                    progress.update(
                        prog_pct,
                        f"✓ {command} sent to node !{node_id:08x}",
                        "executing",
                        i + 1,
                        total_nodes,
                    )
                else:
                    error_msg = result.error or "Unknown error"
                    failed.append({"node_id": node_id, "error": error_msg})
                    progress.update(
                        prog_pct,
                        f"✗ {command} failed for !{node_id:08x}: {error_msg}",
                        "executing",
                        i + 1,
                        total_nodes,
                        is_error=True,
                    )
            except Exception as e:
                failed.append({"node_id": node_id, "error": str(e)})
                progress.update(
                    prog_pct,
                    f"✗ {command} error for !{node_id:08x}: {e}",
                    "executing",
                    i + 1,
                    total_nodes,
                    is_error=True,
                )

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
    from ..database.job_repository import JobRepository, init_job_tables

    # Initialize database tables
    init_job_tables()

    # Clean up any orphaned jobs from previous server runs
    # Jobs running for more than 10 minutes are considered orphaned on startup
    orphaned_count = JobRepository.cleanup_orphaned_jobs(max_running_time_seconds=600)
    if orphaned_count > 0:
        logger.info(
            f"Cleaned up {orphaned_count} orphaned jobs from previous server run"
        )

    # Get and start the service
    service = get_job_service()
    service.start()

    return service
