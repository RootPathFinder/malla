/**
 * Job Manager - Background job management for admin operations
 *
 * This module provides functionality for:
 * - Queuing background jobs (backups, restores, etc.)
 * - Polling for job status updates
 * - Displaying job progress in the UI
 * - Managing the job queue display
 */

class JobManager {
    constructor(options = {}) {
        this.pollInterval = options.pollInterval || 2000;
        this.onJobUpdate = options.onJobUpdate || null;
        this.onJobComplete = options.onJobComplete || null;
        this.onQueueUpdate = options.onQueueUpdate || null;

        this.activePolls = new Map(); // job_id -> interval
        this.lastQueueStatus = null;
    }

    /**
     * Queue a backup job
     * @param {string} nodeId - Node ID to backup
     * @param {string} backupName - Name for the backup
     * @param {string} description - Optional description
     * @returns {Promise<Object>} Job creation result
     */
    async queueBackup(nodeId, backupName, description = '') {
        const response = await fetch('/api/admin/backups/job', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                node_id: nodeId,
                backup_name: backupName,
                description: description
            })
        });

        const result = await response.json();

        if (result.success && result.job_id) {
            this.startPolling(result.job_id);
        }

        return result;
    }

    /**
     * Queue a restore job
     * @param {number} backupId - Backup ID to restore
     * @param {string} targetNodeId - Node ID to restore to
     * @param {Object} options - Restore options
     * @returns {Promise<Object>} Job creation result
     */
    async queueRestore(backupId, targetNodeId, options = {}) {
        const response = await fetch('/api/admin/backups/restore/job', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                backup_id: backupId,
                target_node_id: targetNodeId,
                skip_primary_channel: options.skipPrimaryChannel !== false,
                skip_lora: options.skipLora || false,
                skip_security: options.skipSecurity !== false,
                reboot_after: options.rebootAfter || false
            })
        });

        const result = await response.json();

        if (result.success && result.job_id) {
            this.startPolling(result.job_id);
        }

        return result;
    }

    /**
     * Get job details
     * @param {number} jobId - Job ID
     * @returns {Promise<Object>} Job details
     */
    async getJob(jobId) {
        const response = await fetch(`/api/jobs/${jobId}`);
        return await response.json();
    }

    /**
     * Get all active jobs
     * @param {string} nodeId - Optional node ID filter
     * @returns {Promise<Object>} Active jobs
     */
    async getActiveJobs(nodeId = null) {
        let url = '/api/jobs/active';
        if (nodeId) {
            url += `?node_id=${encodeURIComponent(nodeId)}`;
        }
        const response = await fetch(url);
        return await response.json();
    }

    /**
     * Get job service status
     * @returns {Promise<Object>} Service status and active jobs summary
     */
    async getServiceStatus() {
        const response = await fetch('/api/jobs/status');
        return await response.json();
    }

    /**
     * Get jobs with filtering
     * @param {Object} filters - Filter options
     * @returns {Promise<Object>} Jobs list
     */
    async getJobs(filters = {}) {
        const params = new URLSearchParams();
        if (filters.status) params.append('status', filters.status);
        if (filters.jobType) params.append('job_type', filters.jobType);
        if (filters.nodeId) params.append('node_id', filters.nodeId);
        if (filters.limit) params.append('limit', filters.limit);
        if (filters.includeCompleted !== undefined) {
            params.append('include_completed', filters.includeCompleted);
        }

        const response = await fetch(`/api/jobs?${params.toString()}`);
        return await response.json();
    }

    /**
     * Cancel a queued job
     * @param {number} jobId - Job ID to cancel
     * @returns {Promise<Object>} Cancellation result
     */
    async cancelJob(jobId) {
        const response = await fetch(`/api/jobs/${jobId}/cancel`, {
            method: 'POST'
        });

        const result = await response.json();

        if (result.success) {
            this.stopPolling(jobId);
        }

        return result;
    }

    /**
     * Pause a queued job
     * @param {number} jobId - Job ID to pause
     * @returns {Promise<Object>} Pause result
     */
    async pauseJob(jobId) {
        const response = await fetch(`/api/jobs/${jobId}/pause`, {
            method: 'POST'
        });

        const result = await response.json();

        if (result.success) {
            this.stopPolling(jobId);
        }

        return result;
    }

    /**
     * Resume a paused job
     * @param {number} jobId - Job ID to resume
     * @returns {Promise<Object>} Resume result
     */
    async resumeJob(jobId) {
        const response = await fetch(`/api/jobs/${jobId}/resume`, {
            method: 'POST'
        });

        const result = await response.json();

        if (result.success) {
            this.startPolling(jobId);
        }

        return result;
    }

    /**
     * Start polling for job updates
     * @param {number} jobId - Job ID to poll
     */
    startPolling(jobId) {
        if (this.activePolls.has(jobId)) {
            return; // Already polling
        }

        const poll = async () => {
            try {
                const job = await this.getJob(jobId);

                if (this.onJobUpdate) {
                    this.onJobUpdate(job);
                }

                // Check if job is complete
                if (job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled') {
                    this.stopPolling(jobId);
                    if (this.onJobComplete) {
                        this.onJobComplete(job);
                    }
                }
            } catch (error) {
                console.error(`Error polling job ${jobId}:`, error);
            }
        };

        // Poll immediately, then at interval
        poll();
        const interval = setInterval(poll, this.pollInterval);
        this.activePolls.set(jobId, interval);
    }

    /**
     * Stop polling for a job
     * @param {number} jobId - Job ID to stop polling
     */
    stopPolling(jobId) {
        const interval = this.activePolls.get(jobId);
        if (interval) {
            clearInterval(interval);
            this.activePolls.delete(jobId);
        }
    }

    /**
     * Stop all polling
     */
    stopAllPolling() {
        for (const [jobId, interval] of this.activePolls) {
            clearInterval(interval);
        }
        this.activePolls.clear();
    }

    /**
     * Start polling for queue updates
     * @param {number} interval - Poll interval in ms
     */
    startQueuePolling(interval = 5000) {
        if (this._queuePollInterval) {
            return;
        }

        const poll = async () => {
            try {
                const status = await this.getServiceStatus();

                if (this.onQueueUpdate) {
                    this.onQueueUpdate(status);
                }

                this.lastQueueStatus = status;
            } catch (error) {
                console.error('Error polling queue status:', error);
            }
        };

        poll();
        this._queuePollInterval = setInterval(poll, interval);
    }

    /**
     * Stop queue polling
     */
    stopQueuePolling() {
        if (this._queuePollInterval) {
            clearInterval(this._queuePollInterval);
            this._queuePollInterval = null;
        }
    }

    /**
     * Resume polling for any running/queued jobs
     * Call this when returning to the admin page
     */
    async resumePolling() {
        const { jobs } = await this.getActiveJobs();
        for (const job of jobs) {
            this.startPolling(job.id);
        }
    }
}


/**
 * Job Progress UI Component
 * Renders job progress in a container element
 */
class JobProgressUI {
    constructor(containerId, options = {}) {
        this.container = document.getElementById(containerId);
        this.options = {
            showQueue: options.showQueue !== false,
            showCompleted: options.showCompleted !== false,
            maxCompleted: options.maxCompleted || 5,
            ...options
        };

        this.jobManager = new JobManager({
            onJobUpdate: (job) => this.updateJobDisplay(job),
            onJobComplete: (job) => this.handleJobComplete(job),
            onQueueUpdate: (status) => this.updateQueueDisplay(status)
        });

        this.jobs = new Map();
        this.init();
    }

    init() {
        if (!this.container) {
            console.warn('JobProgressUI container not found');
            return;
        }

        this.render();
        this.jobManager.startQueuePolling();
        this.jobManager.resumePolling();
    }

    render() {
        this.container.innerHTML = `
            <div class="job-progress-panel">
                <div class="job-queue-header d-flex justify-content-between align-items-center mb-2">
                    <h6 class="mb-0">
                        <i class="bi bi-list-task me-2"></i>
                        Background Jobs
                        <span class="badge bg-secondary ms-2" id="jobQueueCount">0</span>
                    </h6>
                    <button class="btn btn-sm btn-outline-secondary" id="refreshJobsBtn" title="Refresh">
                        <i class="bi bi-arrow-clockwise"></i>
                    </button>
                </div>
                <div id="activeJobsList" class="job-list"></div>
                <div id="completedJobsList" class="job-list mt-2" style="display: none;">
                    <small class="text-muted">Recently completed:</small>
                </div>
                <div id="noJobsMessage" class="text-muted small py-2">
                    <i class="bi bi-check-circle me-1"></i>
                    No active jobs
                </div>
            </div>
        `;

        // Bind refresh button
        document.getElementById('refreshJobsBtn')?.addEventListener('click', () => {
            this.refresh();
        });
    }

    async refresh() {
        const status = await this.jobManager.getServiceStatus();
        this.updateQueueDisplay(status);
    }

    updateQueueDisplay(status) {
        const countBadge = document.getElementById('jobQueueCount');
        const activeList = document.getElementById('activeJobsList');
        const noJobsMsg = document.getElementById('noJobsMessage');

        if (!countBadge || !activeList) return;

        const totalActive = status.running_jobs + status.queued_jobs;
        countBadge.textContent = totalActive;
        countBadge.className = `badge ms-2 ${totalActive > 0 ? 'bg-primary' : 'bg-secondary'}`;

        if (status.active_jobs && status.active_jobs.length > 0) {
            noJobsMsg.style.display = 'none';

            // Update or create job cards
            for (const job of status.active_jobs) {
                this.updateJobDisplay(job);
                // Start polling for this job if not already
                this.jobManager.startPolling(job.id);
            }

            // Remove jobs that are no longer active
            const activeIds = new Set(status.active_jobs.map(j => j.id));
            for (const [jobId, element] of this.jobs) {
                if (!activeIds.has(jobId)) {
                    // Job completed, will be handled by onJobComplete
                }
            }
        } else {
            noJobsMsg.style.display = 'block';
        }
    }

    updateJobDisplay(job) {
        const activeList = document.getElementById('activeJobsList');
        const noJobsMsg = document.getElementById('noJobsMessage');

        if (!activeList) return;

        noJobsMsg.style.display = 'none';

        let card = this.jobs.get(job.id);

        if (!card) {
            card = document.createElement('div');
            card.className = 'job-card card mb-2';
            card.id = `job-card-${job.id}`;
            activeList.appendChild(card);
            this.jobs.set(job.id, card);
        }

        const statusIcon = this.getStatusIcon(job.status);
        const statusClass = this.getStatusClass(job.status);
        const progress = job.progress || 0;

        card.innerHTML = `
            <div class="card-body py-2 px-3">
                <div class="d-flex justify-content-between align-items-start mb-1">
                    <div>
                        <span class="badge ${statusClass} me-2">${statusIcon} ${job.status}</span>
                        <strong class="small">${this.escapeHtml(job.job_name)}</strong>
                    </div>
                    ${job.status === 'queued' ? `
                        <button class="btn btn-sm btn-outline-danger py-0 px-1"
                                onclick="jobProgressUI.cancelJob(${job.id})"
                                title="Cancel">
                            <i class="bi bi-x"></i>
                        </button>
                    ` : ''}
                </div>
                ${job.status === 'running' ? `
                    <div class="progress mb-1" style="height: 6px;">
                        <div class="progress-bar progress-bar-striped progress-bar-animated"
                             style="width: ${progress}%"></div>
                    </div>
                    <small class="text-muted">${this.escapeHtml(job.progress_message || 'Working...')}</small>
                ` : ''}
                ${job.status === 'queued' && job.queue_position ? `
                    <small class="text-muted">Queue position: ${job.queue_position}</small>
                ` : ''}
            </div>
        `;
    }

    handleJobComplete(job) {
        // Remove from active display
        const card = this.jobs.get(job.id);
        if (card) {
            card.remove();
            this.jobs.delete(job.id);
        }

        // Add to completed list if showing
        if (this.options.showCompleted) {
            this.addToCompletedList(job);
        }

        // Update no jobs message if needed
        const activeList = document.getElementById('activeJobsList');
        const noJobsMsg = document.getElementById('noJobsMessage');
        if (activeList && activeList.children.length === 0) {
            noJobsMsg.style.display = 'block';
        }

        // Trigger custom callback if provided
        if (this.options.onComplete) {
            this.options.onComplete(job);
        }

        // Show notification
        this.showJobNotification(job);
    }

    addToCompletedList(job) {
        const completedList = document.getElementById('completedJobsList');
        if (!completedList) return;

        completedList.style.display = 'block';

        const item = document.createElement('div');
        item.className = `small py-1 ${job.status === 'completed' ? 'text-success' : 'text-danger'}`;
        item.innerHTML = `
            <i class="bi ${job.status === 'completed' ? 'bi-check-circle' : 'bi-x-circle'}"></i>
            ${this.escapeHtml(job.job_name)}
            <span class="text-muted">- ${this.formatTimeAgo(job.completed_at)}</span>
        `;

        completedList.insertBefore(item, completedList.children[1]);

        // Limit completed items
        while (completedList.children.length > this.options.maxCompleted + 1) {
            completedList.removeChild(completedList.lastChild);
        }
    }

    showJobNotification(job) {
        const isSuccess = job.status === 'completed';
        const message = isSuccess
            ? `${job.job_name} completed successfully`
            : `${job.job_name} failed: ${job.error_message || 'Unknown error'}`;

        // Use the global showAlert if available
        if (typeof showAlert === 'function') {
            showAlert(isSuccess ? 'success' : 'danger', message);
        } else {
            console.log(`Job ${job.id} ${job.status}:`, message);
        }
    }

    async cancelJob(jobId) {
        if (!confirm('Cancel this job?')) return;

        const result = await this.jobManager.cancelJob(jobId);

        if (result.success) {
            const card = this.jobs.get(jobId);
            if (card) {
                card.remove();
                this.jobs.delete(jobId);
            }

            if (typeof showAlert === 'function') {
                showAlert('info', 'Job cancelled');
            }
        } else {
            if (typeof showAlert === 'function') {
                showAlert('warning', result.error || 'Could not cancel job');
            }
        }
    }

    getStatusIcon(status) {
        switch (status) {
            case 'queued': return '<i class="bi bi-hourglass"></i>';
            case 'running': return '<i class="bi bi-arrow-repeat spin"></i>';
            case 'completed': return '<i class="bi bi-check-circle"></i>';
            case 'failed': return '<i class="bi bi-x-circle"></i>';
            case 'cancelled': return '<i class="bi bi-slash-circle"></i>';
            default: return '<i class="bi bi-question-circle"></i>';
        }
    }

    getStatusClass(status) {
        switch (status) {
            case 'queued': return 'bg-secondary';
            case 'running': return 'bg-primary';
            case 'completed': return 'bg-success';
            case 'failed': return 'bg-danger';
            case 'cancelled': return 'bg-warning';
            default: return 'bg-secondary';
        }
    }

    formatTimeAgo(timestamp) {
        if (!timestamp) return '';
        const seconds = Math.floor(Date.now() / 1000 - timestamp);
        if (seconds < 60) return 'just now';
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
        if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
        return `${Math.floor(seconds / 86400)}d ago`;
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    destroy() {
        this.jobManager.stopAllPolling();
        this.jobManager.stopQueuePolling();
        this.jobs.clear();
    }
}


// Export for use in templates
window.JobManager = JobManager;
window.JobProgressUI = JobProgressUI;
