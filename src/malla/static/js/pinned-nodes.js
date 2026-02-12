/**
 * Pinned Nodes Manager
 * Provides a quick actions toolbar for frequently-used nodes
 * Persists to localStorage for cross-page functionality
 */

(function() {
    'use strict';

    const STORAGE_KEY = 'malla_pinned_nodes';
    const MAX_PINNED = 10;

    /**
     * Get pinned nodes from localStorage
     * @returns {Array} Array of pinned node objects
     */
    function getPinnedNodes() {
        try {
            const stored = localStorage.getItem(STORAGE_KEY);
            return stored ? JSON.parse(stored) : [];
        } catch (e) {
            console.error('Error reading pinned nodes:', e);
            return [];
        }
    }

    /**
     * Save pinned nodes to localStorage
     * @param {Array} nodes - Array of pinned node objects
     */
    function savePinnedNodes(nodes) {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(nodes));
        } catch (e) {
            console.error('Error saving pinned nodes:', e);
        }
    }

    /**
     * Pin a node
     * @param {Object} node - Node object with node_id and name
     * @returns {boolean} True if pinned successfully
     */
    function pinNode(node) {
        const nodes = getPinnedNodes();

        // Check if already pinned
        if (nodes.some(n => n.node_id === node.node_id)) {
            return false;
        }

        // Check max limit
        if (nodes.length >= MAX_PINNED) {
            return false;
        }

        nodes.push({
            node_id: node.node_id,
            hex_id: node.hex_id || `!${node.node_id.toString(16).padStart(8, '0')}`,
            name: node.name || node.long_name || node.short_name || 'Unnamed',
            pinned_at: Date.now()
        });

        savePinnedNodes(nodes);
        updatePinnedNodesUI();
        return true;
    }

    /**
     * Unpin a node
     * @param {number|string} nodeId - Node ID to unpin
     * @returns {boolean} True if unpinned successfully
     */
    function unpinNode(nodeId) {
        // Handle both numeric and string nodeId
        const numericId = typeof nodeId === 'string' ? parseInt(nodeId, 10) : nodeId;

        let nodes = getPinnedNodes();
        const originalLength = nodes.length;
        nodes = nodes.filter(n => n.node_id !== numericId);

        if (nodes.length === originalLength) {
            return false;
        }

        savePinnedNodes(nodes);
        updatePinnedNodesUI();
        return true;
    }

    /**
     * Check if a node is pinned
     * @param {number|string} nodeId - Node ID to check
     * @returns {boolean} True if pinned
     */
    function isNodePinned(nodeId) {
        const numericId = typeof nodeId === 'string' ? parseInt(nodeId, 10) : nodeId;
        return getPinnedNodes().some(n => n.node_id === numericId);
    }

    /**
     * Toggle pin state for a node
     * @param {Object} node - Node object
     * @returns {boolean} New pin state (true = pinned, false = unpinned)
     */
    function togglePin(node) {
        if (isNodePinned(node.node_id)) {
            unpinNode(node.node_id);
            return false;
        } else {
            return pinNode(node);
        }
    }

    /**
     * Render the pinned nodes panel HTML
     */
    function renderPinnedNodesPanel() {
        const nodes = getPinnedNodes();
        const panel = document.getElementById('pinnedNodesPanel');
        const content = document.getElementById('pinnedNodesList');
        const badge = document.getElementById('pinnedNodesBadge');
        const toggleBtn = document.getElementById('pinnedNodesToggle');

        if (!panel || !content) return;

        // Update badge count
        if (badge) {
            badge.textContent = nodes.length;
            badge.style.display = nodes.length > 0 ? 'inline' : 'none';
        }

        // Update toggle button visibility
        if (toggleBtn) {
            toggleBtn.style.display = nodes.length > 0 ? 'block' : 'none';
        }

        if (nodes.length === 0) {
            content.innerHTML = `
                <div class="text-center text-muted py-3">
                    <i class="bi bi-pin-angle fs-3"></i>
                    <p class="small mb-0 mt-2">No pinned nodes</p>
                    <p class="small text-muted">Pin nodes from their detail page</p>
                </div>`;
            return;
        }

        let html = '';
        nodes.forEach(node => {
            const escapedName = escapeHtml(node.name);
            const escapedHexId = escapeHtml(node.hex_id);

            html += `
                <div class="pinned-node-item d-flex align-items-center justify-content-between py-2 px-2 border-bottom">
                    <div class="pinned-node-info flex-grow-1 min-width-0">
                        <a href="/node/${node.node_id}" class="text-decoration-none fw-medium d-block text-truncate" title="${escapedName}">
                            ${escapedName}
                        </a>
                        <small class="text-muted">${escapedHexId}</small>
                    </div>
                    <div class="pinned-node-actions btn-group btn-group-sm ms-2">
                        <a href="/packets?from_node=${node.node_id}" class="btn btn-outline-secondary btn-sm" title="View packets">
                            <i class="bi bi-envelope"></i>
                        </a>
                        <a href="/traceroute?from_node=${node.node_id}" class="btn btn-outline-secondary btn-sm" title="View traceroutes">
                            <i class="bi bi-diagram-3"></i>
                        </a>
                        <button type="button" class="btn btn-outline-danger btn-sm" title="Unpin"
                                onclick="unpinNode(${node.node_id})">
                            <i class="bi bi-x-lg"></i>
                        </button>
                    </div>
                </div>`;
        });

        content.innerHTML = html;
    }

    /**
     * Escape HTML to prevent XSS
     */
    function escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    /**
     * Update all pinned nodes UI elements
     */
    function updatePinnedNodesUI() {
        renderPinnedNodesPanel();
        updatePinButtons();
    }

    /**
     * Update all pin buttons on the page to reflect current state
     */
    function updatePinButtons() {
        document.querySelectorAll('[data-pin-node-id]').forEach(btn => {
            const nodeId = parseInt(btn.getAttribute('data-pin-node-id'), 10);
            const isPinned = isNodePinned(nodeId);

            btn.classList.toggle('active', isPinned);
            btn.classList.toggle('btn-warning', isPinned);
            btn.classList.toggle('btn-outline-warning', !isPinned);

            const icon = btn.querySelector('i');
            if (icon) {
                icon.classList.toggle('bi-pin-fill', isPinned);
                icon.classList.toggle('bi-pin-angle', !isPinned);
            }

            btn.title = isPinned ? 'Unpin node' : 'Pin node';
        });
    }

    /**
     * Toggle pinned nodes panel visibility
     */
    function togglePinnedNodesPanel() {
        const panel = document.getElementById('pinnedNodesPanel');
        if (panel) {
            const isVisible = panel.classList.contains('show');
            if (isVisible) {
                panel.classList.remove('show');
            } else {
                panel.classList.add('show');
            }
        }
    }

    /**
     * Initialize pinned nodes feature
     */
    function initPinnedNodes() {
        // Render the panel on page load
        updatePinnedNodesUI();

        // Listen for pin button clicks
        document.addEventListener('click', function(e) {
            const pinBtn = e.target.closest('[data-pin-node-id]');
            if (pinBtn) {
                e.preventDefault();
                const nodeId = parseInt(pinBtn.getAttribute('data-pin-node-id'), 10);
                const nodeName = pinBtn.getAttribute('data-pin-node-name') || 'Unnamed';
                const nodeHexId = pinBtn.getAttribute('data-pin-node-hex') || `!${nodeId.toString(16).padStart(8, '0')}`;

                togglePin({
                    node_id: nodeId,
                    name: nodeName,
                    hex_id: nodeHexId
                });
            }
        });

        // Listen for toggle button clicks
        const toggleBtn = document.getElementById('pinnedNodesToggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', togglePinnedNodesPanel);
        }

        // Close panel when clicking outside
        document.addEventListener('click', function(e) {
            const panel = document.getElementById('pinnedNodesPanel');
            const toggleBtn = document.getElementById('pinnedNodesToggle');

            if (panel && panel.classList.contains('show')) {
                if (!panel.contains(e.target) && !toggleBtn.contains(e.target)) {
                    panel.classList.remove('show');
                }
            }
        });
    }

    // =========================================================================
    // Live Activity Popup System
    // =========================================================================

    const ACTIVITY_POLL_INTERVAL = 5000; // Poll every 5 seconds
    const ACTIVITY_POPUP_DURATION = 20000; // Show popup for 20 seconds
    const MAX_VISIBLE_POPUPS = 5;

    let lastActivityTimestamp = Date.now() / 1000;
    let activityPollTimer = null;
    let activePopups = new Map(); // Track active popups by event ID

    /**
     * Show an activity popup for a node
     * @param {Object} event - Activity event object
     * @param {boolean} isPinned - Whether this node is pinned
     */
    function showActivityPopup(event, isPinned) {
        const container = document.getElementById('nodeActivityContainer');
        if (!container) return;

        // Create unique ID for this popup
        const popupId = `activity-${event.id || Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

        // Limit number of visible popups
        if (activePopups.size >= MAX_VISIBLE_POPUPS) {
            // Remove oldest popup
            const oldestId = activePopups.keys().next().value;
            removePopup(oldestId);
        }

        const nodeName = event.from_name || event.node_name || `!${(event.from_node || event.node_id || 0).toString(16).padStart(8, '0')}`;
        const nodeId = event.from_node || event.node_id;
        const eventType = event.type || event.portnum_name || 'Activity';
        const timeAgo = formatTimeAgoShort(event.timestamp);

        const popup = document.createElement('div');
        popup.id = popupId;
        popup.className = `node-activity-popup${isPinned ? ' pinned' : ''}`;
        popup.innerHTML = `
            <div class="activity-header">
                ${isPinned ? '<i class="bi bi-pin-fill text-warning"></i>' : '<i class="bi bi-broadcast text-primary"></i>'}
                <a href="/node/${nodeId}" class="activity-node-name" title="View node details">${escapeHtml(nodeName)}</a>
                <button type="button" class="activity-close" onclick="dismissActivityPopup('${popupId}')" title="Dismiss">
                    <i class="bi bi-x"></i>
                </button>
            </div>
            <div class="activity-details">
                <span class="activity-type">${escapeHtml(eventType)}</span>
                <span class="activity-time">${timeAgo}</span>
            </div>
        `;

        container.appendChild(popup);
        activePopups.set(popupId, {
            element: popup,
            timer: setTimeout(() => removePopup(popupId), ACTIVITY_POPUP_DURATION)
        });
    }

    /**
     * Remove a popup by ID with fade animation
     * @param {string} popupId - Popup element ID
     */
    function removePopup(popupId) {
        const popupData = activePopups.get(popupId);
        if (!popupData) return;

        clearTimeout(popupData.timer);
        popupData.element.classList.add('fading');

        setTimeout(() => {
            if (popupData.element.parentNode) {
                popupData.element.parentNode.removeChild(popupData.element);
            }
            activePopups.delete(popupId);
        }, 300);
    }

    /**
     * Dismiss a popup (called from close button)
     * @param {string} popupId - Popup element ID
     */
    function dismissActivityPopup(popupId) {
        removePopup(popupId);
    }

    /**
     * Format time ago in short format
     * @param {number} timestamp - Unix timestamp
     * @returns {string} Short time ago string
     */
    function formatTimeAgoShort(timestamp) {
        const now = Math.floor(Date.now() / 1000);
        const diff = now - Math.floor(timestamp);

        if (diff < 60) return 'just now';
        if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
        if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
        return `${Math.floor(diff / 86400)}d ago`;
    }

    /**
     * Poll for new activity from pinned nodes
     */
    async function pollNodeActivity() {
        const pinnedNodes = getPinnedNodes();

        // Always poll if we have any pinned nodes to show their activity
        if (pinnedNodes.length === 0) {
            return;
        }

        try {
            const response = await fetch('/api/live/recent?limit=20');
            if (!response.ok) return;

            const data = await response.json();
            const events = data.events || [];

            // Filter to events newer than last check
            const newEvents = events.filter(e => e.timestamp > lastActivityTimestamp);

            if (newEvents.length > 0) {
                lastActivityTimestamp = Math.max(...newEvents.map(e => e.timestamp));

                // Get pinned node IDs for matching
                const pinnedNodeIds = new Set(pinnedNodes.map(n => n.node_id));

                // Show popups for pinned node activity (prioritize pinned)
                newEvents.forEach(event => {
                    // Event data is nested under event.data from the API
                    const eventData = event.data || {};
                    const nodeId = eventData.from_node || eventData.node_id || eventData.to_node;

                    if (nodeId == null) {
                        return;
                    }

                    const isPinned = pinnedNodeIds.has(nodeId);

                    // Only show activity for pinned nodes
                    if (isPinned) {
                        // Flatten event data for popup display
                        const flatEvent = {
                            ...eventData,
                            id: event.id,
                            type: eventData.type || event.type,
                            timestamp: event.timestamp,
                            severity: event.severity
                        };
                        showActivityPopup(flatEvent, true);
                    }
                });
            }
        } catch (error) {
            console.debug('Activity poll error:', error);
        }
    }

    /**
     * Start activity polling
     */
    function startActivityPolling() {
        if (activityPollTimer) return;

        // Initial poll
        pollNodeActivity();

        // Set up interval
        activityPollTimer = setInterval(pollNodeActivity, ACTIVITY_POLL_INTERVAL);
    }

    /**
     * Stop activity polling
     */
    function stopActivityPolling() {
        if (activityPollTimer) {
            clearInterval(activityPollTimer);
            activityPollTimer = null;
        }
    }

    // Export to global scope
    window.pinNode = pinNode;
    window.unpinNode = unpinNode;
    window.isNodePinned = isNodePinned;
    window.togglePin = togglePin;
    window.updatePinnedNodesUI = updatePinnedNodesUI;
    window.togglePinnedNodesPanel = togglePinnedNodesPanel;
    window.getPinnedNodes = getPinnedNodes;
    window.dismissActivityPopup = dismissActivityPopup;
    window.showActivityPopup = showActivityPopup;

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() {
            initPinnedNodes();
            startActivityPolling();
        });
    } else {
        initPinnedNodes();
        startActivityPolling();
    }
})();
