/**
 * Node Connection Manager
 *
 * Manages persistent connections to Meshtastic nodes (TCP/Serial) with:
 * - Connection state persistence across page navigation
 * - Auto-reconnect with exponential backoff (up to 1 minute)
 * - Global status indicator updates
 */

const NodeConnectionManager = (function() {
    'use strict';

    // Connection state
    let connectionState = {
        connected: false,
        connectionType: null,  // 'tcp', 'serial', 'mqtt'
        localNodeId: null,
        localNodeHex: null,
        tcpHost: null,
        tcpPort: null,
        serialPort: null,
        lastConnected: null,
        reconnectAttempts: 0,
        maxReconnectTime: 60000,  // 1 minute max retry time
        isReconnecting: false,
        userDisconnected: false,  // Track if user explicitly disconnected
    };

    // Reconnect timing
    let reconnectTimeout = null;
    let statusCheckInterval = null;
    const STATUS_CHECK_INTERVAL = 5000;  // Check every 5 seconds
    const INITIAL_RECONNECT_DELAY = 1000;  // Start with 1 second

    // Event callbacks
    let onConnectionChange = null;
    let onReconnectAttempt = null;

    /**
     * Initialize the connection manager
     */
    function init(options = {}) {
        if (options.onConnectionChange) {
            onConnectionChange = options.onConnectionChange;
        }
        if (options.onReconnectAttempt) {
            onReconnectAttempt = options.onReconnectAttempt;
        }

        // Load persisted connection settings
        loadPersistedSettings();

        // Start status monitoring
        startStatusMonitoring();

        // Initial status check
        checkStatus();

        console.log('[NodeConnectionManager] Initialized');
    }

    /**
     * Load connection settings from localStorage
     */
    function loadPersistedSettings() {
        try {
            const saved = localStorage.getItem('nodeConnectionSettings');
            if (saved) {
                const settings = JSON.parse(saved);
                connectionState.tcpHost = settings.tcpHost || null;
                connectionState.tcpPort = settings.tcpPort || null;
                connectionState.serialPort = settings.serialPort || null;
                connectionState.connectionType = settings.connectionType || null;
                connectionState.userDisconnected = settings.userDisconnected || false;
            }
        } catch (e) {
            console.warn('[NodeConnectionManager] Failed to load settings:', e);
        }
    }

    /**
     * Save connection settings to localStorage
     */
    function savePersistedSettings() {
        try {
            const settings = {
                tcpHost: connectionState.tcpHost,
                tcpPort: connectionState.tcpPort,
                serialPort: connectionState.serialPort,
                connectionType: connectionState.connectionType,
                userDisconnected: connectionState.userDisconnected,
            };
            localStorage.setItem('nodeConnectionSettings', JSON.stringify(settings));
        } catch (e) {
            console.warn('[NodeConnectionManager] Failed to save settings:', e);
        }
    }

    /**
     * Start periodic status monitoring
     */
    function startStatusMonitoring() {
        if (statusCheckInterval) {
            clearInterval(statusCheckInterval);
        }
        statusCheckInterval = setInterval(checkStatus, STATUS_CHECK_INTERVAL);
    }

    /**
     * Stop status monitoring
     */
    function stopStatusMonitoring() {
        if (statusCheckInterval) {
            clearInterval(statusCheckInterval);
            statusCheckInterval = null;
        }
    }

    /**
     * Check current connection status from server
     */
    async function checkStatus() {
        try {
            const response = await fetch('/api/admin/status');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            const data = await response.json();

            const wasConnected = connectionState.connected;

            // Update state from server
            connectionState.connected = data.connected;
            connectionState.connectionType = data.connection_type;

            if (data.connected) {
                connectionState.lastConnected = Date.now();
                connectionState.reconnectAttempts = 0;
                connectionState.isReconnecting = false;

                // Update connection details
                if (data.connection_type === 'tcp') {
                    connectionState.tcpHost = data.tcp_host;
                    connectionState.tcpPort = data.tcp_port;
                } else if (data.connection_type === 'serial') {
                    connectionState.serialPort = data.serial_port;
                }

                // Extract local node info if available
                if (data.local_node_id) {
                    connectionState.localNodeId = data.local_node_id;
                    connectionState.localNodeHex = data.local_node_hex || `!${data.local_node_id.toString(16).padStart(8, '0')}`;
                }
            }

            // Detect unexpected disconnection (was connected, now not, user didn't disconnect)
            if (wasConnected && !data.connected && !connectionState.userDisconnected) {
                console.log('[NodeConnectionManager] Connection lost, initiating auto-reconnect');
                scheduleReconnect();
            }

            // Update UI
            updateGlobalIndicator();

            if (onConnectionChange && wasConnected !== data.connected) {
                onConnectionChange(connectionState);
            }

            return data;
        } catch (error) {
            console.error('[NodeConnectionManager] Status check failed:', error);

            // If we thought we were connected, we might have lost connection
            if (connectionState.connected && !connectionState.userDisconnected) {
                connectionState.connected = false;
                updateGlobalIndicator();
                scheduleReconnect();
            }

            return null;
        }
    }

    /**
     * Schedule a reconnection attempt with exponential backoff
     */
    function scheduleReconnect() {
        if (connectionState.userDisconnected) {
            console.log('[NodeConnectionManager] User disconnected, not auto-reconnecting');
            return;
        }

        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
        }

        // Calculate delay with exponential backoff
        const delay = Math.min(
            INITIAL_RECONNECT_DELAY * Math.pow(2, connectionState.reconnectAttempts),
            connectionState.maxReconnectTime
        );

        // Check if we've exceeded max retry time
        const totalTimeElapsed = connectionState.reconnectAttempts > 0
            ? INITIAL_RECONNECT_DELAY * (Math.pow(2, connectionState.reconnectAttempts) - 1)
            : 0;

        if (totalTimeElapsed >= connectionState.maxReconnectTime) {
            console.log('[NodeConnectionManager] Max reconnect time exceeded, stopping auto-reconnect');
            connectionState.isReconnecting = false;
            updateGlobalIndicator();
            return;
        }

        connectionState.isReconnecting = true;
        connectionState.reconnectAttempts++;

        console.log(`[NodeConnectionManager] Scheduling reconnect attempt ${connectionState.reconnectAttempts} in ${delay}ms`);

        if (onReconnectAttempt) {
            onReconnectAttempt(connectionState.reconnectAttempts, delay);
        }

        updateGlobalIndicator();

        reconnectTimeout = setTimeout(attemptReconnect, delay);
    }

    /**
     * Attempt to reconnect based on last connection type
     */
    async function attemptReconnect() {
        if (connectionState.userDisconnected) {
            return;
        }

        console.log(`[NodeConnectionManager] Attempting reconnect (attempt ${connectionState.reconnectAttempts})`);

        try {
            let success = false;

            if (connectionState.connectionType === 'tcp' && connectionState.tcpHost) {
                success = await connectTcp(connectionState.tcpHost, connectionState.tcpPort, true);
            } else if (connectionState.connectionType === 'serial' && connectionState.serialPort) {
                success = await connectSerial(connectionState.serialPort, true);
            }

            if (success) {
                console.log('[NodeConnectionManager] Reconnect successful');
                connectionState.reconnectAttempts = 0;
                connectionState.isReconnecting = false;
            } else {
                // Schedule next attempt
                scheduleReconnect();
            }
        } catch (error) {
            console.error('[NodeConnectionManager] Reconnect failed:', error);
            scheduleReconnect();
        }
    }

    /**
     * Connect via TCP
     */
    async function connectTcp(host, port, isReconnect = false) {
        try {
            const response = await fetch('/api/admin/tcp/connect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ host, port: parseInt(port) || 4403 })
            });
            const data = await response.json();

            if (data.success) {
                connectionState.connected = true;
                connectionState.connectionType = 'tcp';
                connectionState.tcpHost = host;
                connectionState.tcpPort = port || 4403;
                connectionState.localNodeId = data.local_node_id;
                connectionState.localNodeHex = data.local_node_hex;
                connectionState.lastConnected = Date.now();
                connectionState.userDisconnected = false;
                connectionState.reconnectAttempts = 0;
                connectionState.isReconnecting = false;

                savePersistedSettings();
                updateGlobalIndicator();

                if (onConnectionChange) {
                    onConnectionChange(connectionState);
                }

                return true;
            }

            return false;
        } catch (error) {
            console.error('[NodeConnectionManager] TCP connect error:', error);
            return false;
        }
    }

    /**
     * Connect via Serial
     */
    async function connectSerial(port, isReconnect = false) {
        try {
            const response = await fetch('/api/admin/serial/connect', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ port })
            });
            const data = await response.json();

            if (data.success) {
                connectionState.connected = true;
                connectionState.connectionType = 'serial';
                connectionState.serialPort = port;
                connectionState.localNodeId = data.local_node_id;
                connectionState.localNodeHex = data.local_node_hex;
                connectionState.lastConnected = Date.now();
                connectionState.userDisconnected = false;
                connectionState.reconnectAttempts = 0;
                connectionState.isReconnecting = false;

                savePersistedSettings();
                updateGlobalIndicator();

                if (onConnectionChange) {
                    onConnectionChange(connectionState);
                }

                return true;
            }

            return false;
        } catch (error) {
            console.error('[NodeConnectionManager] Serial connect error:', error);
            return false;
        }
    }

    /**
     * Disconnect (user-initiated)
     */
    async function disconnect() {
        // Cancel any pending reconnect
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
            reconnectTimeout = null;
        }

        connectionState.userDisconnected = true;
        connectionState.isReconnecting = false;
        connectionState.reconnectAttempts = 0;

        try {
            let endpoint = '/api/admin/tcp/disconnect';
            if (connectionState.connectionType === 'serial') {
                endpoint = '/api/admin/serial/disconnect';
            }

            const response = await fetch(endpoint, { method: 'POST' });
            const data = await response.json();

            if (data.success) {
                connectionState.connected = false;
                connectionState.localNodeId = null;
                connectionState.localNodeHex = null;

                savePersistedSettings();
                updateGlobalIndicator();

                if (onConnectionChange) {
                    onConnectionChange(connectionState);
                }

                return true;
            }

            return false;
        } catch (error) {
            console.error('[NodeConnectionManager] Disconnect error:', error);
            return false;
        }
    }

    /**
     * Update the global connection indicator in the header
     */
    function updateGlobalIndicator() {
        const indicator = document.getElementById('node-connection-indicator');
        if (!indicator) return;

        const iconEl = indicator.querySelector('.connection-icon');
        const textEl = indicator.querySelector('.connection-text');

        if (!iconEl || !textEl) return;

        // Remove all state classes
        indicator.classList.remove('connected', 'disconnected', 'reconnecting');

        if (connectionState.connected) {
            indicator.classList.add('connected');
            iconEl.className = 'bi bi-broadcast connection-icon';

            let text = 'Connected';
            if (connectionState.localNodeHex) {
                text = connectionState.localNodeHex;
            }
            textEl.textContent = text;

            // Update tooltip
            let tooltip = `Connected via ${connectionState.connectionType?.toUpperCase() || 'Unknown'}`;
            if (connectionState.connectionType === 'tcp') {
                tooltip += `\n${connectionState.tcpHost}:${connectionState.tcpPort}`;
            } else if (connectionState.connectionType === 'serial') {
                tooltip += `\n${connectionState.serialPort}`;
            }
            indicator.title = tooltip;

        } else if (connectionState.isReconnecting) {
            indicator.classList.add('reconnecting');
            iconEl.className = 'bi bi-arrow-repeat connection-icon spin';
            textEl.textContent = `Retry ${connectionState.reconnectAttempts}...`;
            indicator.title = 'Attempting to reconnect...';

        } else {
            indicator.classList.add('disconnected');
            iconEl.className = 'bi bi-broadcast connection-icon';
            textEl.textContent = 'No Node';
            indicator.title = 'Not connected to any node. Go to Admin page to connect.';
        }
    }

    /**
     * Get current connection state
     */
    function getState() {
        return { ...connectionState };
    }

    /**
     * Check if connected
     */
    function isConnected() {
        return connectionState.connected;
    }

    /**
     * Reset user disconnect flag (allows auto-reconnect again)
     */
    function allowAutoReconnect() {
        connectionState.userDisconnected = false;
        savePersistedSettings();
    }

    // Public API
    return {
        init,
        checkStatus,
        connectTcp,
        connectSerial,
        disconnect,
        getState,
        isConnected,
        allowAutoReconnect,
        updateGlobalIndicator,
    };
})();

// Auto-initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    NodeConnectionManager.init();
});
