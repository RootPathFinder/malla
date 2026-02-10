/**
 * Custom Dashboard Manager
 *
 * Provides a fully user-configurable dashboard that persists to localStorage.
 * Users can add/remove widgets, pick nodes and metrics, rename dashboards,
 * and manage multiple dashboard tabs.
 */
(function () {
    'use strict';

    const STORAGE_KEY = 'malla_custom_dashboards_v1';
    const REFRESH_INTERVAL_MS = 30000; // 30 seconds
    const MAX_DASHBOARDS = 20;
    const MAX_WIDGETS = 50;

    // ── Metric Definitions ──────────────────────────────────────
    const METRIC_CATEGORIES = {
        device: {
            label: 'Device Metrics',
            icon: 'bi-cpu',
            metrics: {
                battery_level: { label: 'Battery Level', unit: '%', icon: 'bi-battery-half', thresholds: { danger: 20, warning: 50 } },
                voltage: { label: 'Voltage', unit: 'V', icon: 'bi-lightning-charge', thresholds: { danger: 3.3, warning: 3.6 }, thresholdDir: 'below' },
                channel_utilization: { label: 'Channel Util', unit: '%', icon: 'bi-broadcast', thresholds: { warning: 50, danger: 75 } },
                air_util_tx: { label: 'Air Util TX', unit: '%', icon: 'bi-send', thresholds: { warning: 10, danger: 25 } },
                uptime_seconds: { label: 'Uptime', unit: '', icon: 'bi-clock-history', format: 'duration' },
            }
        },
        environment: {
            label: 'Environment',
            icon: 'bi-thermometer-half',
            metrics: {
                temperature: { label: 'Temperature', unit: '°C', icon: 'bi-thermometer-half', thresholds: { warning: 45, danger: 60 } },
                relative_humidity: { label: 'Humidity', unit: '%', icon: 'bi-droplet', thresholds: { warning: 80, danger: 95 } },
                barometric_pressure: { label: 'Pressure', unit: 'hPa', icon: 'bi-speedometer2' },
                gas_resistance: { label: 'Gas Resistance', unit: 'Ω', icon: 'bi-wind' },
            }
        },
        power: {
            label: 'Power Metrics',
            icon: 'bi-plug',
            metrics: {
                ch1_voltage: { label: 'CH1 Voltage', unit: 'V', icon: 'bi-lightning' },
                ch1_current: { label: 'CH1 Current', unit: 'mA', icon: 'bi-lightning' },
                ch2_voltage: { label: 'CH2 Voltage', unit: 'V', icon: 'bi-lightning' },
                ch2_current: { label: 'CH2 Current', unit: 'mA', icon: 'bi-lightning' },
                ch3_voltage: { label: 'CH3 Voltage', unit: 'V', icon: 'bi-lightning' },
                ch3_current: { label: 'CH3 Current', unit: 'mA', icon: 'bi-lightning' },
            }
        }
    };

    const WIDGET_TYPES = {
        single_metric: { label: 'Single Metric', icon: 'bi-speedometer', desc: 'Large display of one metric for one node', multiNode: false, multiMetric: false },
        multi_metric: { label: 'Multi Metric', icon: 'bi-grid-3x2', desc: 'Multiple metrics for one node', multiNode: false, multiMetric: true },
        node_status: { label: 'Node Status', icon: 'bi-card-checklist', desc: 'Overview status card for a node', multiNode: false, multiMetric: false, autoMetrics: true },
        multi_node_compare: { label: 'Multi-Node Compare', icon: 'bi-people', desc: 'Compare one metric across nodes', multiNode: true, multiMetric: false },
    };

    // ── State ───────────────────────────────────────────────────
    let dashboards = [];
    let activeDashboardId = null;
    let refreshTimer = null;
    let isRefreshing = false;
    let nodeSearchTimeout = null;

    // ── Persistence ─────────────────────────────────────────────
    function loadDashboards() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            if (raw) {
                const parsed = JSON.parse(raw);
                if (Array.isArray(parsed)) {
                    dashboards = parsed;
                }
            }
        } catch (e) {
            console.warn('CustomDashboard: Failed to load from localStorage:', e);
        }

        // If no dashboards, create a default one
        if (dashboards.length === 0) {
            dashboards.push(createDashboard('My Dashboard'));
        }

        // Set active dashboard
        if (!activeDashboardId || !dashboards.find(d => d.id === activeDashboardId)) {
            activeDashboardId = dashboards[0].id;
        }
    }

    function saveDashboards() {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(dashboards));
        } catch (e) {
            console.warn('CustomDashboard: Failed to save to localStorage:', e);
        }
    }

    function createDashboard(name) {
        return {
            id: 'db_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
            name: name || 'Untitled Dashboard',
            widgets: [],
            createdAt: Date.now(),
            updatedAt: Date.now(),
        };
    }

    function getActiveDashboard() {
        return dashboards.find(d => d.id === activeDashboardId) || dashboards[0];
    }

    // ── Initialization ──────────────────────────────────────────
    function init() {
        loadDashboards();
        renderToolbar();
        renderWidgets();
        startAutoRefresh();
        refreshAllWidgets();
    }

    // ── Toolbar ─────────────────────────────────────────────────
    function renderToolbar() {
        const toolbar = document.getElementById('dashboard-toolbar');
        if (!toolbar) return;

        const db = getActiveDashboard();

        toolbar.innerHTML = `
            <div class="btn-group" role="group">
                <button class="btn btn-sm btn-outline-secondary dropdown-toggle" data-bs-toggle="dropdown" aria-expanded="false" title="Switch dashboard">
                    <i class="bi bi-collection"></i>
                </button>
                <ul class="dropdown-menu">
                    ${dashboards.map(d => `
                        <li>
                            <a class="dropdown-item ${d.id === activeDashboardId ? 'active' : ''}" href="#" data-action="switch-dashboard" data-id="${d.id}">
                                ${escapeHtml(d.name)}
                                <span class="badge bg-secondary ms-1">${d.widgets.length}</span>
                            </a>
                        </li>
                    `).join('')}
                    <li><hr class="dropdown-divider"></li>
                    <li>
                        <a class="dropdown-item text-primary" href="#" data-action="new-dashboard">
                            <i class="bi bi-plus-circle"></i> New Dashboard
                        </a>
                    </li>
                </ul>
            </div>

            <span class="dashboard-name-display" id="dashboard-name-display" title="Click to rename">${escapeHtml(db.name)}</span>
            <input type="text" class="form-control form-control-sm dashboard-name-input d-none" id="dashboard-name-input" value="${escapeHtml(db.name)}">

            <div class="refresh-indicator" id="refresh-indicator">
                <span class="text-muted">Auto-refresh: 30s</span>
            </div>

            <div class="dashboard-actions">
                <button class="btn btn-sm btn-primary" data-action="add-widget" title="Add Widget">
                    <i class="bi bi-plus-lg"></i> Add Widget
                </button>
                <button class="btn btn-sm btn-outline-secondary" data-action="refresh" title="Refresh Now">
                    <i class="bi bi-arrow-clockwise"></i>
                </button>
                <button class="btn btn-sm btn-outline-danger" data-action="delete-dashboard" title="Delete Dashboard" ${dashboards.length <= 1 ? 'disabled' : ''}>
                    <i class="bi bi-trash"></i>
                </button>
            </div>
        `;

        // Bind events
        toolbar.querySelectorAll('[data-action]').forEach(el => {
            el.addEventListener('click', handleToolbarAction);
        });

        // Dashboard name edit
        const nameDisplay = document.getElementById('dashboard-name-display');
        const nameInput = document.getElementById('dashboard-name-input');

        nameDisplay.addEventListener('click', () => {
            nameDisplay.classList.add('d-none');
            nameInput.classList.remove('d-none');
            nameInput.focus();
            nameInput.select();
        });

        nameInput.addEventListener('blur', () => finishRename());
        nameInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') finishRename();
            if (e.key === 'Escape') {
                nameInput.value = db.name;
                finishRename();
            }
        });
    }

    function finishRename() {
        const nameDisplay = document.getElementById('dashboard-name-display');
        const nameInput = document.getElementById('dashboard-name-input');
        if (!nameDisplay || !nameInput) return;

        const newName = nameInput.value.trim() || 'Untitled Dashboard';
        const db = getActiveDashboard();
        db.name = newName;
        db.updatedAt = Date.now();
        saveDashboards();

        nameDisplay.textContent = newName;
        nameDisplay.classList.remove('d-none');
        nameInput.classList.add('d-none');
    }

    function handleToolbarAction(e) {
        e.preventDefault();
        const action = e.currentTarget.dataset.action;

        switch (action) {
            case 'add-widget':
                showAddWidgetModal();
                break;
            case 'refresh':
                refreshAllWidgets();
                break;
            case 'delete-dashboard':
                deleteDashboard();
                break;
            case 'new-dashboard':
                newDashboard();
                break;
            case 'switch-dashboard':
                switchDashboard(e.currentTarget.dataset.id);
                break;
        }
    }

    // ── Dashboard CRUD ──────────────────────────────────────────
    function newDashboard() {
        if (dashboards.length >= MAX_DASHBOARDS) {
            alert(`Maximum of ${MAX_DASHBOARDS} dashboards allowed.`);
            return;
        }
        const name = prompt('Dashboard name:', `Dashboard ${dashboards.length + 1}`);
        if (name === null) return;

        const db = createDashboard(name);
        dashboards.push(db);
        activeDashboardId = db.id;
        saveDashboards();
        renderToolbar();
        renderWidgets();
    }

    function deleteDashboard() {
        if (dashboards.length <= 1) return;
        const db = getActiveDashboard();
        if (!confirm(`Delete dashboard "${db.name}"? This cannot be undone.`)) return;

        dashboards = dashboards.filter(d => d.id !== db.id);
        activeDashboardId = dashboards[0].id;
        saveDashboards();
        renderToolbar();
        renderWidgets();
        refreshAllWidgets();
    }

    function switchDashboard(id) {
        activeDashboardId = id;
        renderToolbar();
        renderWidgets();
        refreshAllWidgets();
    }

    // ── Widget Rendering ────────────────────────────────────────
    function renderWidgets() {
        const container = document.getElementById('widget-grid');
        if (!container) return;

        const db = getActiveDashboard();

        if (db.widgets.length === 0) {
            container.innerHTML = `
                <div class="widget-grid-empty">
                    <i class="bi bi-grid-1x2"></i>
                    <h5>No widgets yet</h5>
                    <p>Add widgets to monitor your nodes. Choose from battery levels, temperature, signal quality, and more.</p>
                    <button class="btn btn-primary" data-action="add-widget-empty">
                        <i class="bi bi-plus-lg"></i> Add Your First Widget
                    </button>
                </div>
            `;
            container.querySelector('[data-action="add-widget-empty"]')
                ?.addEventListener('click', () => showAddWidgetModal());
            return;
        }

        container.innerHTML = db.widgets.map((widget, index) => renderWidgetCard(widget, index)).join('');

        // Setup drag and drop
        setupDragAndDrop(container);
    }

    function renderWidgetCard(widget, index) {
        const typeInfo = WIDGET_TYPES[widget.type] || {};
        const nodeLabel = widget.nodes?.length > 1
            ? `${widget.nodes.length} nodes`
            : (widget.nodeNames?.[0] || widget.nodes?.[0] || 'No node');

        return `
            <div class="widget-card" data-widget-index="${index}" draggable="true">
                <div class="widget-card-header">
                    <i class="bi bi-grip-vertical drag-handle"></i>
                    <span class="widget-title">${escapeHtml(widget.title || typeInfo.label || 'Widget')}</span>
                    <span class="widget-node-badge" title="${escapeHtml(nodeLabel)}">${escapeHtml(nodeLabel)}</span>
                    <div class="widget-actions">
                        <button class="btn btn-sm btn-outline-secondary" data-action="edit-widget" data-index="${index}" title="Edit">
                            <i class="bi bi-pencil"></i>
                        </button>
                        <button class="btn btn-sm btn-outline-danger" data-action="delete-widget" data-index="${index}" title="Remove">
                            <i class="bi bi-x-lg"></i>
                        </button>
                    </div>
                </div>
                <div class="widget-card-body" id="widget-body-${index}">
                    <div class="widget-loading">
                        <div class="spinner-border spinner-border-sm text-secondary" role="status">
                            <span class="visually-hidden">Loading...</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    // ── Widget Content Rendering ────────────────────────────────
    function renderWidgetContent(widget, index, telemetryData) {
        const body = document.getElementById(`widget-body-${index}`);
        if (!body) return;

        // Wire up action buttons
        const card = body.closest('.widget-card');
        card.querySelector('[data-action="edit-widget"]')?.addEventListener('click', () => showEditWidgetModal(index));
        card.querySelector('[data-action="delete-widget"]')?.addEventListener('click', () => deleteWidget(index));

        if (!telemetryData || Object.keys(telemetryData).length === 0) {
            body.innerHTML = '<div class="widget-no-data"><i class="bi bi-inbox"></i><span>No data available</span></div>';
            return;
        }

        switch (widget.type) {
            case 'single_metric':
                renderSingleMetric(body, widget, telemetryData);
                break;
            case 'multi_metric':
                renderMultiMetric(body, widget, telemetryData);
                break;
            case 'node_status':
                renderNodeStatus(body, widget, telemetryData);
                break;
            case 'multi_node_compare':
                renderMultiNodeCompare(body, widget, telemetryData);
                break;
            default:
                body.innerHTML = '<div class="widget-error">Unknown widget type</div>';
        }
    }

    function renderSingleMetric(body, widget, telemetryData) {
        const nodeId = widget.nodes[0];
        const nodeData = telemetryData[nodeId];
        if (!nodeData?.telemetry) {
            body.innerHTML = '<div class="widget-no-data"><i class="bi bi-inbox"></i><span>No telemetry data</span></div>';
            return;
        }

        const metricKey = widget.metrics[0];
        const metricDef = findMetricDef(metricKey);
        const value = extractMetricValue(nodeData.telemetry, metricKey);

        if (value === null || value === undefined) {
            body.innerHTML = `<div class="widget-no-data"><i class="bi bi-inbox"></i><span>No ${metricDef?.label || metricKey} data</span></div>`;
            return;
        }

        const formatted = formatMetricValue(value, metricDef);
        const statusClass = getStatusClass(value, metricDef);
        const lastUpdated = nodeData.node_info?.last_updated
            ? formatTimeAgo(nodeData.node_info.last_updated)
            : '';

        body.innerHTML = `
            <div class="widget-value-display">
                <div class="metric-value ${statusClass}">${formatted}<span class="metric-unit">${metricDef?.unit || ''}</span></div>
                <div class="metric-label">${metricDef?.label || metricKey}</div>
                ${lastUpdated ? `<div class="metric-timestamp">Updated ${lastUpdated}</div>` : ''}
            </div>
        `;
    }

    function renderMultiMetric(body, widget, telemetryData) {
        const nodeId = widget.nodes[0];
        const nodeData = telemetryData[nodeId];
        if (!nodeData?.telemetry) {
            body.innerHTML = '<div class="widget-no-data"><i class="bi bi-inbox"></i><span>No telemetry data</span></div>';
            return;
        }

        const metricsHtml = widget.metrics.map(metricKey => {
            const metricDef = findMetricDef(metricKey);
            const value = extractMetricValue(nodeData.telemetry, metricKey);
            const formatted = value !== null && value !== undefined ? formatMetricValue(value, metricDef) : '—';
            const statusClass = value !== null ? getStatusClass(value, metricDef) : 'status-unknown';

            return `
                <div class="mini-metric">
                    <div class="mini-value ${statusClass}">${formatted}<small>${metricDef?.unit || ''}</small></div>
                    <div class="mini-label">${metricDef?.label || metricKey}</div>
                </div>
            `;
        }).join('');

        body.innerHTML = `<div class="widget-multi-metrics">${metricsHtml}</div>`;
    }

    function renderNodeStatus(body, widget, telemetryData) {
        const nodeId = widget.nodes[0];
        const nodeData = telemetryData[nodeId];
        if (!nodeData) {
            body.innerHTML = '<div class="widget-no-data"><i class="bi bi-inbox"></i><span>No node data</span></div>';
            return;
        }

        const info = nodeData.node_info || {};
        const telemetry = nodeData.telemetry || {};
        const deviceMetrics = telemetry.device_metrics || {};
        const envMetrics = telemetry.environment_metrics || {};

        const rows = [];

        // Always show basic info
        if (info.hw_model) rows.push({ label: 'Hardware', value: info.hw_model });
        if (info.role) rows.push({ label: 'Role', value: info.role });
        if (info.firmware_version) rows.push({ label: 'Firmware', value: info.firmware_version });

        // Device metrics
        if (deviceMetrics.battery_level !== undefined && deviceMetrics.battery_level !== null) {
            const bl = deviceMetrics.battery_level;
            const cls = bl <= 20 ? 'text-danger' : bl <= 50 ? 'text-warning' : 'text-success';
            rows.push({ label: 'Battery', value: `<span class="${cls}">${Math.round(bl)}%</span>` });
        }
        if (deviceMetrics.voltage !== undefined && deviceMetrics.voltage !== null) {
            rows.push({ label: 'Voltage', value: `${Number(deviceMetrics.voltage).toFixed(2)}V` });
        }
        if (deviceMetrics.channel_utilization !== undefined && deviceMetrics.channel_utilization !== null) {
            rows.push({ label: 'Ch. Util', value: `${Number(deviceMetrics.channel_utilization).toFixed(1)}%` });
        }
        if (deviceMetrics.air_util_tx !== undefined && deviceMetrics.air_util_tx !== null) {
            rows.push({ label: 'Air Util TX', value: `${Number(deviceMetrics.air_util_tx).toFixed(1)}%` });
        }
        if (deviceMetrics.uptime_seconds) {
            rows.push({ label: 'Uptime', value: formatDuration(deviceMetrics.uptime_seconds) });
        }

        // Environment metrics
        if (envMetrics.temperature !== undefined && envMetrics.temperature !== null) {
            rows.push({ label: 'Temperature', value: `${Number(envMetrics.temperature).toFixed(1)}°C` });
        }
        if (envMetrics.relative_humidity !== undefined && envMetrics.relative_humidity !== null) {
            rows.push({ label: 'Humidity', value: `${Number(envMetrics.relative_humidity).toFixed(1)}%` });
        }

        // Power type
        if (info.power_type && info.power_type !== 'unknown') {
            rows.push({ label: 'Power Type', value: capitalize(info.power_type) });
        }

        // Last seen
        if (info.last_updated) {
            rows.push({ label: 'Last Seen', value: formatTimeAgo(info.last_updated) });
        }

        if (rows.length === 0) {
            body.innerHTML = '<div class="widget-no-data"><i class="bi bi-inbox"></i><span>No status data</span></div>';
            return;
        }

        body.innerHTML = `
            <div class="widget-node-status">
                ${rows.map(r => `
                    <div class="status-row">
                        <span class="status-label">${r.label}</span>
                        <span class="status-value">${r.value}</span>
                    </div>
                `).join('')}
            </div>
        `;
    }

    function renderMultiNodeCompare(body, widget, telemetryData) {
        const metricKey = widget.metrics[0];
        const metricDef = findMetricDef(metricKey);

        const rows = widget.nodes.map(nodeId => {
            const nodeData = telemetryData[nodeId];
            const nodeName = widget.nodeNames?.[widget.nodes.indexOf(nodeId)] || nodeId;
            const value = nodeData?.telemetry ? extractMetricValue(nodeData.telemetry, metricKey) : null;
            const formatted = value !== null && value !== undefined ? formatMetricValue(value, metricDef) : '—';
            const statusClass = value !== null && value !== undefined ? getStatusClass(value, metricDef) : 'status-unknown';

            return `
                <div class="status-row">
                    <span class="status-label">${escapeHtml(nodeName)}</span>
                    <span class="status-value ${statusClass}">${formatted}${metricDef?.unit ? ' ' + metricDef.unit : ''}</span>
                </div>
            `;
        }).join('');

        body.innerHTML = `
            <div class="widget-node-status">
                <div class="status-row" style="border-bottom: 2px solid var(--bs-border-color);">
                    <span class="status-label" style="font-weight: 600;">Node</span>
                    <span class="status-value" style="font-weight: 600;">${metricDef?.label || metricKey}</span>
                </div>
                ${rows}
            </div>
        `;
    }

    // ── Data Fetching ───────────────────────────────────────────
    function refreshAllWidgets() {
        const db = getActiveDashboard();
        if (!db || db.widgets.length === 0) return;
        if (isRefreshing) return;

        isRefreshing = true;
        updateRefreshIndicator(true);

        // Collect all unique node IDs
        const allNodeIds = [...new Set(db.widgets.flatMap(w => w.nodes || []))];

        if (allNodeIds.length === 0) {
            isRefreshing = false;
            updateRefreshIndicator(false);
            return;
        }

        fetch('/api/custom-dashboard/nodes/telemetry', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_ids: allNodeIds })
        })
            .then(r => r.json())
            .then(data => {
                if (data.nodes) {
                    db.widgets.forEach((widget, index) => {
                        const widgetData = {};
                        (widget.nodes || []).forEach(nid => {
                            if (data.nodes[nid]) widgetData[nid] = data.nodes[nid];
                        });
                        renderWidgetContent(widget, index, widgetData);
                    });
                }
            })
            .catch(err => {
                console.error('CustomDashboard: Refresh failed:', err);
                db.widgets.forEach((widget, index) => {
                    const body = document.getElementById(`widget-body-${index}`);
                    if (body) {
                        body.innerHTML = '<div class="widget-error"><i class="bi bi-exclamation-triangle"></i> Failed to load data</div>';
                    }
                });
            })
            .finally(() => {
                isRefreshing = false;
                updateRefreshIndicator(false);
            });
    }

    function updateRefreshIndicator(loading) {
        const el = document.getElementById('refresh-indicator');
        if (!el) return;
        if (loading) {
            el.innerHTML = '<div class="spinner-border spinner-border-sm text-primary" role="status"></div> <span>Refreshing...</span>';
        } else {
            el.innerHTML = '<span class="text-muted">Auto-refresh: 30s</span>';
        }
    }

    function startAutoRefresh() {
        if (refreshTimer) clearInterval(refreshTimer);
        refreshTimer = setInterval(() => refreshAllWidgets(), REFRESH_INTERVAL_MS);
    }

    // ── Add Widget Modal ────────────────────────────────────────
    function showAddWidgetModal() {
        const db = getActiveDashboard();
        if (db.widgets.length >= MAX_WIDGETS) {
            alert(`Maximum of ${MAX_WIDGETS} widgets per dashboard.`);
            return;
        }

        const modal = createModal('add-widget-modal', 'Add Widget', buildAddWidgetForm());
        document.body.appendChild(modal);
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();

        // Setup form interactions
        setupWidgetForm(modal, null);

        modal.addEventListener('hidden.bs.modal', () => {
            modal.remove();
        });
    }

    function showEditWidgetModal(widgetIndex) {
        const db = getActiveDashboard();
        const widget = db.widgets[widgetIndex];
        if (!widget) return;

        const modal = createModal('edit-widget-modal', 'Edit Widget', buildAddWidgetForm(widget));
        document.body.appendChild(modal);
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();

        setupWidgetForm(modal, widget, widgetIndex);

        modal.addEventListener('hidden.bs.modal', () => {
            modal.remove();
        });
    }

    function buildAddWidgetForm(existingWidget) {
        const isEdit = !!existingWidget;

        return `
            <form id="widget-form">
                <!-- Step 1: Widget Type -->
                <div class="mb-3">
                    <label class="form-label fw-semibold">Widget Type</label>
                    <div class="widget-type-grid">
                        ${Object.entries(WIDGET_TYPES).map(([key, type]) => `
                            <div class="widget-type-option ${existingWidget?.type === key ? 'selected' : ''}" data-type="${key}">
                                <i class="bi ${type.icon}"></i>
                                <div class="type-name">${type.label}</div>
                                <div class="type-desc">${type.desc}</div>
                            </div>
                        `).join('')}
                    </div>
                    <input type="hidden" id="widget-type" value="${existingWidget?.type || ''}">
                </div>

                <!-- Step 2: Widget Title -->
                <div class="mb-3">
                    <label class="form-label fw-semibold" for="widget-title-input">Title</label>
                    <input type="text" class="form-control" id="widget-title-input"
                           placeholder="e.g., Living Room Node Battery"
                           value="${escapeHtml(existingWidget?.title || '')}">
                </div>

                <!-- Step 3: Node Selection -->
                <div class="mb-3" id="node-selection-group">
                    <label class="form-label fw-semibold">Select Node(s)</label>
                    <div class="dashboard-node-search">
                        <input type="text" class="form-control" id="widget-node-search"
                               placeholder="Search by node name or ID..." autocomplete="off">
                        <div class="search-results" id="widget-node-results"></div>
                    </div>
                    <div class="selected-nodes-tags" id="selected-nodes-tags">
                        ${(existingWidget?.nodes || []).map((nid, i) => `
                            <span class="selected-node-tag" data-node-id="${escapeHtml(nid)}">
                                ${escapeHtml(existingWidget?.nodeNames?.[i] || nid)}
                                <span class="tag-remove" data-node-id="${escapeHtml(nid)}">&times;</span>
                            </span>
                        `).join('')}
                    </div>
                </div>

                <!-- Step 4: Metric Selection -->
                <div class="mb-3" id="metric-selection-group" style="display: none;">
                    <label class="form-label fw-semibold">Select Metric(s)</label>
                    <div id="metric-picker">
                        ${Object.entries(METRIC_CATEGORIES).map(([catKey, cat]) => `
                            <div class="metric-category">
                                <div class="metric-category-title"><i class="bi ${cat.icon}"></i> ${cat.label}</div>
                                ${Object.entries(cat.metrics).map(([mKey, m]) => `
                                    <div class="metric-option ${existingWidget?.metrics?.includes(mKey) ? 'selected' : ''}" data-metric="${mKey}">
                                        <input type="checkbox" ${existingWidget?.metrics?.includes(mKey) ? 'checked' : ''} data-metric="${mKey}">
                                        <span class="metric-name"><i class="bi ${m.icon}"></i> ${m.label} ${m.unit ? '(' + m.unit + ')' : ''}</span>
                                    </div>
                                `).join('')}
                            </div>
                        `).join('')}
                    </div>
                </div>

                <div class="d-flex justify-content-end gap-2 mt-4">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                    <button type="submit" class="btn btn-primary" id="widget-save-btn">
                        <i class="bi bi-check-lg"></i> ${isEdit ? 'Update Widget' : 'Add Widget'}
                    </button>
                </div>
            </form>
        `;
    }

    function setupWidgetForm(modal, existingWidget, editIndex) {
        const form = modal.querySelector('#widget-form');
        const typeInput = modal.querySelector('#widget-type');
        const typeOptions = modal.querySelectorAll('.widget-type-option');
        const metricGroup = modal.querySelector('#metric-selection-group');
        const nodeSearch = modal.querySelector('#widget-node-search');
        const nodeResults = modal.querySelector('#widget-node-results');
        const nodesTagContainer = modal.querySelector('#selected-nodes-tags');

        // Track selections
        let selectedNodes = existingWidget?.nodes?.map((nid, i) => ({
            id: nid,
            name: existingWidget.nodeNames?.[i] || nid
        })) || [];
        let selectedMetrics = existingWidget?.metrics?.slice() || [];
        let selectedType = existingWidget?.type || '';
        let allowMultiNode = false;
        let allowMultiMetric = false;

        // Widget type selection
        typeOptions.forEach(opt => {
            opt.addEventListener('click', () => {
                typeOptions.forEach(o => o.classList.remove('selected'));
                opt.classList.add('selected');
                selectedType = opt.dataset.type;
                typeInput.value = selectedType;

                const typeInfo = WIDGET_TYPES[selectedType];
                allowMultiNode = typeInfo?.multiNode || false;
                allowMultiMetric = typeInfo?.multiMetric || false;

                // Show/hide metric selection based on type
                if (typeInfo?.autoMetrics) {
                    metricGroup.style.display = 'none';
                } else {
                    metricGroup.style.display = 'block';
                    // Update checkbox vs radio based on multiMetric
                    updateMetricInputTypes(modal, allowMultiMetric);
                }

                // If single node and we have many, trim to first
                if (!allowMultiNode && selectedNodes.length > 1) {
                    selectedNodes = [selectedNodes[0]];
                    renderSelectedNodes();
                }
            });
        });

        // Trigger initial state if editing
        if (selectedType) {
            const typeInfo = WIDGET_TYPES[selectedType];
            allowMultiNode = typeInfo?.multiNode || false;
            allowMultiMetric = typeInfo?.multiMetric || false;

            if (typeInfo && !typeInfo.autoMetrics) {
                metricGroup.style.display = 'block';
                updateMetricInputTypes(modal, allowMultiMetric);
            }
        }

        // Node search
        nodeSearch.addEventListener('input', () => {
            if (nodeSearchTimeout) clearTimeout(nodeSearchTimeout);
            const q = nodeSearch.value.trim();
            if (!q) {
                nodeResults.classList.remove('show');
                return;
            }
            nodeSearchTimeout = setTimeout(() => searchNodes(q, nodeResults, selectedNodes, addNode), 250);
        });

        nodeSearch.addEventListener('focus', () => {
            const q = nodeSearch.value.trim();
            if (q) {
                searchNodes(q, nodeResults, selectedNodes, addNode);
            }
        });

        // Close search results on outside click
        document.addEventListener('click', function handler(e) {
            if (!nodeSearch.contains(e.target) && !nodeResults.contains(e.target)) {
                nodeResults.classList.remove('show');
            }
            if (!modal.isConnected) document.removeEventListener('click', handler);
        });

        function addNode(node) {
            if (selectedNodes.find(n => n.id === node.id)) return;
            if (!allowMultiNode) selectedNodes = [];
            selectedNodes.push(node);
            renderSelectedNodes();
            nodeSearch.value = '';
            nodeResults.classList.remove('show');
        }

        function removeNode(nodeId) {
            selectedNodes = selectedNodes.filter(n => n.id !== nodeId);
            renderSelectedNodes();
        }

        function renderSelectedNodes() {
            nodesTagContainer.innerHTML = selectedNodes.map(n => `
                <span class="selected-node-tag" data-node-id="${escapeHtml(n.id)}">
                    ${escapeHtml(n.name)}
                    <span class="tag-remove" data-node-id="${escapeHtml(n.id)}">&times;</span>
                </span>
            `).join('');

            nodesTagContainer.querySelectorAll('.tag-remove').forEach(el => {
                el.addEventListener('click', () => removeNode(el.dataset.nodeId));
            });
        }

        // Metric selection
        modal.querySelectorAll('.metric-option').forEach(opt => {
            opt.addEventListener('click', (e) => {
                if (e.target.tagName === 'INPUT') return; // let checkbox handle itself
                const checkbox = opt.querySelector('input');
                const metricKey = opt.dataset.metric;

                if (allowMultiMetric) {
                    const isChecked = !checkbox.checked;
                    checkbox.checked = isChecked;
                    opt.classList.toggle('selected', isChecked);

                    if (isChecked) {
                        if (!selectedMetrics.includes(metricKey)) selectedMetrics.push(metricKey);
                    } else {
                        selectedMetrics = selectedMetrics.filter(m => m !== metricKey);
                    }
                } else {
                    // Single select mode
                    modal.querySelectorAll('.metric-option').forEach(o => {
                        o.classList.remove('selected');
                        o.querySelector('input').checked = false;
                    });
                    checkbox.checked = true;
                    opt.classList.add('selected');
                    selectedMetrics = [metricKey];
                }
            });

            // Also handle direct checkbox clicks
            const checkbox = opt.querySelector('input');
            checkbox.addEventListener('change', () => {
                const metricKey = opt.dataset.metric;
                opt.classList.toggle('selected', checkbox.checked);

                if (!allowMultiMetric && checkbox.checked) {
                    // Unselect others
                    modal.querySelectorAll('.metric-option').forEach(o => {
                        if (o !== opt) {
                            o.classList.remove('selected');
                            o.querySelector('input').checked = false;
                        }
                    });
                    selectedMetrics = [metricKey];
                } else if (checkbox.checked) {
                    if (!selectedMetrics.includes(metricKey)) selectedMetrics.push(metricKey);
                } else {
                    selectedMetrics = selectedMetrics.filter(m => m !== metricKey);
                }
            });
        });

        // Form submit
        form.addEventListener('submit', (e) => {
            e.preventDefault();

            if (!selectedType) {
                alert('Please select a widget type.');
                return;
            }
            if (selectedNodes.length === 0) {
                alert('Please select at least one node.');
                return;
            }

            const typeInfo = WIDGET_TYPES[selectedType];
            if (!typeInfo.autoMetrics && selectedMetrics.length === 0) {
                alert('Please select at least one metric.');
                return;
            }

            const title = modal.querySelector('#widget-title-input').value.trim();

            const widget = {
                id: existingWidget?.id || 'w_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
                type: selectedType,
                title: title || generateWidgetTitle(selectedType, selectedNodes, selectedMetrics),
                nodes: selectedNodes.map(n => n.id),
                nodeNames: selectedNodes.map(n => n.name),
                metrics: typeInfo.autoMetrics ? [] : selectedMetrics,
                createdAt: existingWidget?.createdAt || Date.now(),
                updatedAt: Date.now(),
            };

            const db = getActiveDashboard();
            if (editIndex !== undefined && editIndex !== null) {
                db.widgets[editIndex] = widget;
            } else {
                db.widgets.push(widget);
            }
            db.updatedAt = Date.now();
            saveDashboards();

            bootstrap.Modal.getInstance(modal).hide();
            renderWidgets();
            refreshAllWidgets();
        });
    }

    function updateMetricInputTypes(modal, allowMulti) {
        modal.querySelectorAll('.metric-option input').forEach(inp => {
            inp.type = allowMulti ? 'checkbox' : 'radio';
            inp.name = allowMulti ? '' : 'metric-select';
        });
    }

    function searchNodes(query, resultsContainer, selectedNodes, onSelect) {
        fetch(`/api/custom-dashboard/nodes/search?q=${encodeURIComponent(query)}&limit=15`)
            .then(r => r.json())
            .then(data => {
                if (!data.nodes || data.nodes.length === 0) {
                    resultsContainer.innerHTML = '<div class="p-2 text-muted text-center">No nodes found</div>';
                    resultsContainer.classList.add('show');
                    return;
                }

                const selectedIds = new Set(selectedNodes.map(n => n.id));

                resultsContainer.innerHTML = data.nodes
                    .filter(n => !selectedIds.has(n.hex_id))
                    .map(node => {
                        const name = node.long_name || node.short_name || node.hex_id;
                        return `
                            <div class="search-result-item" data-node-id="${escapeHtml(node.hex_id)}" data-node-name="${escapeHtml(name)}">
                                <div>
                                    <div class="node-name">${escapeHtml(name)}</div>
                                    <div class="node-id">${escapeHtml(node.hex_id)}${node.hw_model ? ' · ' + escapeHtml(node.hw_model) : ''}</div>
                                </div>
                            </div>
                        `;
                    }).join('') || '<div class="p-2 text-muted text-center">All matching nodes already selected</div>';

                resultsContainer.classList.add('show');

                resultsContainer.querySelectorAll('.search-result-item').forEach(item => {
                    item.addEventListener('click', () => {
                        onSelect({ id: item.dataset.nodeId, name: item.dataset.nodeName });
                    });
                });
            })
            .catch(err => {
                console.error('Node search failed:', err);
                resultsContainer.innerHTML = '<div class="p-2 text-danger text-center">Search failed</div>';
                resultsContainer.classList.add('show');
            });
    }

    // ── Widget CRUD ─────────────────────────────────────────────
    function deleteWidget(index) {
        const db = getActiveDashboard();
        const widget = db.widgets[index];
        if (!widget) return;
        if (!confirm(`Remove widget "${widget.title}"?`)) return;

        db.widgets.splice(index, 1);
        db.updatedAt = Date.now();
        saveDashboards();
        renderWidgets();
        refreshAllWidgets();
    }

    // ── Drag and Drop ───────────────────────────────────────────
    function setupDragAndDrop(container) {
        let draggedIndex = null;

        container.querySelectorAll('.widget-card').forEach(card => {
            card.addEventListener('dragstart', (e) => {
                draggedIndex = parseInt(card.dataset.widgetIndex);
                card.classList.add('dragging');
                e.dataTransfer.effectAllowed = 'move';
                e.dataTransfer.setData('text/plain', draggedIndex);
            });

            card.addEventListener('dragend', () => {
                card.classList.remove('dragging');
                container.querySelectorAll('.widget-card').forEach(c => c.classList.remove('drag-over'));
                draggedIndex = null;
            });

            card.addEventListener('dragover', (e) => {
                e.preventDefault();
                e.dataTransfer.dropEffect = 'move';
                card.classList.add('drag-over');
            });

            card.addEventListener('dragleave', () => {
                card.classList.remove('drag-over');
            });

            card.addEventListener('drop', (e) => {
                e.preventDefault();
                card.classList.remove('drag-over');

                const targetIndex = parseInt(card.dataset.widgetIndex);
                if (draggedIndex === null || draggedIndex === targetIndex) return;

                const db = getActiveDashboard();
                const [moved] = db.widgets.splice(draggedIndex, 1);
                db.widgets.splice(targetIndex, 0, moved);
                db.updatedAt = Date.now();
                saveDashboards();
                renderWidgets();
                refreshAllWidgets();
            });
        });
    }

    // ── Helpers ──────────────────────────────────────────────────
    function findMetricDef(metricKey) {
        for (const cat of Object.values(METRIC_CATEGORIES)) {
            if (cat.metrics[metricKey]) return cat.metrics[metricKey];
        }
        return null;
    }

    function extractMetricValue(telemetry, metricKey) {
        // Search in device_metrics, environment_metrics, power_metrics
        const sections = ['device_metrics', 'environment_metrics', 'power_metrics', 'air_quality_metrics'];
        for (const section of sections) {
            if (telemetry[section] && telemetry[section][metricKey] !== undefined) {
                return telemetry[section][metricKey];
            }
        }
        // Direct top-level
        if (telemetry[metricKey] !== undefined) return telemetry[metricKey];
        return null;
    }

    function formatMetricValue(value, metricDef) {
        if (value === null || value === undefined) return '—';

        if (metricDef?.format === 'duration') {
            return formatDuration(value);
        }

        const num = Number(value);
        if (isNaN(num)) return String(value);

        // Smart formatting
        if (Number.isInteger(num) || num > 100) return String(Math.round(num));
        if (num < 10) return num.toFixed(2);
        return num.toFixed(1);
    }

    function getStatusClass(value, metricDef) {
        if (!metricDef?.thresholds) return '';

        const num = Number(value);
        if (isNaN(num)) return '';

        const dir = metricDef.thresholdDir || 'above'; // 'above' means high is bad, 'below' means low is bad

        if (dir === 'above') {
            if (num >= metricDef.thresholds.danger) return 'status-danger';
            if (num >= metricDef.thresholds.warning) return 'status-warning';
            return 'status-good';
        } else {
            if (num <= metricDef.thresholds.danger) return 'status-danger';
            if (num <= metricDef.thresholds.warning) return 'status-warning';
            return 'status-good';
        }
    }

    function formatDuration(seconds) {
        if (!seconds || seconds < 0) return '—';
        const days = Math.floor(seconds / 86400);
        const hours = Math.floor((seconds % 86400) / 3600);
        const mins = Math.floor((seconds % 3600) / 60);

        if (days > 0) return `${days}d ${hours}h`;
        if (hours > 0) return `${hours}h ${mins}m`;
        return `${mins}m`;
    }

    function formatTimeAgo(timestamp) {
        if (!timestamp) return '';
        const now = Date.now() / 1000;
        const diff = now - timestamp;

        if (diff < 60) return 'just now';
        if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
        if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
        return `${Math.floor(diff / 86400)}d ago`;
    }

    function generateWidgetTitle(type, nodes, metrics) {
        const typeLabel = WIDGET_TYPES[type]?.label || 'Widget';
        const nodeName = nodes[0]?.name || 'Node';

        if (type === 'node_status') return `${nodeName} Status`;
        if (type === 'multi_node_compare') {
            const metricLabel = findMetricDef(metrics[0])?.label || metrics[0];
            return `${metricLabel} Comparison`;
        }
        if (metrics.length === 1) {
            const metricLabel = findMetricDef(metrics[0])?.label || metrics[0];
            return `${nodeName} — ${metricLabel}`;
        }
        return `${nodeName} — ${typeLabel}`;
    }

    function escapeHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function capitalize(str) {
        return str.charAt(0).toUpperCase() + str.slice(1);
    }

    function createModal(id, title, bodyContent) {
        const div = document.createElement('div');
        div.className = 'modal fade';
        div.id = id;
        div.tabIndex = -1;
        div.innerHTML = `
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title"><i class="bi bi-grid-1x2"></i> ${title}</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        ${bodyContent}
                    </div>
                </div>
            </div>
        `;
        return div;
    }

    // ── Boot ────────────────────────────────────────────────────
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();
