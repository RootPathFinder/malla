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
        single_metric: { label: 'Single Metric', icon: 'bi-speedometer', desc: 'Large display of one metric for one node', multiNode: false, multiMetric: false, hasDisplayMode: true },
        multi_metric: { label: 'Multi Metric', icon: 'bi-grid-3x2', desc: 'Multiple metrics — data points or graph', multiNode: false, multiMetric: true, hasDisplayMode: true },
        multi_metric_chart: { label: 'Metric Chart', icon: 'bi-graph-up', desc: 'Time-series graph of metrics', multiNode: false, multiMetric: true, hasDisplayMode: true, isChart: true, hidden: true },
        node_status: { label: 'Node Status', icon: 'bi-card-checklist', desc: 'Overview status card for a node', multiNode: false, multiMetric: false, autoMetrics: true, hasDisplayMode: true },
        multi_node_compare: { label: 'Multi-Node Compare', icon: 'bi-people', desc: 'Compare one metric across nodes', multiNode: true, multiMetric: false, hasDisplayMode: true },
    };

    // ── State ───────────────────────────────────────────────────
    let dashboards = [];
    let activeDashboardId = null;
    let refreshTimer = null;
    let isRefreshing = false;
    let nodeSearchTimeout = null;
    let _saveTimeout = null;

    // ── Grid Layout Constants ───────────────────────────────────
    const GRID_COLS = 12;
    const DEFAULT_LAYOUTS = {
        single_metric:      { w: 3, h: 2 },
        multi_metric:       { w: 4, h: 3 },
        multi_metric_chart: { w: 6, h: 4 },
        node_status:        { w: 4, h: 3 },
        multi_node_compare: { w: 6, h: 3 },
    };
    // Per-widget-type minimum sizes to prevent content from being cut off
    // These are enforced during resize to ensure widgets remain usable
    const MIN_LAYOUTS = {
        single_metric:      { w: 2, h: 2 },
        multi_metric:       { w: 3, h: 3 },
        multi_metric_chart: { w: 5, h: 4 },
        node_status:        { w: 3, h: 3 },
        multi_node_compare: { w: 4, h: 3 },
    };
    const MIN_W = 2;  // Fallback minimum width
    const MIN_H = 2;  // Fallback minimum height
    const MAX_H = 12;

    /** Get minimum width for a widget type */
    function getMinWidth(widgetType) {
        return MIN_LAYOUTS[widgetType]?.w || MIN_W;
    }

    /** Get minimum height for a widget type */
    function getMinHeight(widgetType) {
        return MIN_LAYOUTS[widgetType]?.h || MIN_H;
    }

    /**
     * Ensure every widget in a dashboard has a layout property.
     * New widgets get placed in the first open slot.
     */
    function ensureWidgetLayouts(widgets) {
        // Build occupancy grid from widgets that already have layout
        const occupied = new Set();
        widgets.forEach(w => {
            if (w.layout) {
                for (let r = w.layout.row; r < w.layout.row + w.layout.h; r++) {
                    for (let c = w.layout.col; c < w.layout.col + w.layout.w; c++) {
                        occupied.add(`${r},${c}`);
                    }
                }
            }
        });

        widgets.forEach(w => {
            if (w.layout) return;

            const def = DEFAULT_LAYOUTS[w.type] || { w: 4, h: 3 };
            // If display mode is chart, make it wider
            const isChart = w.displayMode === 'chart' || w.type === 'multi_metric_chart';
            const ww = isChart ? Math.max(def.w, 6) : def.w;
            const hh = isChart ? Math.max(def.h, 4) : def.h;

            // Find first open position scanning row by row
            let placed = false;
            for (let row = 1; row < 200 && !placed; row++) {
                for (let col = 1; col <= GRID_COLS - ww + 1 && !placed; col++) {
                    let fits = true;
                    for (let r = row; r < row + hh && fits; r++) {
                        for (let c = col; c < col + ww && fits; c++) {
                            if (occupied.has(`${r},${c}`)) fits = false;
                        }
                    }
                    if (fits) {
                        w.layout = { col: col, row: row, w: ww, h: hh };
                        for (let r = row; r < row + hh; r++) {
                            for (let c = col; c < col + ww; c++) {
                                occupied.add(`${r},${c}`);
                            }
                        }
                        placed = true;
                    }
                }
            }
            if (!placed) {
                w.layout = { col: 1, row: 1, w: ww, h: hh };
            }
        });
    }

    // Detect whether the user is authenticated (set via template data attr)
    function isAuthenticated() {
        const container = document.querySelector('[data-authenticated]');
        return container && container.dataset.authenticated === 'true';
    }

    // ── Persistence ─────────────────────────────────────────────
    function loadDashboardsFromLocalStorage() {
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
    }

    function loadDashboards() {
        // Always start by loading from localStorage as fast fallback
        loadDashboardsFromLocalStorage();

        // Ensure at least one dashboard exists
        if (dashboards.length === 0) {
            dashboards.push(createDashboard('My Dashboard'));
        }

        // Set active dashboard
        if (!activeDashboardId || !dashboards.find(d => d.id === activeDashboardId)) {
            activeDashboardId = dashboards[0].id;
        }
    }

    async function loadDashboardsFromServer() {
        if (!isAuthenticated()) return false;

        try {
            const resp = await fetch('/api/custom-dashboard/config');
            if (resp.status === 204) {
                // No server config yet – push local dashboards to server
                saveDashboardsToServer();
                return false;
            }
            if (!resp.ok) return false;

            const data = await resp.json();
            if (data.dashboards && Array.isArray(data.dashboards) && data.dashboards.length > 0) {
                dashboards = data.dashboards;
                activeDashboardId = data.active_dashboard_id || dashboards[0].id;

                // Mirror to localStorage as offline cache
                saveToLocalStorage();

                return true;
            }
        } catch (e) {
            console.warn('CustomDashboard: Failed to load from server, using localStorage:', e);
        }
        return false;
    }

    function saveToLocalStorage() {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(dashboards));
        } catch (e) {
            console.warn('CustomDashboard: Failed to save to localStorage:', e);
        }
    }

    function saveDashboardsToServer() {
        if (!isAuthenticated()) return;

        // Debounce server saves to avoid excessive requests
        if (_saveTimeout) clearTimeout(_saveTimeout);
        _saveTimeout = setTimeout(() => {
            fetch('/api/custom-dashboard/config', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    dashboards: dashboards,
                    active_dashboard_id: activeDashboardId,
                }),
            }).catch(e => {
                console.warn('CustomDashboard: Failed to save to server:', e);
            });
        }, 500);
    }

    function saveDashboards() {
        saveToLocalStorage();
        saveDashboardsToServer();
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

        // Asynchronously load from server; re-render if data differs
        loadDashboardsFromServer().then(loaded => {
            if (loaded) {
                renderToolbar();
                renderWidgets();
                refreshAllWidgets();
            }
        });
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

        // Ensure every widget has grid layout data
        ensureWidgetLayouts(db.widgets);

        container.innerHTML = db.widgets.map((widget, index) => renderWidgetCard(widget, index)).join('');

        // Setup drag-to-move and resize interactions
        setupGridInteractions(container);
    }

    function renderWidgetCard(widget, index) {
        const typeInfo = WIDGET_TYPES[widget.type] || {};
        const nodeLabel = widget.nodes?.length > 1
            ? `${widget.nodes.length} nodes`
            : (widget.nodeNames?.[0] || widget.nodes?.[0] || 'No node');

        const layout = widget.layout || { col: 1, row: 1, w: 4, h: 3 };
        const gridStyle = `grid-column: ${layout.col} / span ${layout.w}; grid-row: ${layout.row} / span ${layout.h};`;

        return `
            <div class="widget-card" data-widget-index="${index}" style="${gridStyle}">
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
                <div class="widget-resize-handle" data-resize-index="${index}"></div>
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

        // Chart mode: any widget with displayMode 'chart' or the legacy multi_metric_chart type
        if (widget.displayMode === 'chart' || widget.type === 'multi_metric_chart') {
            if (widget.type === 'multi_node_compare') {
                renderMultiNodeChart(body, widget, telemetryData);
            } else {
                renderMultiMetricChart(body, widget, telemetryData);
            }
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

    function getChartMetrics(widget) {
        // For node_status, chart all standard device metrics
        if (widget.type === 'node_status') {
            return ['battery_level', 'voltage', 'channel_utilization', 'air_util_tx'];
        }
        return widget.metrics || [];
    }

    function renderMultiMetricChart(body, widget, telemetryData) {
        const nodeId = widget.nodes[0];
        const nodeData = telemetryData[nodeId];
        if (!nodeData?.telemetry) {
            body.innerHTML = '<div class="widget-no-data"><i class="bi bi-inbox"></i><span>No telemetry data</span></div>';
            return;
        }

        // Show current values summary + chart container
        const hours = widget.chartHours || 24;
        const chartId = 'chart-' + widget.id;
        const chartMetrics = getChartMetrics(widget);

        // Build a compact current-values row above the chart
        const summaryHtml = chartMetrics.map(metricKey => {
            const metricDef = findMetricDef(metricKey);
            const value = extractMetricValue(nodeData.telemetry, metricKey);
            const formatted = value !== null && value !== undefined ? formatMetricValue(value, metricDef) : '—';
            const statusClass = value !== null ? getStatusClass(value, metricDef) : 'status-unknown';
            return `<span class="chart-summary-pill"><span class="${statusClass}">${formatted}</span> <small>${metricDef?.unit || ''} ${metricDef?.label || metricKey}</small></span>`;
        }).join('');

        body.innerHTML = `
            <div class="widget-chart-wrapper">
                <div class="chart-summary-row">${summaryHtml}</div>
                <div class="chart-controls">
                    <select class="form-select form-select-sm chart-hours-select" style="width: auto; font-size: 0.75rem;">
                        <option value="6" ${hours === 6 ? 'selected' : ''}>6h</option>
                        <option value="12" ${hours === 12 ? 'selected' : ''}>12h</option>
                        <option value="24" ${hours === 24 ? 'selected' : ''}>24h</option>
                        <option value="48" ${hours === 48 ? 'selected' : ''}>48h</option>
                        <option value="168" ${hours === 168 ? 'selected' : ''}>7d</option>
                    </select>
                </div>
                <div id="${chartId}" class="widget-chart-container"></div>
            </div>
        `;

        // Wire up hours selector
        body.querySelector('.chart-hours-select')?.addEventListener('change', (e) => {
            const db = getActiveDashboard();
            const widgetIdx = db.widgets.findIndex(w => w.id === widget.id);
            if (widgetIdx >= 0) {
                db.widgets[widgetIdx].chartHours = parseInt(e.target.value);
                saveDashboards();
                fetchAndRenderChart(nodeId, chartMetrics, chartId, parseInt(e.target.value));
            }
        });

        // Fetch history and render chart
        fetchAndRenderChart(nodeId, chartMetrics, chartId, hours);
    }

    function fetchAndRenderChart(nodeId, metrics, chartId, hours) {
        const container = document.getElementById(chartId);
        if (!container) return;

        container.innerHTML = '<div class="widget-loading"><div class="spinner-border spinner-border-sm text-secondary" role="status"></div></div>';

        fetch(`/api/custom-dashboard/node/${encodeURIComponent(nodeId)}/telemetry/history?hours=${hours}`)
            .then(r => r.json())
            .then(data => {
                if (!data.history || (typeof data.history === 'object' && Object.keys(data.history).length === 0)) {
                    container.innerHTML = '<div class="widget-no-data" style="min-height:60px;"><i class="bi bi-inbox"></i><span>No history data</span></div>';
                    return;
                }

                renderPlotlyChart(container, data.history, metrics);
            })
            .catch(err => {
                console.error('Chart data fetch failed:', err);
                container.innerHTML = '<div class="widget-error">Failed to load chart data</div>';
            });
    }

    function fetchAndRenderMultiNodeChart(nodeIds, nodeNames, metricKey, chartId, hours) {
        const container = document.getElementById(chartId);
        if (!container) return;

        container.innerHTML = '<div class="widget-loading"><div class="spinner-border spinner-border-sm text-secondary" role="status"></div></div>';

        // Fetch history for all nodes in parallel
        const fetches = nodeIds.map(nodeId =>
            fetch(`/api/custom-dashboard/node/${encodeURIComponent(nodeId)}/telemetry/history?hours=${hours}`)
                .then(r => r.json())
                .catch(() => ({ history: {} }))
        );

        Promise.all(fetches)
            .then(results => {
                renderPlotlyMultiNodeChart(container, results.map(r => r.history || {}), nodeIds, nodeNames, metricKey);
            })
            .catch(err => {
                console.error('Multi-node chart data fetch failed:', err);
                container.innerHTML = '<div class="widget-error">Failed to load chart data</div>';
            });
    }

    function renderPlotlyChart(container, history, metrics) {
        // Plotly availability check
        if (typeof Plotly === 'undefined') {
            container.innerHTML = '<div class="widget-error">Chart library not loaded</div>';
            return;
        }

        const CHART_COLORS = ['#0d6efd', '#198754', '#dc3545', '#ffc107', '#6f42c1', '#fd7e14', '#20c997', '#0dcaf0'];

        // History is a dict of metric_name -> [{x, y}, ...] from the backend
        const traces = metrics.map((metricKey, i) => {
            const metricDef = findMetricDef(metricKey);
            const metricData = history[metricKey] || [];

            return {
                x: metricData.map(pt => new Date(pt.x)),
                y: metricData.map(pt => Number(pt.y)),
                type: 'scatter',
                mode: 'lines',
                name: (metricDef?.label || metricKey) + (metricDef?.unit ? ` (${metricDef.unit})` : ''),
                line: { color: CHART_COLORS[i % CHART_COLORS.length], width: 2 },
                hovertemplate: '%{y:.2f}<extra>%{fullData.name}</extra>',
            };
        }).filter(t => t.x.length > 0);

        if (traces.length === 0) {
            container.innerHTML = '<div class="widget-no-data" style="min-height:60px;"><i class="bi bi-inbox"></i><span>No data points for selected metrics</span></div>';
            return;
        }

        renderPlotlyTraces(container, traces);
    }

    function renderPlotlyMultiNodeChart(container, historyArrays, nodeIds, nodeNames, metricKey) {
        // Plotly availability check
        if (typeof Plotly === 'undefined') {
            container.innerHTML = '<div class="widget-error">Chart library not loaded</div>';
            return;
        }

        const CHART_COLORS = ['#0d6efd', '#198754', '#dc3545', '#ffc107', '#6f42c1', '#fd7e14', '#20c997', '#0dcaf0'];
        const metricDef = findMetricDef(metricKey);

        const traces = nodeIds.map((nodeId, i) => {
            const history = historyArrays[i] || {};
            const metricData = history[metricKey] || [];
            const nodeName = nodeNames?.[i] || nodeId;

            return {
                x: metricData.map(pt => new Date(pt.x)),
                y: metricData.map(pt => Number(pt.y)),
                type: 'scatter',
                mode: 'lines',
                name: nodeName,
                line: { color: CHART_COLORS[i % CHART_COLORS.length], width: 2 },
                hovertemplate: `%{y:.2f} ${metricDef?.unit || ''}<extra>${escapeHtml(nodeName)}</extra>`,
            };
        }).filter(t => t.x.length > 0);

        if (traces.length === 0) {
            container.innerHTML = '<div class="widget-no-data" style="min-height:60px;"><i class="bi bi-inbox"></i><span>No history data for selected nodes</span></div>';
            return;
        }

        renderPlotlyTraces(container, traces);
    }

    function renderPlotlyTraces(container, traces) {
        const isDark = document.documentElement.getAttribute('data-bs-theme') === 'dark';

        const layout = {
            margin: { t: 8, r: 10, b: 32, l: 40 },
            autosize: true,
            showlegend: traces.length > 1,
            legend: { x: 0, y: 1.15, orientation: 'h', font: { size: 10, color: isDark ? '#adb5bd' : '#6c757d' } },
            xaxis: {
                type: 'date',
                tickfont: { size: 9, color: isDark ? '#adb5bd' : '#6c757d' },
                gridcolor: isDark ? '#495057' : '#e9ecef',
                linecolor: isDark ? '#495057' : '#dee2e6',
            },
            yaxis: {
                tickfont: { size: 9, color: isDark ? '#adb5bd' : '#6c757d' },
                gridcolor: isDark ? '#495057' : '#e9ecef',
                linecolor: isDark ? '#495057' : '#dee2e6',
            },
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            hovermode: 'x unified',
        };

        const config = {
            responsive: true,
            displayModeBar: false,
            staticPlot: false,
        };

        container.innerHTML = '';
        Plotly.newPlot(container, traces, layout, config);
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

    function renderMultiNodeChart(body, widget, telemetryData) {
        const metricKey = widget.metrics[0];
        const metricDef = findMetricDef(metricKey);
        const hours = widget.chartHours || 24;
        const chartId = 'chart-' + widget.id;

        // Build summary row with current values per node
        const summaryHtml = widget.nodes.map(nodeId => {
            const nodeData = telemetryData[nodeId];
            const nodeName = widget.nodeNames?.[widget.nodes.indexOf(nodeId)] || nodeId;
            const value = nodeData?.telemetry ? extractMetricValue(nodeData.telemetry, metricKey) : null;
            const formatted = value !== null && value !== undefined ? formatMetricValue(value, metricDef) : '—';
            const statusClass = value !== null && value !== undefined ? getStatusClass(value, metricDef) : 'status-unknown';
            return `<span class="chart-summary-pill"><span class="${statusClass}">${formatted}</span> <small>${metricDef?.unit || ''} ${escapeHtml(nodeName)}</small></span>`;
        }).join('');

        body.innerHTML = `
            <div class="widget-chart-wrapper">
                <div class="chart-summary-row">${summaryHtml}</div>
                <div class="chart-controls">
                    <select class="form-select form-select-sm chart-hours-select" style="width: auto; font-size: 0.75rem;">
                        <option value="6" ${hours === 6 ? 'selected' : ''}>6h</option>
                        <option value="12" ${hours === 12 ? 'selected' : ''}>12h</option>
                        <option value="24" ${hours === 24 ? 'selected' : ''}>24h</option>
                        <option value="48" ${hours === 48 ? 'selected' : ''}>48h</option>
                        <option value="168" ${hours === 168 ? 'selected' : ''}>7d</option>
                    </select>
                </div>
                <div id="${chartId}" class="widget-chart-container"></div>
            </div>
        `;

        // Wire up hours selector
        body.querySelector('.chart-hours-select')?.addEventListener('change', (e) => {
            const db = getActiveDashboard();
            const widgetIdx = db.widgets.findIndex(w => w.id === widget.id);
            if (widgetIdx >= 0) {
                db.widgets[widgetIdx].chartHours = parseInt(e.target.value);
                saveDashboards();
                fetchAndRenderMultiNodeChart(widget.nodes, widget.nodeNames, metricKey, chartId, parseInt(e.target.value));
            }
        });

        // Fetch history for all nodes and render
        fetchAndRenderMultiNodeChart(widget.nodes, widget.nodeNames, metricKey, chartId, hours);
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
                        ${Object.entries(WIDGET_TYPES).filter(([, type]) => !type.hidden).map(([key, type]) => `
                            <div class="widget-type-option ${(existingWidget?.type === key || (key === 'multi_metric' && existingWidget?.type === 'multi_metric_chart')) ? 'selected' : ''}" data-type="${key}">
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

                <!-- Step 4: Display Mode (for multi-metric types) -->
                <div class="mb-3" id="display-mode-group" style="display: none;">
                    <label class="form-label fw-semibold">Display Mode</label>
                    <div class="d-flex gap-2">
                        <div class="widget-type-option display-mode-option flex-fill ${(!existingWidget || existingWidget?.type === 'multi_metric') ? 'selected' : ''}" data-display-mode="data_points" style="padding: 0.75rem;">
                            <i class="bi bi-grid-3x2"></i>
                            <div class="type-name">Data Points</div>
                            <div class="type-desc">Current values in a grid</div>
                        </div>
                        <div class="widget-type-option display-mode-option flex-fill ${existingWidget?.type === 'multi_metric_chart' ? 'selected' : ''}" data-display-mode="chart" style="padding: 0.75rem;">
                            <i class="bi bi-graph-up"></i>
                            <div class="type-name">Graph</div>
                            <div class="type-desc">Time-series chart of history</div>
                        </div>
                    </div>
                </div>

                <!-- Step 5: Metric Selection -->
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
        const typeOptions = modal.querySelectorAll('.widget-type-option:not(.display-mode-option)');
        const metricGroup = modal.querySelector('#metric-selection-group');
        const displayModeGroup = modal.querySelector('#display-mode-group');
        const nodeSearch = modal.querySelector('#widget-node-search');
        const nodeResults = modal.querySelector('#widget-node-results');
        const nodesTagContainer = modal.querySelector('#selected-nodes-tags');

        // Track selections
        let selectedNodes = existingWidget?.nodes?.map((nid, i) => ({
            id: nid,
            name: existingWidget.nodeNames?.[i] || nid
        })) || [];
        let selectedMetrics = existingWidget?.metrics?.slice() || [];
        // Normalize legacy multi_metric_chart type back to multi_metric
        let selectedType = existingWidget?.type === 'multi_metric_chart' ? 'multi_metric' : (existingWidget?.type || '');
        let selectedDisplayMode = (existingWidget?.displayMode === 'chart' || existingWidget?.type === 'multi_metric_chart') ? 'chart' : 'data_points';
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

                // Show/hide display mode toggle for multi-metric types
                if (typeInfo?.hasDisplayMode) {
                    displayModeGroup.style.display = 'block';
                } else {
                    displayModeGroup.style.display = 'none';
                    selectedDisplayMode = 'data_points';
                }

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

        // Display mode toggle
        modal.querySelectorAll('.display-mode-option').forEach(opt => {
            opt.addEventListener('click', () => {
                modal.querySelectorAll('.display-mode-option').forEach(o => o.classList.remove('selected'));
                opt.classList.add('selected');
                selectedDisplayMode = opt.dataset.displayMode;
            });
        });

        // Trigger initial state if editing
        if (selectedType) {
            const typeInfo = WIDGET_TYPES[selectedType];
            allowMultiNode = typeInfo?.multiNode || false;
            allowMultiMetric = typeInfo?.multiMetric || false;

            if (typeInfo?.hasDisplayMode) {
                displayModeGroup.style.display = 'block';
            }

            if (typeInfo && !typeInfo.autoMetrics) {
                metricGroup.style.display = 'block';
                updateMetricInputTypes(modal, allowMultiMetric);
            }

            // Attach event listeners to existing node tags (if editing)
            if (selectedNodes.length > 0) {
                renderSelectedNodes();
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
                displayMode: selectedDisplayMode === 'chart' ? 'chart' : undefined,
                title: title || generateWidgetTitle(selectedType, selectedNodes, selectedMetrics, selectedDisplayMode),
                nodes: selectedNodes.map(n => n.id),
                nodeNames: selectedNodes.map(n => n.name),
                metrics: typeInfo.autoMetrics ? [] : selectedMetrics,
                chartHours: existingWidget?.chartHours,
                layout: existingWidget?.layout || undefined, // will be auto-assigned by ensureWidgetLayouts
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

    // ── Grid Interactions (Drag-to-move & Resize) ─────────────
    function setupGridInteractions(container) {
        const db = getActiveDashboard();

        // ── Drag to move (via header) ──────────────
        container.querySelectorAll('.widget-card-header').forEach(header => {
            header.addEventListener('mousedown', onDragStart);
            header.addEventListener('touchstart', onDragStartTouch, { passive: false });
        });

        // ── Resize (via corner handle) ─────────────
        container.querySelectorAll('.widget-resize-handle').forEach(handle => {
            handle.addEventListener('mousedown', onResizeStart);
            handle.addEventListener('touchstart', onResizeStartTouch, { passive: false });
        });

        /* ─── helpers ─── */

        /** Convert a page-coordinate to a grid cell {col, row} */
        function pageToGrid(pageX, pageY) {
            const rect = container.getBoundingClientRect();
            const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
            const scrollLeft = window.pageXOffset || document.documentElement.scrollLeft;
            const x = pageX - rect.left - scrollLeft;
            const y = pageY - rect.top - scrollTop;
            const style = getComputedStyle(container);
            const gap = parseFloat(style.gap) || parseFloat(style.gridGap) || 12;
            const colWidth = (rect.width - gap * (GRID_COLS - 1)) / GRID_COLS;
            const rowHeight = parseFloat(style.gridAutoRows) || 80;
            const col = Math.max(1, Math.min(GRID_COLS, Math.floor(x / (colWidth + gap)) + 1));
            const row = Math.max(1, Math.floor(y / (rowHeight + gap)) + 1);
            return { col, row, colWidth, rowHeight, gap };
        }

        /** Remove placeholder element if it exists */
        function removePlaceholder() {
            container.querySelector('.grid-drop-placeholder')?.remove();
        }

        /** Show / update placeholder */
        function showPlaceholder(col, row, w, h) {
            let ph = container.querySelector('.grid-drop-placeholder');
            if (!ph) {
                ph = document.createElement('div');
                ph.className = 'grid-drop-placeholder';
                container.appendChild(ph);
            }
            ph.style.gridColumn = `${col} / span ${w}`;
            ph.style.gridRow = `${row} / span ${h}`;
        }

        /* ─── DRAG ─── */
        function onDragStartTouch(e) {
            if (e.touches.length !== 1) return;
            const touch = e.touches[0];
            const header = e.currentTarget;
            const card = header.closest('.widget-card');
            if (!card) return;
            e.preventDefault();
            beginDrag(card, touch.pageX, touch.pageY, true);
        }

        function onDragStart(e) {
            // Ignore clicks on buttons inside header
            if (e.target.closest('button')) return;
            const header = e.currentTarget;
            const card = header.closest('.widget-card');
            if (!card) return;
            e.preventDefault();
            beginDrag(card, e.pageX, e.pageY, false);
        }

        function beginDrag(card, startX, startY, isTouch) {
            const widgetIndex = parseInt(card.dataset.widgetIndex);
            const widget = db.widgets[widgetIndex];
            if (!widget) return;

            const layout = widget.layout;
            card.classList.add('dragging');
            container.classList.add('drag-active');

            const moveEvent = isTouch ? 'touchmove' : 'mousemove';
            const upEvent = isTouch ? 'touchend' : 'mouseup';

            let lastCol = layout.col;
            let lastRow = layout.row;

            showPlaceholder(layout.col, layout.row, layout.w, layout.h);

            function onMove(ev) {
                const px = isTouch ? ev.touches[0].pageX : ev.pageX;
                const py = isTouch ? ev.touches[0].pageY : ev.pageY;
                const g = pageToGrid(px, py);
                // Centre the widget on the cursor
                let col = g.col - Math.floor(layout.w / 2);
                col = Math.max(1, Math.min(GRID_COLS - layout.w + 1, col));
                let row = Math.max(1, g.row - Math.floor(layout.h / 2));
                if (col !== lastCol || row !== lastRow) {
                    lastCol = col;
                    lastRow = row;
                    showPlaceholder(col, row, layout.w, layout.h);
                }
            }

            function onUp() {
                document.removeEventListener(moveEvent, onMove);
                document.removeEventListener(upEvent, onUp);
                card.classList.remove('dragging');
                container.classList.remove('drag-active');
                removePlaceholder();

                if (lastCol !== layout.col || lastRow !== layout.row) {
                    layout.col = lastCol;
                    layout.row = lastRow;
                    db.updatedAt = Date.now();
                    saveDashboards();
                    renderWidgets();
                    refreshAllWidgets();
                }
            }

            document.addEventListener(moveEvent, onMove);
            document.addEventListener(upEvent, onUp);
        }

        /* ─── RESIZE ─── */
        function onResizeStartTouch(e) {
            if (e.touches.length !== 1) return;
            e.preventDefault();
            const handle = e.currentTarget;
            const card = handle.closest('.widget-card');
            if (!card) return;
            beginResize(card, e.touches[0].pageX, e.touches[0].pageY, true);
        }

        function onResizeStart(e) {
            e.preventDefault();
            e.stopPropagation();
            const handle = e.currentTarget;
            const card = handle.closest('.widget-card');
            if (!card) return;
            beginResize(card, e.pageX, e.pageY, false);
        }

        function beginResize(card, startX, startY, isTouch) {
            const widgetIndex = parseInt(card.dataset.widgetIndex);
            const widget = db.widgets[widgetIndex];
            if (!widget) return;

            const layout = widget.layout;
            const startW = layout.w;
            const startH = layout.h;

            // Get per-widget-type minimum sizes
            const minW = getMinWidth(widget.type);
            const minH = getMinHeight(widget.type);

            const moveEvent = isTouch ? 'touchmove' : 'mousemove';
            const upEvent = isTouch ? 'touchend' : 'mouseup';

            let newW = startW;
            let newH = startH;

            function onMove(ev) {
                const px = isTouch ? ev.touches[0].pageX : ev.pageX;
                const py = isTouch ? ev.touches[0].pageY : ev.pageY;
                const g = pageToGrid(px, py);
                // Width = cursor col - widget start col + 1, clamped to per-type minimum
                newW = Math.max(minW, Math.min(GRID_COLS - layout.col + 1, g.col - layout.col + 1));
                newH = Math.max(minH, Math.min(MAX_H, g.row - layout.row + 1));

                // Live preview via inline style
                card.style.gridColumn = `${layout.col} / span ${newW}`;
                card.style.gridRow = `${layout.row} / span ${newH}`;

                // Visual feedback when at minimum size
                const atMinW = newW <= minW;
                const atMinH = newH <= minH;
                card.classList.toggle('at-min-width', atMinW);
                card.classList.toggle('at-min-height', atMinH);
                card.classList.toggle('at-min-size', atMinW || atMinH);
            }

            function onUp() {
                document.removeEventListener(moveEvent, onMove);
                document.removeEventListener(upEvent, onUp);

                // Remove visual feedback classes
                card.classList.remove('at-min-width', 'at-min-height', 'at-min-size');

                if (newW !== startW || newH !== startH) {
                    layout.w = newW;
                    layout.h = newH;
                    db.updatedAt = Date.now();
                    saveDashboards();
                    renderWidgets();
                    refreshAllWidgets();
                }
            }

            document.addEventListener(moveEvent, onMove);
            document.addEventListener(upEvent, onUp);
        }
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

    function generateWidgetTitle(type, nodes, metrics, displayMode) {
        const typeLabel = WIDGET_TYPES[type]?.label || 'Widget';
        const nodeName = nodes[0]?.name || 'Node';
        const chartSuffix = displayMode === 'chart' ? ' Chart' : '';

        if (type === 'node_status') return `${nodeName} Status${chartSuffix}`;
        if (type === 'multi_metric_chart') {
            return `${nodeName} — Chart`;
        }
        if (type === 'multi_node_compare') {
            const metricLabel = findMetricDef(metrics[0])?.label || metrics[0];
            return `${metricLabel} Comparison${chartSuffix}`;
        }
        if (metrics.length === 1) {
            const metricLabel = findMetricDef(metrics[0])?.label || metrics[0];
            return `${nodeName} — ${metricLabel}${chartSuffix}`;
        }
        return `${nodeName} — ${typeLabel}${chartSuffix}`;
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
