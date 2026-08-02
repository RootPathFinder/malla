/**
 * Browser notifications for detection-sensor alerts.
 * Polls recent DETECTION_SENSOR_APP packets and notifies for subscribed node/sensor pairs.
 *
 * Modes:
 * - system: Notification API (+ service worker) when available
 * - in_app: toast banners while the tab is open (Chrome/Firefox on iOS, etc.)
 *
 * On iOS, system notifications require Safari with the site added to the Home Screen
 * (iOS 16.4+). Chrome/Firefox on iOS cannot show Web Notifications.
 */
(function () {
    'use strict';

    const PREF_ENABLED = 'detection_notifications_enabled';
    const PREF_SUBSCRIPTIONS = 'detection_notification_subscriptions';
    const STORAGE_LAST_ID = 'malla-detection-notify-last-id';
    const POLL_MS = 15000;
    const LOOKBACK_HOURS = 6;
    const TOAST_MS = 8000;

    let pollTimer = null;
    let started = false;
    let swRegistration = null;
    let toastRoot = null;

    function isAuthenticated() {
        return document.getElementById('userDropdown') !== null;
    }

    function isIOS() {
        const ua = navigator.userAgent || '';
        if (/iPad|iPhone|iPod/.test(ua)) return true;
        // iPadOS 13+ can report as MacIntel with touch
        return navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1;
    }

    function isIOSChromeOrFirefox() {
        if (!isIOS()) return false;
        const ua = navigator.userAgent || '';
        // Chrome iOS: CriOS; Firefox iOS: FxiOS; Edge iOS: EdgiOS
        return /CriOS|FxiOS|EdgiOS|OPiOS/.test(ua);
    }

    function isStandalonePwa() {
        if (window.matchMedia && window.matchMedia('(display-mode: standalone)').matches) {
            return true;
        }
        // iOS Safari home-screen
        return !!(navigator.standalone);
    }

    function supportsSystemNotifications() {
        return typeof Notification !== 'undefined';
    }

    /**
     * @returns {{
     *   mode: 'system'|'in_app',
     *   systemAvailable: boolean,
     *   permission: string,
     *   ios: boolean,
     *   iosAltBrowser: boolean,
     *   standalone: boolean,
     *   guidance: string|null
     * }}
     */
    function getCapability() {
        const ios = isIOS();
        const iosAltBrowser = isIOSChromeOrFirefox();
        const standalone = isStandalonePwa();
        const systemAvailable = supportsSystemNotifications();
        const permission = systemAvailable ? Notification.permission : 'unsupported';

        let guidance = null;
        if (ios && iosAltBrowser) {
            guidance =
                'Chrome and other App Store browsers on iPhone/iPad cannot show system notifications. ' +
                'Open this site in Safari, then Share → Add to Home Screen. Alerts work from the home-screen app. ' +
                'Meanwhile you can enable in-app alerts while this tab stays open.';
        } else if (ios && !systemAvailable && !standalone) {
            guidance =
                'On iPhone/iPad, open this site in Safari and use Share → Add to Home Screen, then open Malla from your home screen to allow system notifications. ' +
                'You can still enable in-app alerts while this tab stays open.';
        } else if (ios && systemAvailable && !standalone && permission !== 'granted') {
            guidance =
                'For background alerts on iPhone/iPad, add Malla to your Home Screen (Safari → Share → Add to Home Screen) and open it from there.';
        } else if (!systemAvailable) {
            guidance =
                'This browser cannot show system notifications. You can still enable in-app alerts while this tab stays open.';
        }

        return {
            mode: systemAvailable ? 'system' : 'in_app',
            systemAvailable,
            permission,
            ios,
            iosAltBrowser,
            standalone,
            guidance,
        };
    }

    function parseStored(value, fallback) {
        if (value === null || value === undefined) return fallback;
        if (typeof value === 'object') return value;
        try {
            return JSON.parse(value);
        } catch (e) {
            return fallback;
        }
    }

    async function loadPrefs() {
        if (window.UserPreferences && typeof window.UserPreferences.loadPreferencesFromServer === 'function') {
            try {
                await window.UserPreferences.loadPreferencesFromServer();
            } catch (e) { /* ignore */ }
        }
        let enabled = false;
        let subscriptions = [];
        try {
            const res = await fetch('/api/preferences', { credentials: 'same-origin' });
            if (res.ok) {
                const data = await res.json();
                const prefs = data.preferences || {};
                enabled = !!prefs[PREF_ENABLED];
                subscriptions = parseStored(prefs[PREF_SUBSCRIPTIONS], []);
            }
        } catch (e) {
            enabled = localStorage.getItem('malla-' + PREF_ENABLED) === 'true';
            subscriptions = parseStored(localStorage.getItem('malla-' + PREF_SUBSCRIPTIONS), []);
        }
        if (!Array.isArray(subscriptions)) subscriptions = [];
        return { enabled, subscriptions };
    }

    function normalizeNodeId(value) {
        if (value === null || value === undefined || value === '') return null;
        if (typeof value === 'number') return value;
        const text = String(value).trim();
        if (text.startsWith('!')) {
            return parseInt(text.slice(1), 16);
        }
        if (/^[0-9a-fA-F]{8}$/.test(text)) {
            return parseInt(text, 16);
        }
        const n = Number(text);
        return Number.isFinite(n) ? n : null;
    }

    function normalizeSensorName(value) {
        let text = String(value || '').trim();
        if (!text) return '';
        text = text.replace(/\s+dwell_ms=\d+\s*$/i, '').trim();
        text = text.replace(/\s+detected\s*$/i, '').trim();
        return text || String(value || '').trim();
    }

    function eventMatches(event, subscriptions) {
        if (!subscriptions.length) return false;
        const eventNode = normalizeNodeId(event.from_node_id ?? event.from_node_hex);
        const sensor = normalizeSensorName(event.detection_name || event.detection_text || '');
        return subscriptions.some((sub) => {
            const subNode = normalizeNodeId(sub.node_id ?? sub.node_hex);
            if (subNode === null || eventNode === null || subNode !== eventNode) return false;
            const subSensor = (sub.sensor_name || '*').trim();
            if (!subSensor || subSensor === '*') return true;
            return sensor === normalizeSensorName(subSensor);
        });
    }

    function ensureToastRoot() {
        if (toastRoot && document.body.contains(toastRoot)) return toastRoot;
        toastRoot = document.createElement('div');
        toastRoot.id = 'malla-detection-toast-root';
        toastRoot.setAttribute('aria-live', 'polite');
        toastRoot.setAttribute('aria-relevant', 'additions');
        Object.assign(toastRoot.style, {
            position: 'fixed',
            left: '50%',
            bottom: '1.25rem',
            transform: 'translateX(-50%)',
            zIndex: '1080',
            display: 'flex',
            flexDirection: 'column',
            gap: '0.5rem',
            width: 'min(420px, calc(100vw - 1.5rem))',
            pointerEvents: 'none',
        });
        document.body.appendChild(toastRoot);
        return toastRoot;
    }

    function showInAppToast(title, body, data) {
        const root = ensureToastRoot();
        const toast = document.createElement('div');
        toast.className = 'malla-detection-toast';
        Object.assign(toast.style, {
            pointerEvents: 'auto',
            background: 'var(--bs-body-bg, #fff)',
            color: 'var(--bs-body-color, #212529)',
            border: '1px solid var(--bs-border-color, #dee2e6)',
            borderLeft: '4px solid var(--bs-warning, #ffc107)',
            borderRadius: '0.5rem',
            boxShadow: '0 0.5rem 1.25rem rgba(0,0,0,0.18)',
            padding: '0.75rem 0.9rem',
            cursor: 'pointer',
        });

        const titleEl = document.createElement('div');
        titleEl.style.fontWeight = '600';
        titleEl.textContent = title;
        const bodyEl = document.createElement('div');
        bodyEl.style.fontSize = '0.875rem';
        bodyEl.style.opacity = '0.85';
        bodyEl.textContent = body || '';
        toast.appendChild(titleEl);
        if (body) toast.appendChild(bodyEl);

        const url = data && data.url ? data.url : '/sensor-dashboard';
        toast.addEventListener('click', () => {
            window.location.href = url;
        });

        root.appendChild(toast);
        window.setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transition = 'opacity 0.25s ease';
            window.setTimeout(() => toast.remove(), 280);
        }, TOAST_MS);
    }

    async function ensureServiceWorker() {
        if (!('serviceWorker' in navigator)) return null;
        try {
            swRegistration = await navigator.serviceWorker.register('/sw.js', { scope: '/' });
            await navigator.serviceWorker.ready;
            return swRegistration;
        } catch (e) {
            console.debug('Service worker registration failed:', e);
            return null;
        }
    }

    async function showNotification(title, body, data) {
        const options = {
            body,
            tag: data && data.tag ? data.tag : 'detection-' + (data && data.id ? data.id : Date.now()),
            renotify: true,
            data: data || {},
            icon: '/static/img/notification-icon.png',
            badge: '/static/img/notification-badge.png',
        };

        const cap = getCapability();
        if (!cap.systemAvailable || (cap.systemAvailable && Notification.permission !== 'granted')) {
            showInAppToast(title, body, data);
            return;
        }

        if (swRegistration && swRegistration.active) {
            swRegistration.active.postMessage({
                type: 'SHOW_NOTIFICATION',
                title,
                body: options.body,
                tag: options.tag,
                data: options.data,
                icon: options.icon,
                badge: options.badge,
            });
            return;
        }

        try {
            // eslint-disable-next-line no-new
            new Notification(title, options);
        } catch (e) {
            // Some mobile browsers require SW showNotification only
            if (swRegistration) {
                try {
                    await swRegistration.showNotification(title, options);
                    return;
                } catch (e2) { /* fall through */ }
            }
            showInAppToast(title, body, data);
        }
    }

    async function pollOnce(subscriptions) {
        const res = await fetch(
            `/api/detection-sensors?hours=${LOOKBACK_HOURS}&limit=50`,
            { credentials: 'same-origin' }
        );
        if (!res.ok) return;
        const data = await res.json();
        const events = Array.isArray(data.events) ? data.events : [];
        if (!events.length) return;

        // API returns newest-first
        const newestId = events[0].id;
        const lastIdRaw = sessionStorage.getItem(STORAGE_LAST_ID);
        if (lastIdRaw === null) {
            // First run: baseline without flooding
            sessionStorage.setItem(STORAGE_LAST_ID, String(newestId));
            return;
        }
        const lastId = parseInt(lastIdRaw, 10) || 0;
        const fresh = events
            .filter((e) => Number(e.id) > lastId)
            .sort((a, b) => Number(a.id) - Number(b.id));

        sessionStorage.setItem(STORAGE_LAST_ID, String(Math.max(lastId, newestId)));

        for (const event of fresh) {
            if (!eventMatches(event, subscriptions)) continue;
            const sensor = event.detection_name || 'Sensor';
            const longName = (event.long_name || '').trim();
            const shortName = (event.short_name || '').trim();
            let node = longName || event.node_name || event.from_node_hex || 'Node';
            if (shortName && shortName !== longName && !String(node).endsWith(`(${shortName})`)) {
                // Prefer "LongName SHORT" so duplicate long names stay distinguishable
                if (longName) node = `${longName} ${shortName}`;
                else if (!String(node).includes(shortName)) node = `${node} ${shortName}`;
            }
            let body = `${node} · ${sensor}`;
            const dwellMs = event.dwell_ms;
            if (dwellMs != null && Number(dwellMs) > 0) {
                const ms = Number(dwellMs);
                const dwellLabel = ms < 1000
                    ? `${ms}ms`
                    : (Math.abs(ms / 1000 - Math.round(ms / 1000)) < 0.05
                        ? `${Math.round(ms / 1000)}s`
                        : `${(ms / 1000).toFixed(1)}s`);
                body += ` · dwell ${dwellLabel}`;
            }
            await showNotification(
                `${sensor} detection`,
                body,
                {
                    id: event.id,
                    url: '/sensor-dashboard',
                    tag: 'detection-' + event.id,
                }
            );
        }
    }

    async function start() {
        if (!isAuthenticated()) return;
        const prefs = await loadPrefs();
        if (!prefs.enabled || !prefs.subscriptions.length) {
            stop();
            return;
        }
        const cap = getCapability();
        // System permission required only when we can request it; otherwise in-app mode is fine
        if (cap.systemAvailable && Notification.permission === 'denied') {
            // Still allow in-app while tab is open
        }
        if (cap.systemAvailable && Notification.permission === 'granted') {
            await ensureServiceWorker();
        }
        if (pollTimer) clearInterval(pollTimer);
        const tick = async () => {
            try {
                const latest = await loadPrefs();
                if (!latest.enabled || !latest.subscriptions.length) {
                    stop();
                    return;
                }
                await pollOnce(latest.subscriptions);
            } catch (e) {
                console.debug('Detection notification poll failed:', e);
            }
        };
        await tick();
        pollTimer = setInterval(tick, POLL_MS);
        started = true;
    }

    function stop() {
        if (pollTimer) {
            clearInterval(pollTimer);
            pollTimer = null;
        }
        started = false;
    }

    async function requestPermission() {
        const cap = getCapability();
        if (!cap.systemAvailable) {
            return {
                status: 'unsupported',
                mode: 'in_app',
                guidance: cap.guidance,
            };
        }
        await ensureServiceWorker();
        if (Notification.permission === 'granted') {
            return { status: 'granted', mode: 'system', guidance: cap.guidance };
        }
        if (Notification.permission === 'denied') {
            return { status: 'denied', mode: 'in_app', guidance: cap.guidance };
        }
        const result = await Notification.requestPermission();
        const next = getCapability();
        return {
            status: result,
            mode: result === 'granted' ? 'system' : 'in_app',
            guidance: next.guidance,
        };
    }

    async function sendTestNotification() {
        const cap = getCapability();
        if (cap.systemAvailable && Notification.permission === 'granted') {
            await ensureServiceWorker();
        }
        await showNotification(
            'Test detection alert',
            cap.systemAvailable && Notification.permission === 'granted'
                ? 'Browser notifications are working for Malla detection sensors.'
                : 'In-app alert (tab must stay open). For background alerts on iPhone, use Safari → Add to Home Screen.',
            { url: '/sensor-dashboard', tag: 'detection-test' }
        );
        return getCapability();
    }

    window.DetectionNotifications = {
        start,
        stop,
        requestPermission,
        sendTestNotification,
        ensureServiceWorker,
        getCapability,
        supportsSystemNotifications,
        PREF_ENABLED,
        PREF_SUBSCRIPTIONS,
        isStarted: () => started,
    };

    window.addEventListener('preferenceChanged', (ev) => {
        const key = ev.detail && ev.detail.key;
        if (key === PREF_ENABLED || key === PREF_SUBSCRIPTIONS) {
            start();
        }
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => { start(); });
    } else {
        start();
    }
})();
