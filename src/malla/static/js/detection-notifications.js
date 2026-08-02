/**
 * Browser notifications for detection-sensor alerts.
 * Polls recent DETECTION_SENSOR_APP packets and notifies for subscribed node/sensor pairs.
 * Uses the Notification API + service worker showNotification for mobile/background tabs.
 */
(function () {
    'use strict';

    const PREF_ENABLED = 'detection_notifications_enabled';
    const PREF_SUBSCRIPTIONS = 'detection_notification_subscriptions';
    const STORAGE_LAST_ID = 'malla-detection-notify-last-id';
    const POLL_MS = 15000;
    const LOOKBACK_HOURS = 6;

    let pollTimer = null;
    let started = false;
    let swRegistration = null;

    function isAuthenticated() {
        return document.getElementById('userDropdown') !== null;
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

    function eventMatches(event, subscriptions) {
        if (!subscriptions.length) return false;
        const eventNode = normalizeNodeId(event.from_node_id ?? event.from_node_hex);
        const sensor = (event.detection_name || '').trim();
        return subscriptions.some((sub) => {
            const subNode = normalizeNodeId(sub.node_id ?? sub.node_hex);
            if (subNode === null || eventNode === null || subNode !== eventNode) return false;
            const subSensor = (sub.sensor_name || '*').trim();
            if (!subSensor || subSensor === '*') return true;
            return sensor === subSensor;
        });
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

        if (typeof Notification !== 'undefined' && Notification.permission === 'granted') {
            try {
                // eslint-disable-next-line no-new
                new Notification(title, options);
            } catch (e) {
                // Some mobile browsers require SW showNotification only
                if (swRegistration) {
                    await swRegistration.showNotification(title, options);
                }
            }
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
            const node = event.node_name || event.from_node_hex || 'Node';
            await showNotification(
                `${sensor} detection`,
                `${node} · ${sensor}`,
                {
                    id: event.id,
                    url: '/sensor-dashboard',
                    tag: 'detection-' + event.id,
                }
            );
        }
    }

    async function start() {
        if (!isAuthenticated() || typeof Notification === 'undefined') return;
        const prefs = await loadPrefs();
        if (!prefs.enabled || !prefs.subscriptions.length) {
            stop();
            return;
        }
        if (Notification.permission !== 'granted') {
            stop();
            return;
        }
        await ensureServiceWorker();
        if (pollTimer) clearInterval(pollTimer);
        const tick = async () => {
            try {
                const latest = await loadPrefs();
                if (!latest.enabled || !latest.subscriptions.length || Notification.permission !== 'granted') {
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
        if (typeof Notification === 'undefined') {
            return 'unsupported';
        }
        await ensureServiceWorker();
        if (Notification.permission === 'granted') return 'granted';
        if (Notification.permission === 'denied') return 'denied';
        const result = await Notification.requestPermission();
        return result;
    }

    async function sendTestNotification() {
        await ensureServiceWorker();
        await showNotification(
            'Test detection alert',
            'Browser notifications are working for Malla detection sensors.',
            { url: '/sensor-dashboard', tag: 'detection-test' }
        );
    }

    window.DetectionNotifications = {
        start,
        stop,
        requestPermission,
        sendTestNotification,
        ensureServiceWorker,
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
