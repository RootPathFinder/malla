/**
 * User Preferences Manager
 * Centralized preference management using database (via API) for authenticated users
 * Falls back to localStorage for non-authenticated users or when API is unavailable
 */

(function() {
    'use strict';

    const PREFERENCES_KEYS = {
        TEMPERATURE_UNIT: 'temperature_unit',
        TIMEZONE: 'timezone',
        PINNED_POLL_INTERVAL: 'pinned_poll_interval'
    };

    // Legacy localStorage keys for backwards compatibility
    const LEGACY_KEYS = {
        TEMPERATURE_UNIT: 'malla-temperature-unit',
        TIMEZONE: 'malla-timezone-preference',
        PINNED_POLL_INTERVAL: 'malla-pinned-poll-interval'
    };

    // Older telemetry UI accidentally used this key/value scheme
    const OBSOLETE_TEMP_KEY = 'temperatureUnit';

    const DEFAULT_VALUES = {
        TEMPERATURE_UNIT: 'C',
        TIMEZONE: 'local',
        PINNED_POLL_INTERVAL: 5
    };

    // Cache for preferences loaded from server
    let preferencesCache = null;
    let isAuthenticated = false;
    let loadPromise = null;

    /**
     * Check if user is authenticated by looking for user indicators in the DOM
     */
    function checkAuthentication() {
        // Check for user dropdown or logout button
        return document.getElementById('userDropdown') !== null;
    }

    /**
     * Migrate obsolete temperatureUnit=celsius|fahrenheit localStorage entries.
     */
    function migrateObsoleteTemperatureKey() {
        try {
            const obsolete = localStorage.getItem(OBSOLETE_TEMP_KEY);
            if (!obsolete) {
                return;
            }
            const existing = localStorage.getItem(LEGACY_KEYS.TEMPERATURE_UNIT);
            if (!existing) {
                const normalized = String(obsolete).toLowerCase();
                if (normalized === 'fahrenheit' || normalized === 'f') {
                    localStorage.setItem(LEGACY_KEYS.TEMPERATURE_UNIT, 'F');
                } else if (normalized === 'celsius' || normalized === 'c') {
                    localStorage.setItem(LEGACY_KEYS.TEMPERATURE_UNIT, 'C');
                }
            }
            localStorage.removeItem(OBSOLETE_TEMP_KEY);
        } catch (e) {
            // ignore storage access errors
        }
    }

    /**
     * Load preferences from server
     * @returns {Promise<Object>} Preferences object
     */
    async function loadPreferencesFromServer() {
        try {
            const response = await fetch('/api/preferences');
            if (response.ok) {
                const data = await response.json();
                preferencesCache = data.preferences || {};
                return preferencesCache;
            }
            return {};
        } catch (e) {
            console.debug('Could not load preferences from server:', e);
            return {};
        }
    }

    /**
     * Mirror cached server preferences into localStorage so legacy readers
     * (TemperatureToggle, maps, etc.) see the profile value on every page.
     */
    function syncCacheToLocalStorage() {
        if (!preferencesCache) {
            return;
        }

        try {
            const temp = preferencesCache[PREFERENCES_KEYS.TEMPERATURE_UNIT];
            if (temp === 'C' || temp === 'F') {
                localStorage.setItem(LEGACY_KEYS.TEMPERATURE_UNIT, temp);
            }

            const tz = preferencesCache[PREFERENCES_KEYS.TIMEZONE];
            if (tz === 'local' || tz === 'utc') {
                localStorage.setItem(LEGACY_KEYS.TIMEZONE, tz);
            }

            const poll = preferencesCache[PREFERENCES_KEYS.PINNED_POLL_INTERVAL];
            if (poll !== undefined && poll !== null) {
                localStorage.setItem(LEGACY_KEYS.PINNED_POLL_INTERVAL, String(poll));
            }
        } catch (e) {
            console.error('Error syncing preferences to localStorage:', e);
        }
    }

    /**
     * Save a preference to server
     * @param {string} key - Preference key
     * @param {any} value - Preference value
     * @returns {Promise<boolean>} Success
     */
    async function savePreferenceToServer(key, value) {
        try {
            const response = await fetch(`/api/preferences/${key}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ value })
            });
            if (response.ok) {
                if (preferencesCache) {
                    preferencesCache[key] = value;
                }
                return true;
            }
            return false;
        } catch (e) {
            console.debug('Could not save preference to server:', e);
            return false;
        }
    }

    /**
     * Get a preference value from cache/localStorage
     * @param {string} key - Preference key
     * @param {string} legacyKey - Legacy localStorage key
     * @param {any} defaultValue - Default value if not set
     * @returns {any} The preference value
     */
    function getPreference(key, legacyKey, defaultValue) {
        // Try cache first (from server)
        if (preferencesCache && preferencesCache[key] !== undefined) {
            return preferencesCache[key];
        }
        // Fall back to localStorage (legacy key)
        try {
            const value = localStorage.getItem(legacyKey);
            return value !== null ? value : defaultValue;
        } catch (e) {
            console.error('Error reading preference:', e);
            return defaultValue;
        }
    }

    /**
     * Set a preference value
     * @param {string} key - Preference key
     * @param {string} legacyKey - Legacy localStorage key
     * @param {any} value - Value to set
     */
    async function setPreference(key, legacyKey, value) {
        // Update cache immediately
        if (!preferencesCache) {
            preferencesCache = {};
        }
        preferencesCache[key] = value;

        // Always update localStorage for immediate effect and backwards compatibility
        try {
            localStorage.setItem(legacyKey, value.toString());
        } catch (e) {
            console.error('Error saving to localStorage:', e);
        }

        // Save to server if authenticated
        if (isAuthenticated) {
            await savePreferenceToServer(key, value);
        }

        // Dispatch event for other components
        window.dispatchEvent(new CustomEvent('preferenceChanged', {
            detail: { key, value }
        }));
    }

    /**
     * Get temperature unit preference
     * @returns {string} 'C' or 'F'
     */
    function getTemperatureUnit() {
        const value = getPreference(
            PREFERENCES_KEYS.TEMPERATURE_UNIT,
            LEGACY_KEYS.TEMPERATURE_UNIT,
            DEFAULT_VALUES.TEMPERATURE_UNIT
        );
        return ['C', 'F'].includes(value) ? value : DEFAULT_VALUES.TEMPERATURE_UNIT;
    }

    /**
     * Set temperature unit preference
     * @param {string} unit - 'C' or 'F'
     */
    async function setTemperatureUnit(unit) {
        if (!['C', 'F'].includes(unit)) {
            console.warn('Invalid temperature unit:', unit);
            return;
        }
        await setPreference(
            PREFERENCES_KEYS.TEMPERATURE_UNIT,
            LEGACY_KEYS.TEMPERATURE_UNIT,
            unit
        );

        // Dispatch specific event for temperature toggle component
        window.dispatchEvent(new CustomEvent('temperatureUnitChanged', {
            detail: { unit }
        }));
    }

    /**
     * Get timezone preference
     * @returns {string} 'local' or 'utc'
     */
    function getTimezone() {
        const value = getPreference(
            PREFERENCES_KEYS.TIMEZONE,
            LEGACY_KEYS.TIMEZONE,
            DEFAULT_VALUES.TIMEZONE
        );
        return ['local', 'utc'].includes(value) ? value : DEFAULT_VALUES.TIMEZONE;
    }

    /**
     * Set timezone preference
     * @param {string} timezone - 'local' or 'utc'
     */
    async function setTimezone(timezone) {
        if (!['local', 'utc'].includes(timezone)) {
            console.warn('Invalid timezone:', timezone);
            return;
        }
        await setPreference(
            PREFERENCES_KEYS.TIMEZONE,
            LEGACY_KEYS.TIMEZONE,
            timezone
        );

        // Dispatch specific event for timezone toggle component
        window.dispatchEvent(new CustomEvent('timezoneChanged', {
            detail: { timezone }
        }));
    }

    /**
     * Get pinned nodes poll interval
     * @returns {number} Interval in seconds (1-10)
     */
    function getPinnedPollInterval() {
        const value = parseInt(getPreference(
            PREFERENCES_KEYS.PINNED_POLL_INTERVAL,
            LEGACY_KEYS.PINNED_POLL_INTERVAL,
            DEFAULT_VALUES.PINNED_POLL_INTERVAL
        ), 10);
        if (isNaN(value) || value < 1 || value > 10) {
            return DEFAULT_VALUES.PINNED_POLL_INTERVAL;
        }
        return value;
    }

    /**
     * Set pinned nodes poll interval
     * @param {number} seconds - Interval in seconds (1-10)
     */
    async function setPinnedPollInterval(seconds) {
        const value = parseInt(seconds, 10);
        if (isNaN(value) || value < 1 || value > 10) {
            console.warn('Invalid poll interval:', seconds);
            return;
        }
        await setPreference(
            PREFERENCES_KEYS.PINNED_POLL_INTERVAL,
            LEGACY_KEYS.PINNED_POLL_INTERVAL,
            value
        );

        // Dispatch specific event for pinned nodes component
        window.dispatchEvent(new CustomEvent('pinnedPollIntervalChanged', {
            detail: { interval: value }
        }));
    }

    /**
     * Bind profile page form controls when present.
     */
    function bindPreferencesUI() {
        // Temperature unit selector
        const tempSelect = document.getElementById('pref-temperature-unit');
        if (tempSelect) {
            tempSelect.value = getTemperatureUnit();
            tempSelect.addEventListener('change', async function() {
                await setTemperatureUnit(this.value);
                showSavedIndicator(this);
            });
        }

        // Timezone selector
        const tzSelect = document.getElementById('pref-timezone');
        if (tzSelect) {
            tzSelect.value = getTimezone();
            tzSelect.addEventListener('change', async function() {
                await setTimezone(this.value);
                showSavedIndicator(this);
            });
        }

        // Pinned poll interval
        const pollInput = document.getElementById('pref-pinned-poll-interval');
        const pollValue = document.getElementById('pref-pinned-poll-value');
        if (pollInput) {
            pollInput.value = getPinnedPollInterval();
            if (pollValue) {
                pollValue.textContent = pollInput.value + 's';
            }
            pollInput.addEventListener('input', function() {
                if (pollValue) {
                    pollValue.textContent = this.value + 's';
                }
            });
            pollInput.addEventListener('change', async function() {
                await setPinnedPollInterval(this.value);
                showSavedIndicator(this);
            });
        }
    }

    /**
     * Show a brief "Saved" indicator next to a form element
     * @param {HTMLElement} element - The form element
     */
    function showSavedIndicator(element) {
        // Find the parent container (could be a div for range inputs)
        let parent = element.parentElement;
        if (element.type === 'range') {
            parent = element.closest('.mb-4') || element.parentElement;
        }

        // Find or create the indicator
        let indicator = parent.querySelector('.pref-saved-indicator');
        if (!indicator) {
            indicator = document.createElement('span');
            indicator.className = 'pref-saved-indicator text-success ms-2';
            indicator.innerHTML = '<i class="bi bi-check-circle"></i> Saved';
            indicator.style.transition = 'opacity 0.3s ease';
            indicator.style.opacity = '0';

            // Insert after the element or at end of parent
            if (element.type === 'select-one') {
                element.parentElement.appendChild(indicator);
            } else {
                parent.appendChild(indicator);
            }
        }

        // Show with animation
        indicator.style.opacity = '1';
        setTimeout(() => {
            indicator.style.opacity = '0';
        }, 1500);
    }

    /**
     * Initialize preferences: migrate keys, sync from server for signed-in users,
     * then bind profile UI controls when present.
     * @returns {Promise<void>}
     */
    async function initPreferencesUI() {
        migrateObsoleteTemperatureKey();

        isAuthenticated = checkAuthentication();
        let previousTemp = 'C';
        let previousTz = 'local';
        try {
            previousTemp = localStorage.getItem(LEGACY_KEYS.TEMPERATURE_UNIT) || DEFAULT_VALUES.TEMPERATURE_UNIT;
            previousTz = localStorage.getItem(LEGACY_KEYS.TIMEZONE) || DEFAULT_VALUES.TIMEZONE;
        } catch (e) {
            // ignore
        }

        if (isAuthenticated) {
            await loadPreferencesFromServer();
            syncCacheToLocalStorage();

            const nextTemp = getTemperatureUnit();
            if (nextTemp !== previousTemp) {
                window.dispatchEvent(new CustomEvent('temperatureUnitChanged', {
                    detail: { unit: nextTemp, source: 'server' }
                }));
            }

            const nextTz = getTimezone();
            if (nextTz !== previousTz) {
                window.dispatchEvent(new CustomEvent('timezoneChanged', {
                    detail: { timezone: nextTz, source: 'server' }
                }));
            }
        }

        bindPreferencesUI();
        window.dispatchEvent(new CustomEvent('userPreferencesReady', {
            detail: {
                temperatureUnit: getTemperatureUnit(),
                timezone: getTimezone(),
                pinnedPollInterval: getPinnedPollInterval()
            }
        }));
    }

    /**
     * Ensure preferences are loaded (deduped). Useful for pages that need
     * the server value before rendering temperature-dependent UI.
     */
    function ensureLoaded() {
        if (!loadPromise) {
            loadPromise = initPreferencesUI();
        }
        return loadPromise;
    }

    // Export to global scope
    window.UserPreferences = {
        KEYS: PREFERENCES_KEYS,
        LEGACY_KEYS: LEGACY_KEYS,
        getTemperatureUnit,
        setTemperatureUnit,
        getTimezone,
        setTimezone,
        getPinnedPollInterval,
        setPinnedPollInterval,
        initPreferencesUI,
        loadPreferencesFromServer,
        ensureLoaded
    };

    // Initialize when DOM is ready (every page that includes this script)
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            loadPromise = initPreferencesUI();
        });
    } else {
        loadPromise = initPreferencesUI();
    }
})();
