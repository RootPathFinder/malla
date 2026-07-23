/**
 * Temperature Unit Toggle Component
 * Provides persistent Celsius/Fahrenheit toggle using localStorage
 */

class TemperatureToggle {
    constructor() {
        this.storageKey = 'malla-temperature-unit';
        this.init();
    }

    init() {
        // Apply saved preference on page load
        this.applyPreference();

        // Initialize toggle button if it exists
        this.initToggleButton();

        // Listen for preference changes from other tabs
        window.addEventListener('storage', (e) => {
            if (e.key === this.storageKey) {
                this.applyPreference();
            }
        });
    }

    /**
     * Get the user's temperature unit preference
     * @returns {string} 'C' or 'F'
     */
    getUnit() {
        const saved = localStorage.getItem(this.storageKey);
        if (saved && ['C', 'F'].includes(saved)) {
            return saved;
        }
        return 'C'; // Default to Celsius
    }

    /**
     * Set temperature unit preference
     * @param {string} unit - 'C' or 'F'
     */
    setUnit(unit) {
        if (!['C', 'F'].includes(unit)) {
            console.warn('Invalid temperature unit:', unit);
            return;
        }

        localStorage.setItem(this.storageKey, unit);
        this.applyPreference();

        // Dispatch custom event for other components to listen to
        window.dispatchEvent(new CustomEvent('temperatureUnitChanged', {
            detail: { unit: unit }
        }));

        // Update all temperature displays on the page
        this.updateAllTemperatures();
    }

    /**
     * Update all temperature displays on the page
     */
    updateAllTemperatures() {
        const elements = document.querySelectorAll('[data-temperature-celsius]');
        elements.forEach(el => {
            const celsius = parseFloat(el.getAttribute('data-temperature-celsius'));
            if (!isNaN(celsius)) {
                el.textContent = TemperatureToggle.formatTemperature(celsius, 1);
            }
        });
        this.updateAllDistances();
    }

    /**
     * Update all distance displays that follow the C/F unit preference.
     * C → km/m, F → mi/ft. Elements use data-distance-km (kilometers).
     */
    updateAllDistances() {
        const elements = document.querySelectorAll('[data-distance-km]');
        elements.forEach(el => {
            const km = parseFloat(el.getAttribute('data-distance-km'));
            if (!isNaN(km)) {
                el.textContent = TemperatureToggle.formatDistance(km, 1);
            }
        });
    }

    /**
     * Toggle between Celsius and Fahrenheit
     */
    toggle() {
        const current = this.getUnit();
        const newUnit = current === 'C' ? 'F' : 'C';
        this.setUnit(newUnit);
    }

    /**
     * Apply preference to UI
     */
    applyPreference() {
        this.updateToggleButton();
    }

    /**
     * Initialize and update the toggle button
     */
    initToggleButton() {
        const button = document.getElementById('temperature-toggle');
        if (!button) return;

        // Avoid adding duplicate event listeners
        if (!button.hasAttribute('data-temp-toggle-initialized')) {
            button.addEventListener('click', () => this.toggle());
            button.setAttribute('data-temp-toggle-initialized', 'true');
        }
        this.updateToggleButton();
    }

    /**
     * Update the toggle button to show current unit
     */
    updateToggleButton() {
        const button = document.getElementById('temperature-toggle');
        if (!button) return;

        const unit = this.getUnit();
        const icon = button.querySelector('i');
        if (icon) {
            icon.className = 'bi bi-thermometer-half';
        }
        button.title = `Temperature unit: ${unit} (click to toggle)`;
        button.setAttribute('aria-label', `Current unit: ${unit}. Click to toggle to ${unit === 'C' ? 'F' : 'C'}`);

        // Update button text with current unit
        button.innerHTML = `<i class="bi bi-thermometer-half"></i> ${unit}`;
    }

    /**
     * Convert Celsius to Fahrenheit
     * @param {number} celsius - Temperature in Celsius
     * @returns {number} Temperature in Fahrenheit
     */
    static celsiusToFahrenheit(celsius) {
        if (celsius === null || celsius === undefined) return null;
        return (celsius * 9/5) + 32;
    }

    /**
     * Convert Fahrenheit to Celsius
     * @param {number} fahrenheit - Temperature in Fahrenheit
     * @returns {number} Temperature in Celsius
     */
    static fahrenheitToCelsius(fahrenheit) {
        if (fahrenheit === null || fahrenheit === undefined) return null;
        return (fahrenheit - 32) * 5/9;
    }

    /**
     * Format temperature with unit based on user preference
     * @param {number} celsius - Temperature in Celsius
     * @param {number} decimals - Number of decimal places (default: 1)
     * @returns {string} Formatted temperature string (e.g., "22.5°C" or "72.5°F")
     */
    static formatTemperature(celsius, decimals = 1) {
        if (celsius === null || celsius === undefined) return 'N/A';

        // Use singleton instance or create if needed
        if (!temperatureToggleInstance) {
            temperatureToggleInstance = new TemperatureToggle();
        }
        const unit = temperatureToggleInstance.getUnit();

        let temp = celsius;
        if (unit === 'F') {
            temp = this.celsiusToFahrenheit(celsius);
        }

        return `${temp.toFixed(decimals)}°${unit}`;
    }

    /**
     * Get temperature value in user's preferred unit
     * @param {number} celsius - Temperature in Celsius
     * @param {number} decimals - Number of decimal places (default: 1)
     * @returns {number} Temperature in user's preferred unit
     */
    static getTemperatureInPreferredUnit(celsius, decimals = 1) {
        if (celsius === null || celsius === undefined) return null;

        // Use singleton instance or create if needed
        if (!temperatureToggleInstance) {
            temperatureToggleInstance = new TemperatureToggle();
        }
        const unit = temperatureToggleInstance.getUnit();

        let temp = celsius;
        if (unit === 'F') {
            temp = this.celsiusToFahrenheit(celsius);
        }

        return parseFloat(temp.toFixed(decimals));
    }

    /**
     * Format distance using the temperature unit preference as a metric/imperial switch.
     * C → kilometers / meters, F → miles / feet.
     * @param {number} km - Distance in kilometers
     * @param {number} decimals - Decimal places for km/mi (default: 1)
     * @returns {string} Formatted distance string
     */
    static formatDistance(km, decimals = 1) {
        if (km === null || km === undefined || Number.isNaN(Number(km))) return 'N/A';

        if (!temperatureToggleInstance) {
            temperatureToggleInstance = new TemperatureToggle();
        }
        const unit = temperatureToggleInstance.getUnit();
        const value = Number(km);

        if (unit === 'F') {
            const miles = value * 0.621371192237334;
            if (miles < 1) {
                const feet = Math.round(value * 1000 * 3.280839895);
                return `${feet} ft`;
            }
            return `${miles.toFixed(decimals)} mi`;
        }

        if (value < 1) {
            return `${Math.round(value * 1000)} m`;
        }
        return `${value.toFixed(decimals)} km`;
    }
}

// Global instance for easy access
let temperatureToggleInstance = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    if (!temperatureToggleInstance) {
        temperatureToggleInstance = new TemperatureToggle();
    }
    // Update all temperatures / distances on initial page load
    temperatureToggleInstance.updateAllTemperatures();

    // Listen for temperature unit changes to update displays
    window.addEventListener('temperatureUnitChanged', () => {
        temperatureToggleInstance.updateAllTemperatures();
    });
});

// Expose static methods via a convenience object for use in templates
TemperatureToggle.getUnit = function() {
    if (!temperatureToggleInstance) {
        temperatureToggleInstance = new TemperatureToggle();
    }
    return temperatureToggleInstance.getUnit();
};
