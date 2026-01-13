"""
User preferences management for storing and retrieving user settings.

Provides a lightweight preference storage system using browser local storage
via JavaScript API, with server-side API for synchronization and backup.
"""

import json
import logging

logger = logging.getLogger(__name__)


class UserPreferences:
    """
    User preferences with defaults and validation.

    Preferences are stored in browser local storage and can be optionally
    synced to server for backup or cross-device synchronization.
    """

    DEFAULT_PREFERENCES = {
        "theme": "light",  # light, dark, auto
        "dashboard": {
            "auto_refresh": True,
            "refresh_interval": 30,  # seconds
            "default_gateway": None,
            "show_offline_nodes": True,
            "compact_view": False,
        },
        "map": {
            "default_center": None,  # [lat, lon]
            "default_zoom": 10,
            "show_labels": True,
            "show_links": True,
            "cluster_nodes": False,
        },
        "packets": {
            "default_page_size": 50,
            "show_raw_data": False,
            "highlight_errors": True,
        },
        "nodes": {
            "default_page_size": 50,
            "show_hardware_icons": True,
            "sort_by": "last_seen",
            "sort_order": "desc",
        },
        "notifications": {
            "enabled": False,
            "low_battery_alerts": True,
            "new_node_alerts": True,
            "critical_alerts_only": False,
        },
        "charts": {
            "animation_enabled": True,
            "color_scheme": "default",
            "show_legends": True,
        },
        "accessibility": {
            "high_contrast": False,
            "reduced_motion": False,
            "screen_reader_mode": False,
        },
    }

    @classmethod
    def get_default_preferences(cls) -> dict:
        """Get default preferences."""
        return json.loads(json.dumps(cls.DEFAULT_PREFERENCES))  # Deep copy

    @classmethod
    def validate_preferences(cls, prefs: dict) -> dict:
        """
        Validate and sanitize user preferences.

        Args:
            prefs: User preferences dictionary

        Returns:
            dict: Validated preferences with defaults for missing values
        """
        validated = cls.get_default_preferences()

        # Merge user preferences with defaults
        def merge_dicts(default: dict, user: dict) -> dict:
            result = default.copy()
            for key, value in user.items():
                if key in result:
                    if isinstance(result[key], dict) and isinstance(value, dict):
                        result[key] = merge_dicts(result[key], value)
                    else:
                        # Validate type matches
                        if type(result[key]) is type(value):
                            result[key] = value
                        else:
                            logger.warning(
                                f"Preference type mismatch for {key}: "
                                f"expected {type(result[key])}, got {type(value)}"
                            )
            return result

        validated = merge_dicts(validated, prefs)

        # Additional validation rules
        if validated["dashboard"]["refresh_interval"] < 5:
            validated["dashboard"]["refresh_interval"] = 5
        if validated["dashboard"]["refresh_interval"] > 300:
            validated["dashboard"]["refresh_interval"] = 300

        if validated["packets"]["default_page_size"] < 10:
            validated["packets"]["default_page_size"] = 10
        if validated["packets"]["default_page_size"] > 1000:
            validated["packets"]["default_page_size"] = 1000

        if validated["nodes"]["default_page_size"] < 10:
            validated["nodes"]["default_page_size"] = 10
        if validated["nodes"]["default_page_size"] > 1000:
            validated["nodes"]["default_page_size"] = 1000

        return validated

    @classmethod
    def get_preference_schema(cls) -> dict:
        """
        Get preference schema for UI generation.

        Returns:
            dict: Preference schema with types and descriptions
        """
        return {
            "theme": {
                "type": "select",
                "options": ["light", "dark", "auto"],
                "default": "light",
                "label": "Color Theme",
                "description": "Choose your preferred color scheme",
            },
            "dashboard": {
                "type": "group",
                "label": "Dashboard Settings",
                "fields": {
                    "auto_refresh": {
                        "type": "boolean",
                        "default": True,
                        "label": "Auto Refresh",
                        "description": "Automatically refresh dashboard data",
                    },
                    "refresh_interval": {
                        "type": "number",
                        "min": 5,
                        "max": 300,
                        "default": 30,
                        "label": "Refresh Interval (seconds)",
                        "description": "How often to refresh data",
                    },
                    "show_offline_nodes": {
                        "type": "boolean",
                        "default": True,
                        "label": "Show Offline Nodes",
                        "description": "Display nodes that are currently offline",
                    },
                    "compact_view": {
                        "type": "boolean",
                        "default": False,
                        "label": "Compact View",
                        "description": "Use compact layout to show more information",
                    },
                },
            },
            "map": {
                "type": "group",
                "label": "Map Settings",
                "fields": {
                    "default_zoom": {
                        "type": "number",
                        "min": 1,
                        "max": 20,
                        "default": 10,
                        "label": "Default Zoom Level",
                        "description": "Initial zoom level for map view",
                    },
                    "show_labels": {
                        "type": "boolean",
                        "default": True,
                        "label": "Show Labels",
                        "description": "Display node labels on map",
                    },
                    "show_links": {
                        "type": "boolean",
                        "default": True,
                        "label": "Show Links",
                        "description": "Display RF links between nodes",
                    },
                    "cluster_nodes": {
                        "type": "boolean",
                        "default": False,
                        "label": "Cluster Nodes",
                        "description": "Group nearby nodes at low zoom levels",
                    },
                },
            },
            "packets": {
                "type": "group",
                "label": "Packet View Settings",
                "fields": {
                    "default_page_size": {
                        "type": "number",
                        "min": 10,
                        "max": 1000,
                        "default": 50,
                        "label": "Page Size",
                        "description": "Number of packets per page",
                    },
                    "show_raw_data": {
                        "type": "boolean",
                        "default": False,
                        "label": "Show Raw Data",
                        "description": "Display raw packet data by default",
                    },
                    "highlight_errors": {
                        "type": "boolean",
                        "default": True,
                        "label": "Highlight Errors",
                        "description": "Highlight packets with errors",
                    },
                },
            },
            "nodes": {
                "type": "group",
                "label": "Node View Settings",
                "fields": {
                    "default_page_size": {
                        "type": "number",
                        "min": 10,
                        "max": 1000,
                        "default": 50,
                        "label": "Page Size",
                        "description": "Number of nodes per page",
                    },
                    "show_hardware_icons": {
                        "type": "boolean",
                        "default": True,
                        "label": "Show Hardware Icons",
                        "description": "Display hardware icons for nodes",
                    },
                    "sort_by": {
                        "type": "select",
                        "options": [
                            "last_seen",
                            "first_seen",
                            "node_name",
                            "battery_level",
                        ],
                        "default": "last_seen",
                        "label": "Sort By",
                        "description": "Default sort field",
                    },
                    "sort_order": {
                        "type": "select",
                        "options": ["asc", "desc"],
                        "default": "desc",
                        "label": "Sort Order",
                        "description": "Sort direction",
                    },
                },
            },
            "notifications": {
                "type": "group",
                "label": "Notification Settings",
                "fields": {
                    "enabled": {
                        "type": "boolean",
                        "default": False,
                        "label": "Enable Notifications",
                        "description": "Enable browser notifications",
                    },
                    "low_battery_alerts": {
                        "type": "boolean",
                        "default": True,
                        "label": "Low Battery Alerts",
                        "description": "Notify when nodes have low battery",
                    },
                    "new_node_alerts": {
                        "type": "boolean",
                        "default": True,
                        "label": "New Node Alerts",
                        "description": "Notify when new nodes join the network",
                    },
                    "critical_alerts_only": {
                        "type": "boolean",
                        "default": False,
                        "label": "Critical Alerts Only",
                        "description": "Only show critical notifications",
                    },
                },
            },
            "charts": {
                "type": "group",
                "label": "Chart Settings",
                "fields": {
                    "animation_enabled": {
                        "type": "boolean",
                        "default": True,
                        "label": "Enable Animations",
                        "description": "Animate chart transitions",
                    },
                    "show_legends": {
                        "type": "boolean",
                        "default": True,
                        "label": "Show Legends",
                        "description": "Display chart legends",
                    },
                },
            },
            "accessibility": {
                "type": "group",
                "label": "Accessibility Settings",
                "fields": {
                    "high_contrast": {
                        "type": "boolean",
                        "default": False,
                        "label": "High Contrast Mode",
                        "description": "Use high contrast colors",
                    },
                    "reduced_motion": {
                        "type": "boolean",
                        "default": False,
                        "label": "Reduced Motion",
                        "description": "Minimize animations and transitions",
                    },
                    "screen_reader_mode": {
                        "type": "boolean",
                        "default": False,
                        "label": "Screen Reader Mode",
                        "description": "Optimize for screen readers",
                    },
                },
            },
        }
