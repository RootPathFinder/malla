#!/usr/bin/env python3
"""
Meshtastic Mesh Health Web UI - Main Application

A Flask web application for browsing and analyzing Meshtastic mesh network data.
This is the main entry point for the web UI component.
"""

import atexit
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path

from flask import Flask

from .config import AppConfig, get_config
from .database.connection import init_database
from .routes import register_routes
from .services.alert_service import AlertService
from .services.log_service import install_log_handler
from .services.power_monitor import start_power_monitor, stop_power_monitor
from .utils.formatting import format_node_id, format_time_ago
from .utils.node_utils import start_cache_cleanup, stop_cache_cleanup


def start_auto_archive_stale_nodes(interval_seconds=86400):
    """Start a background thread to auto-archive stale nodes periodically."""

    def archive_loop():
        while True:
            try:
                logger.info("Auto-archiving stale nodes...")
                result = AlertService.archive_stale_nodes()
                logger.info(
                    f"Auto-archived {result['archived_count']} nodes (failures: {result['failed_count']})"
                )
            except Exception as e:
                logger.error(f"Error in auto-archive thread: {e}")
            time.sleep(interval_seconds)

    t = threading.Thread(target=archive_loop, daemon=True)
    t.start()


def _auto_connect_services(cfg: AppConfig) -> None:
    """Auto-connect admin TCP and/or start bot based on configuration.

    Runs in a background thread to avoid blocking startup.
    """

    def connect_async():
        # Auto-connect admin TCP if configured
        if cfg.admin_auto_connect and cfg.admin_connection_type == "tcp":
            try:
                from .services.tcp_publisher import get_tcp_publisher

                logger.info(
                    f"Auto-connecting admin TCP to {cfg.admin_tcp_host}:{cfg.admin_tcp_port}..."
                )
                tcp_publisher = get_tcp_publisher()
                if tcp_publisher.connect():
                    logger.info("Admin TCP auto-connected successfully")
                else:
                    logger.warning(
                        "Admin TCP auto-connect failed - connection will need to be established manually"
                    )
            except Exception as e:
                logger.error(f"Error during admin TCP auto-connect: {e}")

        # Auto-start bot if configured (requires TCP connection)
        if cfg.bot_auto_start:
            try:
                from .services.bot_service import get_bot_service

                logger.info("Auto-starting bot service...")
                bot = get_bot_service()
                bot.start()
                logger.info("Bot service auto-started successfully")
            except Exception as e:
                logger.error(f"Error during bot auto-start: {e}")

    # Run connection in background thread to avoid blocking startup
    t = threading.Thread(target=connect_async, daemon=True, name="auto-connect")
    t.start()
    logger.info("Auto-connect scheduled in background thread")


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

# Install memory log handler for admin log viewing
install_log_handler(level=logging.DEBUG)

logger = logging.getLogger(__name__)


def make_json_safe(obj):
    """
    Recursively convert an object to be JSON-serializable by handling bytes objects.

    Args:
        obj: The object to make JSON-safe

    Returns:
        A JSON-serializable version of the object
    """
    if isinstance(obj, bytes):
        # Convert bytes to hex string
        return obj.hex()
    elif isinstance(obj, dict):
        return {key: make_json_safe(value) for key, value in obj.items()}
    elif isinstance(obj, list | tuple):
        return [make_json_safe(item) for item in obj]
    elif hasattr(obj, "__dict__"):
        # Handle objects with attributes by converting to dict
        return make_json_safe(obj.__dict__)
    else:
        # Return as-is for JSON-serializable types (str, int, float, bool, None)
        return obj


def create_app(cfg: AppConfig | None = None):  # noqa: D401
    """Create and configure the Flask application.

    If *cfg* is ``None`` the configuration is loaded via :func:`get_config`.
    Tests can pass an :class:`~malla.config.AppConfig` instance directly which
    eliminates the need for fiddling with environment variables.
    """

    logger.info("Creating Flask application")

    # Get the package directory for templates and static files
    package_dir = Path(__file__).parent

    app = Flask(
        __name__,
        template_folder=str(package_dir / "templates"),
        static_folder=str(package_dir / "static"),
    )

    # ---------------------------------------------------------------------
    # Enable response compression for better performance
    # ---------------------------------------------------------------------
    try:
        from flask_compress import Compress

        Compress(app)
        logger.info("Response compression enabled")
    except ImportError:
        logger.warning("flask-compress not installed - response compression disabled")

    # ---------------------------------------------------------------------
    # Load application configuration (YAML + environment overrides)
    # ---------------------------------------------------------------------

    if cfg is None:
        cfg = get_config()
    else:
        # Ensure subsequent calls to get_config() return this instance (tests)
        from .config import _override_config  # local import to avoid circular

        _override_config(cfg)

    # Persist config on Flask instance for later use
    app.config["APP_CONFIG"] = cfg

    # Setup OpenTelemetry if endpoint is configured
    if cfg.otlp_endpoint:
        from .telemetry import setup_telemetry

        setup_telemetry(app, cfg.otlp_endpoint)

    # Mirror a few frequently-used values to top-level keys for backwards
    # compatibility with the existing code base. Over time we should migrate
    # direct usages to the nested ``APP_CONFIG`` object instead.
    app.config["SECRET_KEY"] = cfg.secret_key
    app.config["DATABASE_FILE"] = cfg.database_file

    # Ensure helper modules relying on env-var fallback pick up the correct DB
    # path in contexts where they cannot access Flask's app.config (e.g.
    # standalone scripts).  This is primarily relevant for the test suite.
    os.environ["MALLA_DATABASE_FILE"] = str(cfg.database_file)

    # ---------------------------------------------------------------------

    # Add template filters for consistent formatting
    @app.template_filter("format_node_id")
    def format_node_id_filter(node_id):
        """Template filter for consistent node ID formatting."""
        return format_node_id(node_id)

    @app.template_filter("format_node_short_name")
    def format_node_short_name_filter(node_name):
        """Template filter for short node names."""
        if not node_name:
            return "Unknown"
        # If it's a long name with hex ID in parentheses, extract just the name part
        if " (" in node_name and node_name.endswith(")"):
            return node_name.split(" (")[0]
        return node_name

    @app.template_filter("format_time_ago")
    def format_time_ago_filter(dt):
        """Template filter for relative time formatting."""
        return format_time_ago(dt)

    @app.template_filter("safe_json")
    def safe_json_filter(obj, indent=None):
        """
        Template filter for safely serializing objects to JSON, handling bytes objects.

        Args:
            obj: The object to serialize
            indent: Optional indentation for pretty printing

        Returns:
            JSON string with bytes objects converted to hex strings
        """
        try:
            safe_obj = make_json_safe(obj)
            return json.dumps(safe_obj, indent=indent, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Error in safe_json filter: {e}")
            return json.dumps(
                {"error": f"Serialization failed: {str(e)}"}, indent=indent
            )

    @app.template_filter("format_rssi")
    def format_rssi_filter(rssi):
        """Template filter for consistent RSSI formatting with 1 decimal place."""
        if rssi is None:
            return "N/A"
        try:
            return f"{float(rssi):.1f}"
        except (ValueError, TypeError):
            return str(rssi)

    @app.template_filter("format_snr")
    def format_snr_filter(snr):
        """Template filter for consistent SNR formatting with 2 decimal places."""
        if snr is None:
            return "N/A"
        try:
            return f"{float(snr):.2f}"
        except (ValueError, TypeError):
            return str(snr)

    @app.template_filter("format_signal")
    def format_signal_filter(value, decimals=1):
        """Template filter for consistent signal value formatting with configurable decimal places."""
        if value is None:
            return "N/A"
        try:
            return f"{float(value):.{decimals}f}"
        except (ValueError, TypeError):
            return str(value)

    @app.template_filter("timestamp_to_datetime")
    def timestamp_to_datetime_filter(timestamp):
        """Template filter to convert Unix timestamp to readable datetime string."""
        if timestamp is None:
            return "N/A"
        try:
            from datetime import datetime

            dt = datetime.fromtimestamp(float(timestamp))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError, OSError):
            return str(timestamp)

    @app.template_filter("format_timestamp")
    def format_timestamp_filter(timestamp):
        """Template filter to format Unix timestamp as human-readable datetime."""
        if timestamp is None:
            return "N/A"
        try:
            from datetime import datetime

            dt = datetime.fromtimestamp(float(timestamp))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError, OSError):
            return str(timestamp)

    # ------------------------------------------------------------------
    # Markdown rendering filter & context processor for config variables
    # ------------------------------------------------------------------

    try:
        import markdown as _markdown  # import locally to avoid hard dependency at runtime until used
    except ModuleNotFoundError:  # pragma: no cover – dependency should be present
        _markdown = None  # type: ignore[assignment]

    @app.template_filter("markdown")
    def markdown_filter(text: str | None):  # noqa: ANN001
        """Render *text* (Markdown) to HTML for safe embedding."""

        if text is None:
            return ""
        if _markdown is None:
            logger.warning("markdown package not installed – returning raw text")
            return text
        from markupsafe import Markup

        return Markup(_markdown.markdown(text))

    @app.context_processor
    def inject_config():
        """Inject selected config values into all templates."""

        return {
            "APP_NAME": cfg.name,
            "APP_CONFIG": cfg,
            "DATABASE_FILE": cfg.database_file,
        }

    # Initialize database
    logger.info("Initializing database connection")
    init_database()

    # Initialize authentication system
    from .services.auth_service import init_auth

    logger.info("Initializing authentication system")
    init_auth(app)

    # Initialize admin tables
    from .database.admin_repository import init_admin_tables

    logger.info("Initializing admin tables")
    init_admin_tables()

    # Initialize job service for background jobs
    from .services.job_service import init_job_service

    logger.info("Initializing job service for background operations")
    init_job_service()

    # Auto-connect admin TCP and/or bot if configured
    _auto_connect_services(cfg)

    # Start periodic cache cleanup for node names
    logger.info("Starting node name cache cleanup background thread")
    start_cache_cleanup()

    # Start periodic power type detection (runs every 10 minutes)
    logger.info("Starting power type monitor background thread")
    start_power_monitor(interval_seconds=600)  # Run every 10 minutes

    # Start auto-archive background thread (every 24 hours)
    logger.info("Starting auto-archive stale nodes background thread")
    start_auto_archive_stale_nodes(interval_seconds=86400)

    # Register cleanup on app shutdown
    atexit.register(stop_cache_cleanup)
    atexit.register(stop_power_monitor)

    # Register all routes
    logger.info("Registering application routes")
    register_routes(app)

    # Add health check endpoint
    @app.route("/health")
    def health_check():
        """Health check endpoint for monitoring."""
        return {
            "status": "healthy",
            "service": "meshtastic-mesh-health-ui",
            "version": "2.0.0",
        }

    # Add application info
    @app.route("/info")
    def app_info():
        """Application information endpoint."""
        return {
            "name": "Meshtastic Mesh Health Web UI",
            "version": "2.0.0",
            "description": "Web interface for monitoring Meshtastic mesh network health",
            "database_file": app.config["DATABASE_FILE"],
            "components": {
                "database": "Repository pattern with SQLite",
                "models": "Data models and packet parsing",
                "services": "Business logic layer",
                "utils": "Utility functions",
                "routes": "HTTP request handling",
            },
        }

    logger.info("Flask application created successfully")
    return app


def main():
    """Main entry point for the application."""
    logger.info("Starting Meshtastic Mesh Health Web UI")

    try:
        # Create the application
        app = create_app()

        # Use configuration values (environment overrides already applied)
        cfg: AppConfig = app.config.get("APP_CONFIG")  # type: ignore[assignment]

        host = cfg.host
        port = cfg.port
        debug = cfg.debug

        # Print startup information
        print("=" * 60)
        print("🌐 Meshtastic Mesh Health Web UI")
        print("=" * 60)
        print(f"Database: {app.config['DATABASE_FILE']}")
        print(f"Web UI: http://{host}:{port}")
        print(f"Debug mode: {debug}")
        print(f"Log level: {logging.getLogger().level}")
        print("=" * 60)
        print()

        logger.info(f"Starting server on {host}:{port} (debug={debug})")

        # Run the application
        app.run(host=host, port=port, debug=debug, threaded=True)

    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
