"""
Routes package for Meshtastic Mesh Health Web UI
"""

from .admin_routes import admin_bp
from .alert_routes import alert_bp
from .api_routes import api_bp
from .auth_routes import auth_bp
from .battery_routes import battery_bp
from .bot_routes import bot_bp
from .dashboard_routes import dashboard_bp
from .gateway_routes import gateway_bp
from .job_routes import job_bp

# Import all route blueprints
from .main_routes import main_bp
from .mesh_routes import mesh_bp
from .node_routes import node_bp
from .packet_routes import packet_bp
from .traceroute_routes import traceroute_bp


def register_routes(app):
    """Register all route blueprints with the Flask app."""
    app.register_blueprint(auth_bp)  # Auth routes first for login
    app.register_blueprint(main_bp)
    app.register_blueprint(packet_bp)
    app.register_blueprint(node_bp)
    app.register_blueprint(traceroute_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(gateway_bp)
    app.register_blueprint(battery_bp)
    app.register_blueprint(mesh_bp)
    app.register_blueprint(alert_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(job_bp)
    app.register_blueprint(bot_bp)
    app.register_blueprint(dashboard_bp)
