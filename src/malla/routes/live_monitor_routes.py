"""
Live Monitor routes for real-time mesh activity display.
"""

import logging

from flask import Blueprint, Response, jsonify, render_template, request

from ..services.live_monitor import get_live_monitor

logger = logging.getLogger(__name__)

live_monitor_bp = Blueprint("live_monitor", __name__)


@live_monitor_bp.route("/live")
def live_monitor_page():
    """Live activity monitor page."""
    logger.info("Live monitor page accessed")
    return render_template("live_monitor.html")


@live_monitor_bp.route("/api/live/stream")
def live_stream():
    """
    Server-Sent Events stream for live activity.
    
    Returns:
        Response: SSE stream with real-time events
    """
    logger.info("Live activity stream endpoint accessed")
    
    def generate():
        monitor = get_live_monitor()
        
        # Send initial connection message
        import json
        conn_msg = json.dumps({"type": "connected", "timestamp": monitor.get_activity_stats()['started_at']})
        yield f"data: {conn_msg}\n\n"
        
        # Stream events
        for event in monitor.get_event_stream():
            yield event
    
    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        }
    )


@live_monitor_bp.route("/api/live/recent")
def live_recent_events():
    """
    Get recent events from history.
    
    Query parameters:
        - limit: Number of events to return (default: 100, max: 1000)
        - type: Filter by event type (optional)
    """
    logger.info("Live recent events endpoint accessed")
    try:
        monitor = get_live_monitor()
        
        limit = min(request.args.get("limit", 100, type=int), 1000)
        event_type = request.args.get("type")
        
        events = monitor.get_recent_events(limit=limit, event_type=event_type)
        
        return jsonify({
            "events": events,
            "count": len(events),
            "limit": limit,
            "type_filter": event_type
        })
        
    except Exception as e:
        logger.error(f"Error in live recent events: {e}")
        return jsonify({"error": str(e)}), 500


@live_monitor_bp.route("/api/live/stats")
def live_stats():
    """Get live activity statistics."""
    logger.info("Live stats endpoint accessed")
    try:
        monitor = get_live_monitor()
        stats = monitor.get_activity_stats()
        
        return jsonify({"stats": stats})
        
    except Exception as e:
        logger.error(f"Error in live stats: {e}")
        return jsonify({"error": str(e)}), 500


@live_monitor_bp.route("/api/live/summary")
def live_summary():
    """
    Get activity summary for recent time window.
    
    Query parameters:
        - window: Time window in seconds (default: 60, max: 3600)
    """
    logger.info("Live summary endpoint accessed")
    try:
        monitor = get_live_monitor()
        
        window = min(request.args.get("window", 60, type=int), 3600)
        summary = monitor.get_activity_summary(time_window=window)
        
        return jsonify({"summary": summary})
        
    except Exception as e:
        logger.error(f"Error in live summary: {e}")
        return jsonify({"error": str(e)}), 500


def register_live_monitor_routes(app):
    """Register live monitor routes with the Flask app."""
    app.register_blueprint(live_monitor_bp)
