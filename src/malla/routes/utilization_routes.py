"""Channel utilization investigation page and APIs."""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify, render_template, request

from ..database.utilization import DEFAULT_HIGH_UTIL_PCT, UtilizationRepository

logger = logging.getLogger(__name__)
utilization_bp = Blueprint("utilization", __name__)


def _hours_arg(default: int = 24) -> int:
    try:
        return int(request.args.get("hours", default))
    except (TypeError, ValueError):
        return default


def _threshold_arg(default: float = DEFAULT_HIGH_UTIL_PCT) -> float:
    try:
        return float(request.args.get("threshold", default))
    except (TypeError, ValueError):
        return default


@utilization_bp.route("/utilization")
def utilization_page():
    """Channel utilization investigation dashboard."""
    logger.info("Utilization page accessed")
    try:
        return render_template(
            "utilization.html",
            default_hours=24,
            high_util_pct=DEFAULT_HIGH_UTIL_PCT,
        )
    except Exception as e:
        logger.error("Error rendering utilization page: %s", e, exc_info=True)
        return f"Utilization page error: {e}", 500


@utilization_bp.route("/api/utilization/summary")
def utilization_summary():
    try:
        data = UtilizationRepository.get_summary(
            hours=_hours_arg(), high_util_pct=_threshold_arg()
        )
        return jsonify(data)
    except Exception as e:
        logger.error("utilization summary API: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


@utilization_bp.route("/api/utilization/nodes")
def utilization_nodes():
    try:
        nodes = UtilizationRepository.get_nodes(
            hours=_hours_arg(), high_util_pct=_threshold_arg()
        )
        return jsonify({"nodes": nodes, "total": len(nodes)})
    except Exception as e:
        logger.error("utilization nodes API: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


@utilization_bp.route("/api/utilization/timeline")
def utilization_timeline():
    try:
        bucket = request.args.get("bucket_mins", type=int)
        data = UtilizationRepository.get_timeline(
            hours=_hours_arg(), bucket_mins=bucket
        )
        return jsonify(data)
    except Exception as e:
        logger.error("utilization timeline API: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


@utilization_bp.route("/api/utilization/talkers")
def utilization_talkers():
    try:
        limit = request.args.get("limit", 25, type=int) or 25
        data = UtilizationRepository.get_talkers(hours=_hours_arg(), limit=limit)
        return jsonify(data)
    except Exception as e:
        logger.error("utilization talkers API: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500
