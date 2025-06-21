"""Flask Blueprint module housing all API/UI routes.

The blueprint is constructed lazily via ``create_blueprint`` to avoid any
circular-import issues with ``app.py``.  The caller (typically ``app.py``)
passes its own module reference so we can access shared globals.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, TYPE_CHECKING

from flask import Blueprint, jsonify, request, current_app

if TYPE_CHECKING:
    import types

def create_blueprint(main_app: "types.ModuleType") -> Blueprint:
    """Build and return a Blueprint wired to *main_app*'s globals."""

    get_chart_data = main_app.get_chart_data
    get_data_freshness = main_app.get_data_freshness
    get_date_range = main_app.get_date_range
    config = main_app.config
    BASE_DIR = main_app.BASE_DIR

    bp = Blueprint("routes", __name__)

    # ─── Data Endpoints ───────────────────────────────────────────────────
    @bp.route("/api/data")
    def get_data():
        """
        Main data endpoint. It's stateless and reads all parameters from the
        request query string, providing sensible defaults.
        """
        today = datetime.utcnow().strftime('%Y-%m-%d')
        start_date_str = request.args.get('start_date', today)
        days_to_show = int(request.args.get('days_to_show', 7))
        maps_to_show = int(request.args.get('maps_to_show', 10))
        percision = int(request.args.get('percision', 2))
        color_intensity = int(request.args.get('color_intensity', 50))
        bias_exponent = float(request.args.get('bias_exponent', 1.2))

        only_maps_containing_str = request.args.get('only_maps_containing', '')
        only_maps_containing = [s.strip() for s in only_maps_containing_str.split(',') if s.strip()]

        chart_data = get_chart_data(
            start_date_str=start_date_str,
            days_to_show=days_to_show,
            only_maps_containing=only_maps_containing,
            maps_to_show=maps_to_show,
            percision=percision,
            color_intensity=color_intensity,
            bias_exponent=bias_exponent
        )
        return jsonify(chart_data)

    @bp.route("/api/data_freshness")
    def get_freshness():
        freshness = get_data_freshness()
        return jsonify({"latest_scan": freshness})

    @bp.route("/api/csv_status")
    def csv_status():
        path = os.path.join(BASE_DIR, config["Filename"])
        exists = os.path.exists(path)
        empty = (os.path.getsize(path) == 0) if exists else True
        return jsonify({"exists": exists, "empty": empty})

    @bp.route("/api/date_range")
    def date_range():
        return jsonify(get_date_range())

    # ─── UI route ───────────────────────────────────────────────────────────
    @bp.route("/")
    def index():  # type: ignore[reuse]
        return current_app.send_static_file("index.html")

    return bp

