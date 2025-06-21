"""Flask Blueprint module housing all API/UI routes.

The blueprint is constructed lazily via ``create_blueprint`` to avoid any
circular-import issues with ``app.py``.  The caller (typically ``app.py``)
passes its own module reference so we can access shared globals.
"""
from __future__ import annotations

import os
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, TYPE_CHECKING

from flask import Blueprint, jsonify, request, current_app

if TYPE_CHECKING:
    import types


def create_blueprint(main_app: "types.ModuleType") -> Blueprint:  # noqa: D401
    """Build and return a Blueprint wired to *main_app*'s globals."""

    rate_limiter = main_app.rate_limiter  # type: ignore[attr-defined]
    require_api_key = main_app.require_api_key  # type: ignore[attr-defined]

    g_chart_data_cache: Dict[str, Any] = main_app.g_chart_data_cache  # type: ignore[attr-defined]
    get_chart_data = main_app.get_chart_data  # type: ignore[attr-defined]
    config: Dict[str, Any] = main_app.config  # type: ignore[attr-defined]
    BASE_DIR: str = main_app.BASE_DIR  # type: ignore[attr-defined]

    bp = Blueprint("routes", __name__)

    # ─── utility endpoints ──────────────────────────────────────────────────
    @bp.route("/api/validate_key")
    @require_api_key
    @rate_limiter
    def validate_key():  # type: ignore[reuse]
        return jsonify({"valid": True})

    @bp.route("/api/heartbeat")
    def heartbeat():  # type: ignore[reuse]
        WINDOW = main_app.WINDOW  # type: ignore[attr-defined]
        MAX_REQ = main_app.MAX_REQ  # type: ignore[attr-defined]
        REQUESTS_PER_IP = main_app.REQUESTS_PER_IP  # type: ignore[attr-defined]

        ip = request.remote_addr or "unknown"
        now = time.time()
        lst: List[float] = REQUESTS_PER_IP.get(ip, [])
        lst = [t for t in lst if t >= now - WINDOW]
        return jsonify(
            {
                "heartbeat": True,
                "requests_left": MAX_REQ - len(lst),
                "ratelimit_reset": int((lst and (WINDOW - (now - lst[0]))) or 0),
            }
        )



    # ─── config / data endpoints ───────────────────────────────────────────
    @bp.route("/api/data")
    @rate_limiter
    def get_data():
        """
        Main data endpoint. It's stateless and reads all parameters from the
        request query string, falling back to the startup config for defaults.
        """
        # Standardize to snake_case.
        start_date_str = request.args.get('start_date', config['Start_Date'])
        days_to_show = int(request.args.get('days_to_show', config.get('DaysToShow', 7)))
        maps_to_show = int(request.args.get('maps_to_show', config.get('MapsToShow', 10)))
        percision = int(request.args.get('percision', config.get('Percision', 2)))
        color_intensity = float(request.args.get('color_intensity', config.get('ColorIntensity', 1.0)))
        bias_exponent = float(request.args.get('bias_exponent', config.get('BiasExponent', 1.0)))

        only_maps_containing_str = request.args.get('only_maps_containing', None)

        if only_maps_containing_str is not None:
            only_maps_containing = [s.strip() for s in only_maps_containing_str.split(',') if s.strip()]
        else:
            # Gracefully handle if default is not a list
            default_maps = config.get('OnlyMapsContaining', [])
            only_maps_containing = default_maps if isinstance(default_maps, list) else []

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
    @rate_limiter
    def get_freshness():  # type: ignore[reuse]
        freshness = main_app.get_data_freshness()  # type: ignore[attr-defined]
        return jsonify({"latest_scan": freshness})

    @bp.route("/api/csv_status")
    @rate_limiter
    def csv_status():  # type: ignore[reuse]
        path = os.path.join(BASE_DIR, config["Filename"])
        return jsonify({"exists": os.path.exists(path), "empty": (os.path.getsize(path) == 0) if os.path.exists(path) else True})

    @bp.route("/api/date_range")
    @rate_limiter
    def date_range():
        get_date_range = main_app.get_date_range
        return jsonify(get_date_range())

    # ─── UI route ───────────────────────────────────────────────────────────
    @bp.route("/")
    def index():  # type: ignore[reuse]
        return current_app.send_static_file("index.html")

    return bp

