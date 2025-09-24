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
import duckdb

if TYPE_CHECKING:
    import types

def create_blueprint(main_app: "types.ModuleType") -> Blueprint:
    """Build and return a Blueprint wired to *main_app*'s globals."""

    get_chart_data = main_app.get_chart_data
    get_data_freshness = main_app.get_data_freshness
    get_date_range = main_app.get_date_range
    config = main_app.config
    BASE_DIR = main_app.BASE_DIR
    DB_FILE = main_app.DB_FILE
    rate_limiter = getattr(main_app, 'rate_limiter', None)

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
        # Clamp numeric inputs to reasonable ranges to protect the server.
        def _to_int(name, default):
            try:
                return int(request.args.get(name, default))
            except Exception:
                return default
        def _to_float(name, default):
            try:
                return float(request.args.get(name, default))
            except Exception:
                return default

        days_to_show = _to_int('days_to_show', 7)
        maps_to_show = _to_int('maps_to_show', 10)
        percision = _to_int('percision', 2)
        color_intensity = _to_int('color_intensity', 50)
        bias_exponent = _to_float('bias_exponent', 1.2)

        only_maps_containing_str = request.args.get('only_maps_containing', '')
        only_maps_containing = [s.strip() for s in only_maps_containing_str.split(',') if s.strip()]

        append_maps_containing_str = request.args.get('append_maps_containing', '')
        append_maps_containing = [s.strip() for s in append_maps_containing_str.split(',') if s.strip()]

        top_servers = _to_int('top_servers', 10)

        # Server filter: 'ALL' or 'IP:PORT'
        server_filter = request.args.get('server_filter', 'ALL').strip() or 'ALL'

        # Apply clamping
        days_to_show = max(1, min(365, days_to_show))
        maps_to_show = max(1, min(50, maps_to_show))
        percision = max(0, min(6, percision))
        color_intensity = max(1, min(50, color_intensity))
        bias_exponent = max(0.1, min(8.0, bias_exponent))
        top_servers = max(1, min(50, top_servers))

        chart_data = get_chart_data(
            start_date_str=start_date_str,
            days_to_show=days_to_show,
            only_maps_containing=only_maps_containing,
            maps_to_show=maps_to_show,
            percision=percision,
            color_intensity=color_intensity,
            bias_exponent=bias_exponent,
            top_servers=top_servers,
            append_maps_containing=append_maps_containing,
            server_filter=server_filter
        )
        return jsonify(chart_data)

    @bp.route("/api/data_freshness")
    def get_freshness():
        freshness = get_data_freshness()
        return jsonify({"latest_scan": freshness})

    @bp.route("/api/csv_status")
    def csv_status():
        # Backwards-compatible endpoint name; now reports DuckDB status
        exists = os.path.exists(DB_FILE)
        empty = True
        if exists:
            try:
                with duckdb.connect(DB_FILE, read_only=True) as con:
                    row = con.execute("SELECT COUNT(*) FROM samples").fetchone()
                    empty = (not row) or (row[0] == 0)
            except Exception:
                empty = True
        return jsonify({"exists": exists, "empty": empty})

    @bp.route("/api/date_range")
    def date_range():
        return jsonify(get_date_range())

    @bp.route("/api/data_coverage")
    def data_coverage():
        """Return the overall date bounds and the list of dates that have data (daily granularity)."""
        try:
            with duckdb.connect(DB_FILE, read_only=True) as con:
                row = con.execute("SELECT cast(min(timestamp) as date), cast(max(timestamp) as date) FROM samples").fetchone()
                if not row or row[0] is None or row[1] is None:
                    return jsonify({"start": None, "end": None, "present_dates": []})

                min_d, max_d = row[0], row[1]
                dates = con.execute("SELECT cast(timestamp as date) AS d FROM samples GROUP BY d ORDER BY d").fetchall()
                present_dates = [str(r[0]) for r in dates]
                return jsonify({"start": str(min_d), "end": str(max_d), "present_dates": present_dates})
        except Exception as e:
            return jsonify({"start": None, "end": None, "present_dates": [], "error": str(e)[:200]}), 500

    # ─── UI route ───────────────────────────────────────────────────────────
    @bp.route("/")
    def index():  # type: ignore[reuse]
        return current_app.send_static_file("index.html")

    # Apply rate limiting dynamically if available
    if callable(rate_limiter):
        for endpoint, view in list(bp.view_functions.items()):
            # Avoid double-wrapping
            if getattr(view, '_rate_wrapped', False):
                continue
            wrapped = rate_limiter(view)
            setattr(wrapped, '_rate_wrapped', True)
            bp.view_functions[endpoint] = wrapped

    return bp

