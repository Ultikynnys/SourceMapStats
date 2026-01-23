"""Flask Blueprint module housing all API/UI routes."""
from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from flask import Blueprint, jsonify, request, current_app

from database import (
    get_chart_data,
    get_data_freshness,
    get_date_range,
    DB_FILE
)
from utils import rate_limiter

bp = Blueprint("routes", __name__)

# ─── Data Endpoints ───────────────────────────────────────────────────
@bp.route("/api/data")
@rate_limiter
def get_data():
    """
    Main data endpoint. It's stateless and reads all parameters from the
    request query string, providing sensible defaults.
    """
    from utils import parse_chart_params
    
    # Parse and sanitize parameters
    params = parse_chart_params(request.args)
    
    logging.info(f"Chart request from {request.remote_addr}: {params}")
    
    chart_data = get_chart_data(
        start_date_str=params['start_date_str'],
        days_to_show=params['days_to_show'],
        only_maps_containing=params['only_maps_containing'],
        maps_to_show=params['maps_to_show'],
        percision=params['percision'],
        color_intensity=params['color_intensity'],
        bias_exponent=params['bias_exponent'],
        top_servers=params['top_servers'],
        append_maps_containing=params['append_maps_containing'],
        server_filter=params['server_filter']
    )
    return jsonify(chart_data)

@bp.route("/api/data_freshness")
@rate_limiter
def get_freshness():
    freshness = get_data_freshness()
    return jsonify({"latest_scan": freshness})

@bp.route("/api/csv_status")
@rate_limiter
def csv_status():
    # Backwards-compatible endpoint; now reports status based on cached data
    exists = os.path.exists(DB_FILE)
    # Check if cache has data (non-blocking check)
    dr = get_date_range()
    empty = dr.get('min_date') is None
    return jsonify({"exists": exists, "empty": empty})

@bp.route("/api/date_range")
@rate_limiter
def date_range():
    return jsonify(get_date_range())

@bp.route("/api/data_coverage")
@rate_limiter
def data_coverage():
    """Return the overall date bounds from cache (non-blocking)."""
    # Use cached date range to avoid blocking on database access
    dr = get_date_range()
    return jsonify({
        "start": dr.get('min_date'),
        "end": dr.get('max_date'),
        "present_dates": []  # Omit to avoid DB hit; frontend can infer from start/end
    })

# ─── UI route ───────────────────────────────────────────────────────────
@bp.route("/")
@rate_limiter
def index():
    return current_app.send_static_file("index.html")

# ─── Admin Panel Routes ─────────────────────────────────────────────────
from utils import admin_only, is_admin_ip, get_request_stats, ADMIN_IPS, track_request

@bp.route("/admin")
@admin_only
def admin_panel():
    """Serve the admin panel (only to whitelisted IPs)."""
    # Track admin page views
    track_request(request.remote_addr or 'unknown', '/admin')
    return current_app.send_static_file("admin.html")

@bp.route("/api/admin/stats")
@admin_only
@rate_limiter
def admin_stats():
    """Get request statistics for the admin panel."""
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        date_filter = request.args.get('date') # Defaults to None, which utils handles as 'today'
    except ValueError:
        page = 1
        limit = 50
        date_filter = None
        
    stats = get_request_stats(page=page, limit=limit, date_filter=date_filter)
    return jsonify(stats)

@bp.route("/api/admin/check")
@admin_only
def admin_check():
    """Check if current IP has admin access (for frontend logic)."""
    client_ip = request.remote_addr or 'unknown'
    return jsonify({
        "is_admin": is_admin_ip(client_ip),
        "admin_enabled": len(ADMIN_IPS) > 0
    })

@bp.route("/debug-whoami")
def debug_whoami():
    """Debug endpoint to see what Flask sees about the request."""
    return jsonify({
        "remote_addr": request.remote_addr,
        "access_route": list(request.access_route),
        "headers": dict(request.headers),
        "environ_remote_addr": request.environ.get('REMOTE_ADDR'),
        "x_forwarded_for": request.headers.get('X-Forwarded-For')
    })
