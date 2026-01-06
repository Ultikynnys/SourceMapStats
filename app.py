import sys
import os
import duckdb
import re
import math
import time
import threading
import json as _json
import socket
from datetime import datetime, timedelta
from functools import wraps
import ast
import mimetypes
import pandas as pd
import requests
from flask import Flask, jsonify, request, g
from waitress import serve
from dotenv import load_dotenv

import logging

# ─── logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure correct MIME types for static assets (Windows can misreport .js)
mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('text/css', '.css')
mimetypes.add_type('image/svg+xml', '.svg')

# ─── add local libs (pythonvalve + a2s) ───────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, "pythonvalve"))
sys.path.insert(0, os.path.join(BASE_DIR, "a2s"))
import a2s
# Note: MasterServerQuerier is no longer used - Valve deprecated hl2master.steampowered.com
# We now use the Steam Web API (IGameServersService/GetServerList) instead
import numpy as np
from flask.json.provider import DefaultJSONProvider


# ─── basic constants ──────────────────────────────────────────────────────────
PUBLIC_MODE           = True           # False → bind 127.0.0.1
MAX_SINGLE_IP_TIMEOUT = 1.0            # hard clamp per server query
ReaderTimeFormat      = "%Y-%m-%d-%H:%M:%S"



# ─── data cache ───────────────────────────────────────────────────────────────
g_cache_file_mtime = 0
g_cache_lock = threading.Lock()
g_chart_data_cache = {}

# ─── flask app ────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static')
# Disable caching of static files to ensure the browser always gets the latest version.
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# --- Security Headers ---
@app.after_request
def add_security_headers(response):
    """Attach a set of security headers to every response."""
    try:
        csp = (
            "default-src 'self'; "
            "script-src 'self' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self' data:; "
            "connect-src 'self' https://cdn.jsdelivr.net; "
            "object-src 'none'; "
            "base-uri 'self'; "
            "frame-ancestors 'none'"
        )
        response.headers.setdefault('Content-Security-Policy', csp)
        response.headers.setdefault('X-Content-Type-Options', 'nosniff')
        response.headers.setdefault('X-Frame-Options', 'DENY')
        response.headers.setdefault('Referrer-Policy', 'no-referrer')
        response.headers.setdefault('Permissions-Policy', 'geolocation=(), microphone=(), camera=()')
        # Only set HSTS on secure requests
        if request.is_secure:
            response.headers.setdefault('Strict-Transport-Security', 'max-age=63072000; includeSubDomains; preload')
    except Exception:
        pass
    return response

@app.route('/favicon.ico')
def favicon():
    return '', 204

# Custom JSON provider to handle numpy types
class NumpyJSONProvider(DefaultJSONProvider):
    def default(self, o):
        if isinstance(o, (np.integer, np.floating, np.bool_)):
            return o.item()
        elif isinstance(o, np.ndarray):
            return o.tolist()
        return super().default(o)

app.json = NumpyJSONProvider(app)

# ─── config (editable at runtime via /api/update_params) ──────────────────────
config = {
    "Game":               "tf",
    "Filename":           "output.csv",
    "DBFilename":         "sourcemapstats.duckdb",
    "MapsToShow":         15,
    "ColorIntensity":     3,
    "Start_Date":         "2001-10-02",
    "DaysToShow":         7,
    "WordFilter":         "final|redux|rc|test|fix|skial|censored|blw|vrs|alpha|beta|fin",
    "OnlyMapsContaining": ["dr_"],
    "IpBlackList":        ['94.226.97.69'],
    "Percision":          2,
    "timeout_query":      0.5,
    "timeout_master":     60,
    "regionserver":       "all",
    "servertimeout":      0.4
}

# --- Constants derived from config ---
RAW_FILE = os.path.join(BASE_DIR, config["Filename"])
DB_FILE = os.path.join(BASE_DIR, config.get("DBFilename", "sourcemapstats.duckdb"))
CACHE_EXPIRY_SECONDS = 300 # Cache chart data for 5 minutes

# --- DuckDB setup ---
def init_db():
    try:
        with duckdb.connect(DB_FILE) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS samples (
                    ip TEXT,
                    port INTEGER,
                    map TEXT,
                    players INTEGER,
                    timestamp TIMESTAMP,
                    country_code TEXT,
                    snapshot_id TEXT
                )
                """
            )
            
            # Server cooldowns table for persistence across restarts
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS server_cooldowns (
                    ip TEXT,
                    port INTEGER,
                    timeout DOUBLE,
                    failures INTEGER,
                    skip_until DOUBLE,
                    updated_at TIMESTAMP,
                    PRIMARY KEY (ip, port)
                )
                """
            )
            
            # One-time migration: import existing CSV if DB is empty
            try:
                row = con.execute("SELECT count(*) FROM samples").fetchone()
                has_rows = (row is not None and row[0] and row[0] > 0)
            except Exception:
                has_rows = False

            if (not has_rows) and os.path.exists(RAW_FILE) and os.path.getsize(RAW_FILE) > 0:
                logging.info("Migrating existing CSV data into DuckDB...")
                try:
                    con.execute(
                        """
                        INSERT INTO samples (ip, port, map, players, timestamp, country_code, snapshot_id)
                        SELECT 
                            ip,
                            try_cast(port AS INTEGER) as port,
                            map,
                            try_cast(players AS INTEGER) as players,
                            strptime(timestamp_str, '%Y-%m-%d-%H:%M:%S') as timestamp,
                            country_code,
                            snapshot_id
                        FROM read_csv(
                            ?,
                            columns={'ip':'VARCHAR','port':'VARCHAR','map':'VARCHAR','players':'VARCHAR','timestamp_str':'VARCHAR','country_code':'VARCHAR','snapshot_id':'VARCHAR'},
                            delim=',',
                            header=false,
                            quote='"',
                            escape='"',
                            sample_size=-1,
                            nullstr=['N/A']
                        )
                        """,
                        [RAW_FILE]
                    )
                    logging.info("CSV migration complete.")
                except Exception as mig_e:
                    logging.error(f"CSV migration to DuckDB failed: {mig_e}")
    except Exception as e:
        logging.error(f"Failed to initialize DuckDB: {e}")

def load_cooldowns_from_db():
    """Load server cooldowns from database on startup."""
    cooldowns = {}
    try:
        with duckdb.connect(DB_FILE, read_only=True) as con:
            rows = con.execute(
                "SELECT ip, port, timeout, failures, skip_until FROM server_cooldowns"
            ).fetchall()
            for ip, port, timeout, failures, skip_until in rows:
                cooldowns[(ip, port)] = {
                    'timeout': timeout,
                    'failures': failures,
                    'skip_until': skip_until
                }
            if cooldowns:
                logging.info(f"Loaded {len(cooldowns)} server cooldowns from database")
    except Exception as e:
        logging.debug(f"Could not load cooldowns from DB: {e}")
    return cooldowns

def save_cooldowns_to_db(cooldowns):
    """Save server cooldowns to database."""
    if not cooldowns:
        return
    try:
        with duckdb.connect(DB_FILE) as con:
            # Batch upsert using INSERT OR REPLACE
            now = datetime.now()
            rows = [
                (ip, port, data['timeout'], data['failures'], data['skip_until'], now)
                for (ip, port), data in cooldowns.items()
            ]
            con.executemany(
                """
                INSERT OR REPLACE INTO server_cooldowns (ip, port, timeout, failures, skip_until, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows
            )
    except Exception as e:
        logging.debug(f"Could not save cooldowns to DB: {e}")

init_db()

# ─── API-key support ──────────────────────────────────────────────────────────
# Load environment variables from .env file
load_dotenv(os.path.join(BASE_DIR, '.env'))

# API keys for authentication (comma-separated in .env)
_api_keys_str = os.getenv('API_KEYS', '')
ACCEPTED_KEYS = set(k.strip() for k in _api_keys_str.split(',') if k.strip())

# Steam API key for server list queries
STEAM_API_KEY = os.getenv('STEAM_API_KEY', '')

if not ACCEPTED_KEYS:
    logging.warning("No API_KEYS found in .env file. API authentication will fail.")
if not STEAM_API_KEY:
    logging.warning("No STEAM_API_KEY found in .env file. Server scanning will be disabled.")
    logging.info("Get a free Steam API key at: https://steamcommunity.com/dev/apikey")

def sanitize_api_key(k):
    return re.sub(r'[^a-zA-Z0-9-_]', '', k)

def require_api_key(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        key = sanitize_api_key(request.headers.get('X-API-KEY', ''))
        if key not in ACCEPTED_KEYS:
            return jsonify({"error": "Unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapped

# ─── rate limiting (60 requests per 15 s / IP) ────────────────────────────────
REQUESTS_PER_IP = {}
MAX_REQ = 60
WINDOW = 15

def rate_limiter(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        # Bypass limits for valid API keys
        try:
            key = sanitize_api_key(request.headers.get('X-API-KEY', ''))
            if key and key in ACCEPTED_KEYS:
                r = app.make_response(fn(*args, **kwargs))
                r.headers.update({
                    "X-RateLimit-Limit": 'bypass',
                    "X-RateLimit-Remaining": 'bypass',
                    "X-RateLimit-Reset": '0'
                })
                return r
        except Exception:
            # Fall back to regular limiting if any issue arises
            pass

        ip = request.remote_addr or 'unknown'
        now = time.time()
        lst = REQUESTS_PER_IP.setdefault(ip, [])
        # drop timestamps older than WINDOW
        while lst and lst[0] <= now - WINDOW:
            lst.pop(0)
        if len(lst) >= MAX_REQ:
            retry = int(WINDOW - (now - lst[0]))
            return jsonify({"error":"Too many requests","cooldown":retry}), 429
        lst.append(now)
        g.rate_remaining = MAX_REQ - len(lst)
        g.rate_reset = int(WINDOW - (now - lst[0]))
        r = app.make_response(fn(*args, **kwargs))
        r.headers.update({
            "X-RateLimit-Limit": MAX_REQ,
            "X-RateLimit-Remaining": g.rate_remaining,
            "X-RateLimit-Reset": g.rate_reset
        })
        return r
    return wrapped

# ─── helpers ──────────────────────────────────────────────────────────────────
def get_color(i: int, total: int, intensity: int) -> str:
    ang = i * intensity * 2 * math.pi / max(total, 1)
    r = int((math.sin(ang) + 1) / 2 * 255)
    g = int((math.sin(ang + 2 * math.pi / 3) + 1) / 2 * 255)
    b = int((math.sin(ang + 4 * math.pi / 3) + 1) / 2 * 255)
    return f"rgb({r},{g},{b})"

def get_data_freshness():
    """Returns the timestamp of the last data update."""
    try:
        with duckdb.connect(DB_FILE, read_only=True) as con:
            row = con.execute("SELECT max(timestamp) FROM samples").fetchone()
            latest = row[0] if row else None
            if not latest:
                return None
            if isinstance(latest, str):
                try:
                    latest_dt = datetime.strptime(latest, ReaderTimeFormat)
                except ValueError:
                    latest_dt = datetime.fromisoformat(latest)
            else:
                latest_dt = latest
            return latest_dt.strftime(ReaderTimeFormat)
    except Exception:
        return None

def get_date_range():
    """Returns the earliest and latest timestamps in the data."""
    try:
        with duckdb.connect(DB_FILE, read_only=True) as con:
            row = con.execute("SELECT min(timestamp), max(timestamp) FROM samples").fetchone()
            if not row or row[0] is None or row[1] is None:
                return {"min_date": None, "max_date": None}
            min_dt, max_dt = row[0], row[1]
            if isinstance(min_dt, str):
                try:
                    min_dt = datetime.strptime(min_dt, ReaderTimeFormat)
                except ValueError:
                    min_dt = datetime.fromisoformat(min_dt)
            if isinstance(max_dt, str):
                try:
                    max_dt = datetime.strptime(max_dt, ReaderTimeFormat)
                except ValueError:
                    max_dt = datetime.fromisoformat(max_dt)
            return {"min_date": min_dt.strftime('%Y-%m-%d'), "max_date": max_dt.strftime('%Y-%m-%d')}
    except Exception:
        return {"min_date": None, "max_date": None}

def get_chart_data(start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent, top_servers=10, append_maps_containing=None, server_filter=None):
    """Processes the data from DuckDB to generate data for the charts."""
    cache_key = (
        start_date_str,
        days_to_show,
        tuple(only_maps_containing),
        maps_to_show,
        percision,
        color_intensity,
        bias_exponent,
        top_servers,
        tuple(append_maps_containing or []),
        server_filter or 'ALL'
    )

    cached_result = g_chart_data_cache.get(cache_key)
    if cached_result and (time.time() - cached_result['timestamp']) < CACHE_EXPIRY_SECONDS:
        logging.info("Returning cached chart data.")
        return cached_result['data']

    logging.info("Generating new chart data...")

    try:
        with duckdb.connect(DB_FILE, read_only=True) as con:
            # Determine max date in DB
            row = con.execute("SELECT max(timestamp) FROM samples").fetchone()
            max_date_in_data = row[0] if row else None
            if not max_date_in_data:
                return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

            # Normalize to pandas Timestamp
            if isinstance(max_date_in_data, str):
                try:
                    max_date_in_data = datetime.strptime(max_date_in_data, ReaderTimeFormat)
                except ValueError:
                    max_date_in_data = datetime.fromisoformat(max_date_in_data)

            start_date = pd.to_datetime(start_date_str) if start_date_str else (pd.Timestamp(max_date_in_data) - pd.Timedelta(days=days_to_show))

            if pd.isna(pd.Timestamp(max_date_in_data)) or (pd.Timestamp(max_date_in_data).date() < pd.Timestamp(start_date).date()):
                start_date = (pd.Timestamp(max_date_in_data) if not pd.isna(pd.Timestamp(max_date_in_data)) else pd.Timestamp.now()) - pd.Timedelta(days=days_to_show)
                logging.warning(f"Start date is out of range. Defaulting to last {days_to_show} days from max date: {start_date.date()}")

            end_date = pd.Timestamp(start_date) + pd.Timedelta(days=int(days_to_show))
            date_range = pd.date_range(start=pd.Timestamp(start_date).date(), end=(pd.Timestamp(end_date).date() - pd.Timedelta(days=1)))

            # Fetch the filtered window from DuckDB
            df_window = con.execute(
                """
                SELECT ip, port, map, players, timestamp, country_code, snapshot_id
                FROM samples
                WHERE timestamp >= ? AND timestamp < ?
                """,
                [pd.Timestamp(start_date).to_pydatetime(), pd.Timestamp(end_date).to_pydatetime()]
            ).df()

    except Exception as e:
        logging.error(f"Failed to load data from DuckDB: {e}")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    # --- Data Cleaning and Normalization ---
    if df_window.empty:
        logging.warning("No data available for the selected parameters.")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    # Work on a filtered copy based on server_filter, but keep df_window for global ranking
    df = df_window.copy()
    df['players'] = pd.to_numeric(df['players'], errors='coerce').fillna(0).astype(int)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)

    # Apply server filter if provided (format: "IP:PORT")
    if server_filter and isinstance(server_filter, str) and server_filter.upper() != 'ALL':
        try:
            ip_str, port_str = server_filter.split(':', 1)
            ip_str = ip_str.strip()
            port_val = int(port_str.strip())
            if ip_str and port_val >= 0:
                df = df[(df['ip'] == ip_str) & (df['port'] == port_val)]
        except Exception:
            # ignore invalid filter
            pass

    # --- Optional map filter ---
    if only_maps_containing:
        try:
            # Escape user-provided tokens to avoid regex DoS/unexpected patterns
            safe_tokens = [re.escape(s)[:50] for s in only_maps_containing if isinstance(s, str) and s]
            if safe_tokens:
                pattern = '|'.join(safe_tokens)
                df = df[df['map'].str.contains(pattern, na=False, regex=True)]
        except re.error:
            # If the regex fails for any reason, skip filtering rather than erroring
            pass

    if df.empty:
        logging.warning("No data available for the selected parameters.")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    # --- Data Aggregation ---
    df['date'] = df['timestamp'].dt.date
    daily_player_sum = df.groupby(['date', 'map'])['players'].sum().reset_index()
    daily_snapshot_count = df.groupby(['date', 'map'])['snapshot_id'].nunique().reset_index()
    daily_snapshot_count.rename(columns={'snapshot_id': 'unique_snapshots'}, inplace=True)

    merged_df = pd.merge(daily_player_sum, daily_snapshot_count, on=['date', 'map'])
    merged_df['avg_players'] = (merged_df['players'] / merged_df['unique_snapshots']).round(percision)
    
    # Calculate daily total for percentage calculation
    daily_total_avg_players = merged_df.groupby('date')['avg_players'].transform('sum')
    merged_df['player_percentage'] = (merged_df['avg_players'] / daily_total_avg_players.replace(0, 1) * 100).fillna(0)

    # --- Chart Dataset Preparation ---
    top_maps = merged_df.groupby('map')['avg_players'].mean().nlargest(maps_to_show).index
    datasets = []
    for map_name in top_maps:
        map_data = merged_df[merged_df['map'] == map_name]
        datasets.append({
            'label': map_name,
            'data': list(map_data.set_index('date')['player_percentage'].reindex(date_range.date, fill_value=0)),
            'backgroundColor': get_color(len(datasets), len(top_maps), color_intensity),
            'borderColor': get_color(len(datasets), len(top_maps), color_intensity).replace('rgb', 'rgba').replace(')', ', 1)'),
            'borderWidth': 1
        })

    # Optionally append additional map series by substring match (not part of top_maps)
    appended_map_names = []
    if append_maps_containing:
        try:
            safe_tokens = [re.escape(s)[:50] for s in append_maps_containing if isinstance(s, str) and s]
            if safe_tokens:
                pattern = '|'.join(safe_tokens)
                matched = merged_df[merged_df['map'].str.contains(pattern, na=False, regex=True)]['map'].unique().tolist()
                # exclude maps already in top_maps
                appended_map_names = [m for m in matched if m not in set(top_maps)]
                # Sort appended by average players (desc) to keep order meaningful
                if appended_map_names:
                    avg_map = merged_df.groupby('map')['avg_players'].mean()
                    appended_map_names.sort(key=lambda m: float(avg_map.get(m, 0)), reverse=True)
        except re.error:
            appended_map_names = []

    for map_name in appended_map_names:
        map_data = merged_df[merged_df['map'] == map_name]
        datasets.append({
            'label': map_name,
            'data': list(map_data.set_index('date')['player_percentage'].reindex(date_range.date, fill_value=0)),
            'backgroundColor': get_color(len(datasets), max(1, len(top_maps) + len(appended_map_names)), color_intensity),
            'borderColor': get_color(len(datasets), max(1, len(top_maps) + len(appended_map_names)), color_intensity).replace('rgb', 'rgba').replace(')', ', 1)'),
            'borderWidth': 1
        })

    other_exclude = set(top_maps).union(set(appended_map_names))
    other_maps_df = merged_df[~merged_df['map'].isin(other_exclude)]
    if not other_maps_df.empty:
        other_data = other_maps_df.groupby('date')['player_percentage'].sum().reindex(date_range.date, fill_value=0)
        datasets.append({
            'label': 'Other',
            'data': list(other_data),
            'backgroundColor': 'rgba(128, 128, 128, 0.5)',
            'borderColor': 'rgba(128, 128, 128, 1)',
            'borderWidth': 1
        })

    # --- Final KPIs and Rankings ---
    # For 'Total Daily Players' chart, calculate the average players across all filtered maps per day.
    total_daily_players = df.groupby('date')['players'].sum()
    total_daily_snapshots = df.groupby('date')['snapshot_id'].nunique()
    # To avoid division by zero if a day has no snapshots
    daily_totals_df = (total_daily_players / total_daily_snapshots).fillna(0)
    daily_totals = daily_totals_df.reindex(date_range.date, fill_value=0).round(percision).tolist()
    
    daily_snapshots_df = df.groupby('date')['snapshot_id'].nunique()
    snapshot_counts = daily_snapshots_df.reindex(date_range.date, fill_value=0).tolist()

    # --- Per-server contributions for Total Players chart ---
    # Contribution per server per day is defined as: sum(players for that server on the day) / total snapshots that day
    # This sums to the overall average players per day (daily_totals).
    try:
        srv_sum = df.groupby(['date', 'ip', 'port'])['players'].sum().reset_index()
        srv = srv_sum.merge(
            daily_snapshots_df.rename('snapshots'), left_on='date', right_index=True, how='left'
        )
        srv['snapshots'] = srv['snapshots'].replace(0, np.nan)
        srv['avg_contrib'] = (srv['players'] / srv['snapshots']).fillna(0)
        srv['server'] = srv['ip'] + ':' + srv['port'].astype(str)

        # Pivot to dates x servers matrix and align to the full date_range
        pivot = srv.pivot_table(index='date', columns='server', values='avg_contrib', aggfunc='sum')
        pivot = pivot.reindex(date_range.date, fill_value=0)

        # Select top N servers by mean contribution across the window
        top_n = min(int(top_servers or 10), pivot.shape[1])
        if top_n > 0:
            means = pivot.mean(axis=0).sort_values(ascending=False)
            top_servers = list(means.head(top_n).index)
            # Build server ranking using the same top_n and include 'Other' if applicable
            server_ranking = [{ 'label': srv, 'pop': round(float(means[srv]), 2) } for srv in top_servers]
            if pivot.shape[1] > top_n:
                other_val = float(means.iloc[top_n:].sum())
                if not np.isnan(other_val) and other_val > 0:
                    server_ranking.append({ 'label': 'Other', 'pop': round(other_val, 2) })
        else:
            top_servers = []
            server_ranking = []

        total_players_server_datasets = []
        for idx, server in enumerate(top_servers):
            series = pivot[server] if server in pivot.columns else pd.Series([0]*len(pivot), index=pivot.index)
            # sanitize NaN -> 0
            series = series.fillna(0)
            total_players_server_datasets.append({
                'label': server,
                'data': list(series.round(percision).astype(float).values),
                'backgroundColor': get_color(idx, max(1, len(top_servers)), color_intensity).replace('rgb', 'rgba').replace(')', ', 0.5)'),
                'borderColor': get_color(idx, max(1, len(top_servers)), color_intensity),
                'fill': True,
                'stack': 'servers',
            })

        if pivot.shape[1] > len(top_servers):
            other_series = pivot.drop(columns=top_servers, errors='ignore').sum(axis=1)
            other_series = other_series.fillna(0)
            total_players_server_datasets.append({
                'label': 'Other',
                'data': list(other_series.round(percision).astype(float).values),
                'backgroundColor': 'rgba(128,128,128,0.4)',
                'borderColor': 'rgba(128,128,128,1)',
                'fill': True,
                'stack': 'servers',
            })
    except Exception as e:
        logging.debug(f"Failed to compute per-server contributions: {e}")
        total_players_server_datasets = []

    # --- Ranking Calculation (based on average daily players) ---
    map_daily_avg = merged_df.groupby('map')['avg_players'].mean()
    total_daily_avg_sum = map_daily_avg.sum()
    ranking = []

    if total_daily_avg_sum > 0:
        # Get averages for the maps that are in the top_maps list (which is used for the chart)
        top_maps_avg = map_daily_avg[map_daily_avg.index.isin(top_maps)].sort_values(ascending=False)

        # Create ranking for top maps
        ranking_df = (top_maps_avg / total_daily_avg_sum * 100).round(2).reset_index(name='pop')
        ranking_df.rename(columns={'map': 'label'}, inplace=True)
        ranking = ranking_df.to_dict('records')

        # Append additional matched maps (if any)
        if appended_map_names:
            app_maps_avg = map_daily_avg[map_daily_avg.index.isin(appended_map_names)].sort_values(ascending=False)
            app_rank_df = (app_maps_avg / total_daily_avg_sum * 100).round(2).reset_index(name='pop')
            app_rank_df.rename(columns={'map': 'label'}, inplace=True)
            ranking += app_rank_df.to_dict('records')

        # Calculate "Other" category
        # Note: other_maps_df is defined earlier
        if not other_maps_df.empty:
            other_maps_avg_sum = map_daily_avg[~map_daily_avg.index.isin(set(top_maps).union(set(appended_map_names)))].sum()
            if other_maps_avg_sum > 0:
                other_pop = round((other_maps_avg_sum / total_daily_avg_sum) * 100, 2)
                ranking.append({'label': 'Other', 'pop': other_pop})

    # Compute a global server ranking (without server_filter) for dropdown population
    try:
        dfw = df_window.copy()
        dfw['timestamp'] = pd.to_datetime(dfw['timestamp'], errors='coerce')
        dfw.dropna(subset=['timestamp'], inplace=True)
        dfw['date'] = dfw['timestamp'].dt.date
        daily_snapshots_w = dfw.groupby('date')['snapshot_id'].nunique()
        srv_sum_w = dfw.groupby(['date', 'ip', 'port'])['players'].sum().reset_index()
        srv_w = srv_sum_w.merge(
            daily_snapshots_w.rename('snapshots'), left_on='date', right_index=True, how='left'
        )
        srv_w['snapshots'] = srv_w['snapshots'].replace(0, np.nan)
        srv_w['avg_contrib'] = (srv_w['players'] / srv_w['snapshots']).fillna(0)
        srv_w['server'] = srv_w['ip'] + ':' + srv_w['port'].astype(str)
        pivot_w = srv_w.pivot_table(index='date', columns='server', values='avg_contrib', aggfunc='sum')
        means_w = pivot_w.mean(axis=0).sort_values(ascending=False) if pivot_w.shape[1] > 0 else pd.Series([], dtype=float)
        top_n_w = min(10, len(means_w))
        global_server_ranking = [{ 'label': srv, 'pop': round(float(means_w[srv]), 2) } for srv in list(means_w.head(top_n_w).index)]
        if len(means_w) > top_n_w:
            other_val_w = float(means_w.iloc[top_n_w:].sum())
            if not np.isnan(other_val_w) and other_val_w > 0:
                global_server_ranking.append({ 'label': 'Other', 'pop': round(other_val_w, 2) })
    except Exception as e:
        logging.debug(f"Failed to compute global server ranking: {e}")
        global_server_ranking = []

    # sanitize NaNs in datasets
    def _sanitize_dataset_list(ds_list):
        out = []
        for ds in ds_list:
            s = dict(ds)
            if isinstance(s.get('data'), list):
                s['data'] = [0 if (isinstance(v, float) and (np.isnan(v))) else (float(v) if isinstance(v, (np.floating, np.integer)) else v) for v in s['data']]
            out.append(s)
        return out

    # If not computed above, attempt a fallback (top 20)
    if 'server_ranking' not in locals():
        try:
            srv_means = pivot.mean(axis=0).sort_values(ascending=False) if 'pivot' in locals() else pd.Series([], dtype=float)
            server_ranking = [{ 'label': srv, 'pop': round(float(val), 2) } for srv, val in srv_means.head(20).items()]
        except Exception:
            server_ranking = []

    result = {
        'labels': [d.strftime('%Y-%m-%d') for d in date_range],
        'datasets': _sanitize_dataset_list(datasets),
        'dailyTotals': [0 if (isinstance(v, float) and np.isnan(v)) else float(v) for v in daily_totals],
        'snapshotCounts': [int(v) for v in snapshot_counts],
        'ranking': ranking,
        'shownMapsCount': len(top_maps),
        'totalPlayersServerDatasets': _sanitize_dataset_list(total_players_server_datasets),
        'serverRanking': server_ranking,
        'globalServerRanking': global_server_ranking,
        'appendedMapsCount': len(appended_map_names),
    }

    g_chart_data_cache[cache_key] = {'timestamp': time.time(), 'data': result}
    return result

# Track timeout and failure count per server (ip:port)
# Load from database on startup, save after each scan cycle
server_cooldowns = load_cooldowns_from_db()
MAX_SINGLE_IP_TIMEOUT = 60.0  # Max timeout after repeated failures
BASE_SKIP_DURATION = 120  # First skip is 2 minutes, then doubles: 4min, 8min, 16min, etc.
MAX_SKIP_DURATION = 3600  # Cap at 1 hour

def is_valid_public_ip(ip_str):
    """Check if an IP address is a valid public IP (not link-local, private, etc.)."""
    try:
        parts = [int(p) for p in ip_str.split('.')]
        if len(parts) != 4:
            return False
        # Filter out link-local (169.254.x.x)
        if parts[0] == 169 and parts[1] == 254:
            return False
        # Filter out localhost
        if parts[0] == 127:
            return False
        # Filter out 0.0.0.0
        if all(p == 0 for p in parts):
            return False
        return True
    except:
        return False

def IpReader(ip):
    """Query single game server, return CSV row or None."""
    ip_str, port = ip
    server_key = (ip_str, port)
    now = time.time()
    
    # Get or initialize cooldown info for this server
    cooldown = server_cooldowns.get(server_key, {
        'timeout': config["servertimeout"],
        'failures': 0,
        'skip_until': 0
    })
    
    # Skip if in cooldown period
    if now < cooldown['skip_until']:
        return None
    
    timeout = cooldown['timeout']

    try:
        info = a2s.info(ip, timeout=timeout)
        map_name = re.sub(r'[\r\n\x00-\x1F\x7F-\x9F]', '', info.map_name).strip()
        timestamp = datetime.now().strftime(ReaderTimeFormat)
        
        # Log successful connection
        logging.info(f"OK {ip_str}:{port} | {info.player_count} players | {map_name}")
        
        # Success! Reduce timeout and reset failure count
        server_cooldowns[server_key] = {
            'timeout': max(0.1, timeout * 0.9),
            'failures': 0,
            'skip_until': 0
        }
        return [ip_str, str(port), map_name, str(info.player_count), timestamp]

    except (socket.timeout, ConnectionResetError):
        failures = cooldown['failures'] + 1
        new_timeout = min(MAX_SINGLE_IP_TIMEOUT, timeout * 2)  # Double timeout on failure
        
        # Exponential backoff: 2min, 4min, 8min, 16min, etc.
        skip_duration = min(MAX_SKIP_DURATION, BASE_SKIP_DURATION * (2 ** (failures - 1)))
        skip_until = now + skip_duration
        logging.debug(f"Timeout for {ip_str}:{port} (failure #{failures}, skip for {skip_duration}s)")
        
        server_cooldowns[server_key] = {
            'timeout': new_timeout,
            'failures': failures,
            'skip_until': skip_until
        }
        return None
        
    except Exception as e:
        failures = cooldown['failures'] + 1
        new_timeout = min(MAX_SINGLE_IP_TIMEOUT, timeout * 2)
        
        # Exponential backoff: 2min, 4min, 8min, 16min, etc.
        skip_duration = min(MAX_SKIP_DURATION, BASE_SKIP_DURATION * (2 ** (failures - 1)))
        skip_until = now + skip_duration
        
        logging.debug(f"Error for {ip_str}:{port}: {str(e)}")
        
        server_cooldowns[server_key] = {
            'timeout': new_timeout,
            'failures': failures,
            'skip_until': skip_until
        }
        return None

def IpReaderMulti(lst, snapshot_id):
    """Process a list of IPs and append a snapshot_id."""
    out = []
    skipped = 0
    
    # Filter out invalid IPs first
    valid_servers = [(ip, port) for ip, port in lst if is_valid_public_ip(ip)]
    filtered_count = len(lst) - len(valid_servers)
    if filtered_count > 0:
        logging.info(f"Filtered out {filtered_count} invalid/link-local addresses")
    
    total = len(valid_servers)
    for i, ip in enumerate(valid_servers, 1):
        row = IpReader(ip)
        if row:
            out.append(row + [snapshot_id])
        elif server_cooldowns.get((ip[0], ip[1]), {}).get('skip_until', 0) > time.time():
            skipped += 1
    
    if skipped > 0:
        logging.info(f"Skipped {skipped} servers in cooldown")
    
    return out

# --- GeoIP ---
GEOIP_DB_PATH = os.path.join(BASE_DIR, 'GeoLite2-Country.mmdb')
geoip_reader = None
if os.path.exists(GEOIP_DB_PATH):
    try:
        import geoip2.database
        from geoip2.errors import AddressNotFoundError
        geoip_reader = geoip2.database.Reader(GEOIP_DB_PATH)
    except ImportError:
        logging.warning("`geoip2` library not found. To enable country lookups, run `pip install geoip2`.")
    except Exception as e:
        logging.error(f"Failed to load GeoIP database: {e}")
else:
    logging.warning(f"GeoIP database not found at '{GEOIP_DB_PATH}'. Country lookups will be disabled.")

def get_country(ip: str) -> str:
    """Looks up the country code for a given IP address."""
    if not geoip_reader:
        return "N/A"
    try:
        response = geoip_reader.country(ip)
        return response.country.iso_code or "N/A"
    except AddressNotFoundError:
        return "N/A" # IP not in database
    except Exception as e:
        logging.debug(f"Could not get country for IP {ip}: {e}")
        return "N/A"

def write_to_csv(rows):
    """Inserts rows into DuckDB 'samples' table (legacy name kept for compatibility)."""
    if not rows:
        return

    prepared_rows = []
    for row in rows:
        try:
            ip = row[0]
            port = int(row[1]) if row[1] is not None else 0
            map_name = row[2]
            players = int(row[3]) if row[3] is not None else 0
            ts_raw = row[4]
            try:
                ts = datetime.strptime(ts_raw, ReaderTimeFormat)
            except ValueError:
                ts = datetime.fromisoformat(ts_raw)
            snapshot_id = row[5] if len(row) > 5 else None
            country_code = get_country(ip)
            prepared_rows.append((ip, port, map_name, players, ts, country_code, snapshot_id))
        except Exception as e:
            logging.debug(f"Skipping row due to parse error: {row} ({e})")

    if not prepared_rows:
        return

    try:
        with duckdb.connect(DB_FILE) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS samples (
                    ip TEXT,
                    port INTEGER,
                    map TEXT,
                    players INTEGER,
                    timestamp TIMESTAMP,
                    country_code TEXT,
                    snapshot_id TEXT
                )
                """
            )
            con.executemany(
                "INSERT INTO samples (ip, port, map, players, timestamp, country_code, snapshot_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                prepared_rows
            )
    except Exception as e:
        logging.error(f"Failed to write to DuckDB: {e}")

def get_server_list():
    """Fetches the server list from the Steam Web API.
    
    Note: The old Valve master server (hl2master.steampowered.com) was deprecated.
    This uses the Steam Web API IGameServersService/GetServerList endpoint instead.
    Requires a Steam Web API key in config_keys.json.
    
    The API has a limit of ~10,000 servers per request. To get more servers,
    we make multiple requests with different region filters and deduplicate.
    """
    all_servers = set()  # Use set to auto-deduplicate
    
    # Use Steam API key from environment
    if not STEAM_API_KEY:
        logging.error("STEAM_API_KEY not found in .env file. Server scanning disabled.")
        logging.info("Get a free Steam API key at: https://steamcommunity.com/dev/apikey")
        return []
    
    try:
        # Map game shortname to app ID
        game_appids = {
            "tf": 440,      # Team Fortress 2
            "csgo": 730,    # CS:GO
            "cs2": 730,     # CS2
            "cstrike": 10,  # Counter-Strike 1.6
            "dod": 30,      # Day of Defeat
            "hl2dm": 320,   # Half-Life 2: Deathmatch
            "l4d": 500,     # Left 4 Dead
            "l4d2": 550,    # Left 4 Dead 2
        }
        
        game = config.get("Game", "tf")
        appid = game_appids.get(game, 440)  # Default to TF2
        
        # Steam Web API endpoint for game servers
        url = "https://api.steampowered.com/IGameServersService/GetServerList/v1/"
        
        # Query by different regions to bypass the ~10k limit per request
        # Region codes: https://developer.valvesoftware.com/wiki/Master_Server_Query_Protocol
        regions = [
            ("us", "\\region\\0"),   # US East
            ("usw", "\\region\\1"),  # US West  
            ("sa", "\\region\\2"),   # South America
            ("eu", "\\region\\3"),   # Europe
            ("asia", "\\region\\4"), # Asia
            ("au", "\\region\\5"),   # Australia
            ("me", "\\region\\6"),   # Middle East
            ("af", "\\region\\7"),   # Africa
            ("world", ""),           # Unfiltered (catches any missed)
        ]
        
        for region_name, region_filter in regions:
            try:
                # Use both appid AND gamedir filters for reliable game filtering
                filter_str = f"\\appid\\{appid}\\gamedir\\{game}{region_filter}"
                params = {
                    "key": STEAM_API_KEY,
                    "filter": filter_str,
                    "limit": 20000  # Request max allowed
                }
                
                response = requests.get(url, params=params, timeout=config.get("timeout_master", 60))
                response.raise_for_status()
                
                data = response.json()
                servers = data.get("response", {}).get("servers", [])
                
                region_count = 0
                for server in servers:
                    # Validate that server is for the correct game
                    server_appid = server.get("appid", 0)
                    server_gamedir = server.get("gamedir", "")
                    
                    # Only add if it matches our game (double-check the API filter worked)
                    if server_appid != appid and server_gamedir.lower() != game.lower():
                        continue
                    
                    addr = server.get("addr", "")
                    if ":" in addr:
                        ip, port = addr.rsplit(":", 1)
                        # Skip invalid/link-local IPs at the source
                        if not is_valid_public_ip(ip):
                            continue
                        try:
                            all_servers.add((ip, int(port)))
                            region_count += 1
                        except ValueError:
                            continue
                
                logging.debug(f"Region {region_name}: found {region_count} {game} servers")
                
            except requests.exceptions.Timeout:
                logging.warning(f"Steam API request timed out for region {region_name}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to fetch servers for region {region_name}: {e}")
        
        logging.info(f"Fetched {len(all_servers)} unique servers from Steam Web API (across all regions).")
        
    except Exception as e:
        logging.error(f"Failed to fetch server list from Steam Web API: {e}")
    
    return list(all_servers)

def scan_loop():
    """The main loop for continuously scanning servers."""
    logging.info("--- Starting scan_loop ---")
    while True:
        snapshot_id = datetime.now().strftime('%Y%m%d%H%M%S')
        logging.info(f"Starting new scan cycle with snapshot_id: {snapshot_id}")
        
        server_list = get_server_list()
        if not server_list:
            logging.warning("Server list is empty. Skipping this scan cycle.")
            time.sleep(60)
            continue

        results = IpReaderMulti(server_list, snapshot_id)
        write_to_csv(results)
        
        # Persist cooldowns to database
        save_cooldowns_to_db(server_cooldowns)
        
        logging.info(f"Scan cycle complete. Wrote {len(results)} rows. Waiting for next cycle.")
        time.sleep(300)

# --- Main Execution ---
import routes, sys
app.register_blueprint(routes.create_blueprint(sys.modules[__name__]))

if __name__ == '__main__':
    # Start the scanning loop in a background thread
    import threading
    scan_thread = threading.Thread(target=scan_loop, daemon=True)
    scan_thread.start()

    host = "0.0.0.0" if PUBLIC_MODE else "127.0.0.1"
    port = int(os.getenv("PORT", 5000))
    serve(app, host=host, port=port, threads=8)
