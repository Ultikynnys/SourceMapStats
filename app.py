import sys
import os
import csv
import re
import math
import time
import threading
import json as _json
import socket
from datetime import datetime, timedelta
from functools import wraps
import ast
import pandas as pd
from flask import Flask, jsonify, request, g
from waitress import serve

import logging

# ─── logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ─── add local libs (pythonvalve + a2s) ───────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, "pythonvalve"))
sys.path.insert(0, os.path.join(BASE_DIR, "a2s"))
import a2s
from valve.source.master_server import MasterServerQuerier
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
CACHE_EXPIRY_SECONDS = 300 # Cache chart data for 5 minutes

# ─── API-key support ──────────────────────────────────────────────────────────
keys_file = os.path.join(BASE_DIR, 'config_keys.json')
if not os.path.exists(keys_file):
    raise RuntimeError("Missing config_keys.json!")
with open(keys_file, 'r') as f:
    ACCEPTED_KEYS = set(_json.load(f).get("accepted_keys", []))

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
        mtime = os.path.getmtime(RAW_FILE)
        return datetime.fromtimestamp(mtime).strftime(ReaderTimeFormat)
    except (OSError, FileNotFoundError):
        return None

def get_date_range():
    """Returns the earliest and latest timestamps in the data."""
    try:
        # Only read the timestamp column for efficiency
        df = pd.read_csv(RAW_FILE, usecols=['timestamp'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=ReaderTimeFormat, errors='coerce')
        df.dropna(inplace=True)
        if df.empty:
            return {"min_date": None, "max_date": None}
        min_date = df['timestamp'].min().strftime('%Y-%m-%d')
        max_date = df['timestamp'].max().strftime('%Y-%m-%d')
        return {"min_date": min_date, "max_date": max_date}
    except (FileNotFoundError, pd.errors.EmptyDataError, KeyError, ValueError):
        # Handle cases where file doesn't exist, is empty, or has no timestamp column
        return {"min_date": None, "max_date": None}

def get_chart_data(start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent):
    """Processes the raw CSV data to generate data for the charts."""
    cache_key = (start_date_str, days_to_show, tuple(only_maps_containing), maps_to_show, percision, color_intensity, bias_exponent)

    cached_result = g_chart_data_cache.get(cache_key)
    if cached_result and (time.time() - cached_result['timestamp']) < CACHE_EXPIRY_SECONDS:
        logging.info("Returning cached chart data.")
        return cached_result['data']

    logging.info("Generating new chart data...")

    try:
        df = pd.read_csv(RAW_FILE, names=['ip', 'port', 'map', 'players', 'timestamp', 'country_code', 'snapshot_id'])
    except FileNotFoundError:
        logging.error(f"Data file not found: {RAW_FILE}")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    # --- Data Cleaning and Normalization ---
    df['players'] = pd.to_numeric(df['players'], errors='coerce').fillna(0).astype(int)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format=ReaderTimeFormat, errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)

    # --- Date Range Filtering ---
    max_date_in_data = df['timestamp'].max()
    start_date = pd.to_datetime(start_date_str) if start_date_str else (max_date_in_data - pd.Timedelta(days=days_to_show))

    if pd.isna(max_date_in_data) or (max_date_in_data.date() < start_date.date()):
        start_date = (max_date_in_data if not pd.isna(max_date_in_data) else datetime.now()) - pd.Timedelta(days=days_to_show)
        logging.warning(f"Start date is out of range. Defaulting to last {days_to_show} days from max date: {start_date.date()}")

    end_date = start_date + pd.Timedelta(days=int(days_to_show))
    date_range = pd.date_range(start=start_date.date(), end=end_date.date() - pd.Timedelta(days=1))
    
    df = df[(df['timestamp'] >= start_date) & (df['timestamp'] < end_date)].copy()
    
    if only_maps_containing:
        pattern = '|'.join(only_maps_containing)
        df = df[df['map'].str.contains(pattern, na=False)]

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
    
    # --- Chart Dataset Preparation ---
    top_maps = merged_df.groupby('map')['avg_players'].mean().nlargest(maps_to_show).index
    datasets = []
    for map_name in top_maps:
        map_data = merged_df[merged_df['map'] == map_name]
        datasets.append({
            'label': map_name,
            'data': list(map_data.set_index('date')['avg_players'].reindex(date_range.date, fill_value=0)),
            'backgroundColor': get_color(len(datasets), len(top_maps), color_intensity),
            'borderColor': get_color(len(datasets), len(top_maps), color_intensity).replace('rgb', 'rgba').replace(')', ', 1)'),
            'borderWidth': 1
        })

    other_maps_df = merged_df[~merged_df['map'].isin(top_maps)]
    if not other_maps_df.empty:
        other_data = other_maps_df.groupby('date')['avg_players'].sum().reindex(date_range.date, fill_value=0)
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

        # Calculate "Other" category
        # Note: other_maps_df is defined earlier
        if not other_maps_df.empty:
            other_maps_avg_sum = map_daily_avg[~map_daily_avg.index.isin(top_maps)].sum()
            if other_maps_avg_sum > 0:
                other_pop = round((other_maps_avg_sum / total_daily_avg_sum) * 100, 2)
                ranking.append({'label': 'Other', 'pop': other_pop})

    result = {
        'labels': [d.strftime('%Y-%m-%d') for d in date_range],
        'datasets': datasets,
        'dailyTotals': daily_totals,
        'snapshotCounts': snapshot_counts,
        'ranking': ranking,
        'shownMapsCount': len(top_maps),
    }

    g_chart_data_cache[cache_key] = {'timestamp': time.time(), 'data': result}
    return result

# --- Backend Scanner ---

ip_timeouts = {}
MAX_SINGLE_IP_TIMEOUT = 3.0

def IpReader(ip):
    """Query single game server, return CSV row or None."""
    ip_str, port = ip
    timeout = ip_timeouts.get(ip_str, config["servertimeout"])

    try:
        info = a2s.info(ip, timeout=timeout)
        map_name = re.sub(r'[\r\n\x00-\x1F\x7F-\x9F]', '', info.map_name).strip()
        timestamp = datetime.now().strftime(ReaderTimeFormat)
        ip_timeouts[ip_str] = max(0.1, timeout * 0.9)
        return [ip_str, str(port), map_name, str(info.player_count), timestamp]

    except (socket.timeout, ConnectionResetError):
        logging.warning(f"Timeout/Reset for {ip_str}:{port}")
        ip_timeouts[ip_str] = min(MAX_SINGLE_IP_TIMEOUT, timeout * 1.5)
        return None
    except Exception as e:
        logging.warning(f"Error for {ip_str}:{port}: {str(e)}")
        return None

def IpReaderMulti(lst, snapshot_id):
    """Process a list of IPs and append a snapshot_id."""
    out = []
    total = len(lst)
    for i, ip in enumerate(lst, 1):
        # logging.info(f"Querying IP {i}/{total}: {ip[0]}:{ip[1]}") # This is too verbose
        row = IpReader(ip)
        if row:
            out.append(row + [snapshot_id])
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
    """Appends rows to the master CSV file in the correct 7-column format."""
    if not rows:
        return

    with open(RAW_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in rows:
            # row from IpReaderMulti is [ip, port, map, players, timestamp, snapshot_id]
            country_code = get_country(row[0])
            row.insert(5, country_code) # Final format: [ip, port, map, players, timestamp, country_code, snapshot_id]
            writer.writerow(row)

def get_server_list():
    """Fetches the server list from the Valve master server."""
    all_servers = []
    try:
        with MasterServerQuerier() as msq:
            all_servers = list(msq.find(gamedir="tf"))
        logging.info(f"Fetched {len(all_servers)} servers from master server.")
    except Exception as e:
        logging.error(f"Failed to fetch server list from master server: {e}")
    return all_servers

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
