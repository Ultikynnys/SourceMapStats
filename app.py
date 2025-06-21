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
from pythonvalve.valve.source import master_server
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
    "servertimeout":      0.5
}

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
def get_chart_data(start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent):
    """Processes the raw CSV data to generate data for the charts."""
    cache_key = (start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent)
    
    cached_result = g_chart_data_cache.get(cache_key)
    if cached_result and (time.time() - cached_result['timestamp']) < CACHE_EXPIRY_SECONDS:
        logging.info("Returning cached chart data.")
        return cached_result['data']

    logging.info("Generating new chart data...")

    try:
        df = pd.read_csv(RAW_FILE, names=['ip', 'port', 'map', 'players', 'timestamp', 'country_code', 'snapshot_id'])
    except FileNotFoundError:
        logging.error(f"Data file not found: {RAW_FILE}")
        return {'labels': [], 'datasets': [], 'total_player_count': 0, 'total_server_count': 0, 'snapshot_count': 0, 'map_ranking': []}

    # --- Data Cleaning and Normalization ---
    df['players'] = pd.to_numeric(df['players'], errors='coerce').fillna(0).astype(int)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format=ReaderTimeFormat, errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)

    max_date_in_data = df['timestamp'].max()
    start_date = pd.to_datetime(start_date_str)

    if pd.isna(max_date_in_data) or (max_date_in_data - start_date).days > 30:
        if not pd.isna(max_date_in_data):
             logging.warning(f"Stale start_date '{start_date.date()}' detected. Defaulting to last {days_to_show} days from max date '{max_date_in_data.date()}'.")
        start_date = (max_date_in_data if not pd.isna(max_date_in_data) else datetime.now()) - pd.Timedelta(days=days_to_show)

    end_date = start_date + pd.Timedelta(days=int(days_to_show))
    logging.info(f"Filtering data from {start_date.date()} to {end_date.date()}")
    df = df[(df['timestamp'] >= start_date) & (df['timestamp'] < end_date)].copy()

    if df.empty:
        logging.warning("No data available for the selected date range.")
        return {'labels': [], 'datasets': [], 'total_player_count': 0, 'total_server_count': 0, 'snapshot_count': 0, 'map_ranking': []}

    # --- Data Aggregation ---
    df['date'] = df['timestamp'].dt.date
    daily_player_sum = df.groupby(['date', 'map'])['players'].sum().reset_index()
    daily_snapshot_count = df.groupby(['date', 'map'])['snapshot_id'].nunique().reset_index()
    daily_snapshot_count.rename(columns={'snapshot_id': 'unique_snapshots'}, inplace=True)

    merged_df = pd.merge(daily_player_sum, daily_snapshot_count, on=['date', 'map'])
    merged_df['avg_players'] = merged_df['players'] / merged_df['unique_snapshots']

    # --- Chart Dataset Preparation ---
    all_maps = merged_df['map'].unique()
    top_maps = merged_df.groupby('map')['avg_players'].mean().nlargest(maps_to_show).index

    datasets = []
    for map_name in top_maps:
        map_data = merged_df[merged_df['map'] == map_name]
        datasets.append({
            'label': map_name,
            'data': list(map_data.set_index('date')['avg_players'].reindex(pd.date_range(start=start_date.date(), end=end_date.date() - pd.Timedelta(days=1)), fill_value=0)),
            'backgroundColor': f'rgba({(hash(map_name) & 0xFF)}, {(hash(map_name) >> 8) & 0xFF}, {(hash(map_name) >> 16) & 0xFF}, {color_intensity})',
            'borderColor': f'rgba({(hash(map_name) & 0xFF)}, {(hash(map_name) >> 8) & 0xFF}, {(hash(map_name) >> 16) & 0xFF}, 1)',
            'borderWidth': 1
        })

    other_maps_df = merged_df[~merged_df['map'].isin(top_maps)]
    if not other_maps_df.empty:
        other_data = other_maps_df.groupby('date')['avg_players'].sum().reindex(pd.date_range(start=start_date.date(), end=end_date.date() - pd.Timedelta(days=1)), fill_value=0)
        datasets.append({
            'label': 'Other',
            'data': list(other_data),
            'backgroundColor': f'rgba(128, 128, 128, {color_intensity})',
            'borderColor': 'rgba(128, 128, 128, 1)',
            'borderWidth': 1
        })

    # --- Final KPIs and Rankings ---
    total_player_count = int(df['players'].sum())
    total_server_count = len(df.groupby(['ip', 'port']))
    snapshot_count = df['snapshot_id'].nunique()

    map_ranking = df.groupby('map')['players'].sum().sort_values(ascending=False).reset_index()
    map_ranking['rank'] = map_ranking['players'].rank(method='dense', ascending=False).astype(int)

    result = {
        'labels': [d.strftime('%Y-%m-%d') for d in pd.date_range(start=start_date.date(), end=end_date.date() - pd.Timedelta(days=1))],
        'datasets': datasets,
        'total_player_count': total_player_count,
        'total_server_count': total_server_count,
        'snapshot_count': snapshot_count,
        'map_ranking': map_ranking.to_dict('records')
    }

    g_chart_data_cache[cache_key] = {'timestamp': time.time(), 'data': result}
    return result

# --- Backend Scanner ---

config = {
    "servertimeout": 0.4,
}

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
    master_server = ('hl2master.steampowered.com', 27011)
    all_servers = []
    try:
        all_servers = list(a2s.find_server(master_server, filter='\gamedir\tf'))
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
