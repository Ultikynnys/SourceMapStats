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

# ─── error counter ───────────────────────────────────────────────────────────
scan_error_count = 0

# ─── data cache ───────────────────────────────────────────────────────────────
g_raw_data_cache = []
g_cache_file_mtime = 0
g_cache_lock = threading.Lock()
g_chart_data_cache = {}

# ─── flask app ────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static')

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
    "RuntimeMinutes":     60,
    "RunForever":         True,
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
    return f"#{r:02x}{g:02x}{b:02x}"

def is_ip_address(ip: str) -> bool:
    return re.match(r"^(([0-1]?\d?\d|2[0-4]\d|25[0-5])\.){3}"
                    r"([0-1]?\d?\d|2[0-4]\d|25[0-5])$", ip) is not None

# ─── per-IP adaptive timeouts ────────────────────────────────────────────────
IP_TIMEOUTS_FILE = os.path.join(BASE_DIR, 'ip_timeouts.json')
try:
    with open(IP_TIMEOUTS_FILE, 'r') as f:
        ip_timeouts = {ip: float(t) for ip, t in _json.load(f).items()}
except FileNotFoundError:
    ip_timeouts = {}

def save_timeouts():
    with open(IP_TIMEOUTS_FILE, 'w') as f:
        _json.dump(ip_timeouts, f)

# ─── data-processing functions ────────────────────────────────────────────────
def RawData():
    logging.info("--- Starting RawData ---")
    global g_raw_data_cache, g_cache_file_mtime, g_cache_lock

    path = os.path.join(BASE_DIR, config["Filename"])
    if not os.path.exists(path):
        return []

    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return []

    with g_cache_lock:
        if mtime == g_cache_file_mtime:
            return g_raw_data_cache

        out = []
        try:
            with open(path, newline='', encoding='utf-8') as f:
                for row in csv.reader(f):
                    if len(row) >= 7 and row[0] not in config["IpBlackList"]:
                        out.append(row)
        except Exception as e:
            print(f"Error reading or processing {path}: {e}")
            return []

        g_raw_data_cache = out
        g_cache_file_mtime = mtime
        return out

def get_date_range():
    """Reads the CSV and returns the min and max dates found."""
    path = os.path.join(BASE_DIR, config["Filename"])
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return {"min_date": None, "max_date": None}

    try:
        # Use pandas to efficiently read only the timestamp column
        df = pd.read_csv(path, usecols=[4], header=None, names=['timestamp'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=ReaderTimeFormat, errors='coerce')
        df.dropna(inplace=True)

        if df.empty:
            return {"min_date": None, "max_date": None}

        min_date = df['timestamp'].min().strftime('%Y-%m-%d')
        max_date = df['timestamp'].max().strftime('%Y-%m-%d')
        
        logging.info(f"Calculated date range: {min_date} to {max_date}")
        return {"min_date": min_date, "max_date": max_date}
    except Exception as e:
        logging.error(f"Error getting date range: {e}")
        return {"min_date": None, "max_date": None}

def get_chart_data(start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent):
    logging.info("--- Starting get_chart_data ---")
    global g_chart_data_cache

    # Ensure parameters have correct types for cache key and logic
    days_to_show = int(days_to_show)
    maps_to_show = int(maps_to_show)
    percision = int(percision)
    color_intensity = int(color_intensity)
    bias_exponent = float(bias_exponent)
    # Use tuple for hashability
    only_maps_containing_tuple = tuple(sorted(only_maps_containing))

    file_mod_time = os.path.getmtime(config["Filename"]) if os.path.exists(config["Filename"]) else 0
    cache_key = (file_mod_time, start_date_str, days_to_show, only_maps_containing_tuple, maps_to_show, percision, bias_exponent)

    # Return cached result if available
    if cache_key in g_chart_data_cache:
        logging.info("Returning cached chart data.")
        return g_chart_data_cache[cache_key]
    
    logging.info(f"Cache miss. Processing data for key: {cache_key}")

    # Get raw data and convert to DataFrame
    rows = RawData()
    if not rows:
        logging.warning("RawData is empty. Returning empty chart data.")
        result = {"labels": [], "datasets": [], "ranking": [], "averageDailyPlayerCount": 0, "totalPlayers": 0, "dailyTotals": [], "snapshotCounts": [], "shownMapsCount": 0, "totalFilteredMaps": 0}
        g_chart_data_cache[cache_key] = result
        return result

    df = pd.DataFrame(rows, columns=['ip', 'port', 'map', 'players', 'timestamp', 'unknown', 'snapshot_id'])
    logging.info(f"Loaded {len(df)} rows into DataFrame from CSV.")
    df['players'] = pd.to_numeric(df['players'], errors='coerce').fillna(0).astype(int)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format=ReaderTimeFormat, errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)
    logging.info(f"DataFrame size after cleaning and type conversion: {len(df)}")

    if df.empty:
        logging.warning("DataFrame is empty after initial processing. Aborting.")
        return {
            'labels': [], 'datasets': [], 'ranking': [], 'averageDailyPlayerCount': 0,
            'daily_avg_players': {'data': [], 'labels': []}, 'snapshots_per_day': {'data': [], 'labels': []}
        }

    # Filter by date range
    logging.info(f"Filtering by date. Start_Date: {start_date_str}, DaysToShow: {days_to_show}")
    start_date = pd.to_datetime(start_date_str)
    end_date = start_date + pd.Timedelta(days=int(days_to_show))
    logging.info(f"Calculated date range for filtering: {start_date.date()} to {end_date.date()}")
    logging.info(f"Filtering from {start_date} to {end_date}")
    df = df[(df['timestamp'] >= start_date) & (df['timestamp'] < end_date)].copy()
    logging.info(f"DataFrame size after date filtering: {len(df)}")

    if df.empty:
        logging.warning("DataFrame is empty after date filtering. Returning empty dataset.")
        return {
            "labels": [],
            "datasets": [],
            "ranking": [],
            "averageDailyPlayerCount": 0,
            "totalPlayers": 0,
            "dailyTotals": [],
            "snapshotCounts": [],
            "shownMapsCount": 0,
            "totalFilteredMaps": 0
        }

    # This is a bug fix. The original code referenced a non-existent 'WordFilter' config key.
    # We will now use the 'map' column directly for filtering and aggregation.
    df['map_clean'] = df['map'].str.strip()
    
    # Filter by map names if specified
    if only_maps_containing:
        logging.info(f"Filtering with map name patterns: {only_maps_containing}")
        # Create a regex pattern from the list of substrings.
        # This will match any map_clean that contains any of the provided substrings.
        # The pattern is a simple OR of all substrings.
        pattern = '|'.join(re.escape(s) for s in only_maps_containing)
        logging.info(f"Using regex pattern for map filtering: '{pattern}'")
        logging.info(f"Sample map_clean values before filtering: {df['map_clean'].unique()[:5]}")
        df = df[df['map_clean'].str.contains(pattern, case=False, na=False)].copy()
    logging.info(f"DataFrame size after map name filtering: {len(df)}")

    if df.empty:
        logging.warning("DataFrame is empty after all filters. Returning empty chart data.")
        return {
            "labels": [],
            "datasets": [],
            "ranking": [],
            "averageDailyPlayerCount": 0,
            "totalPlayers": 0,
            "dailyTotals": [],
            "snapshotCounts": [],
            "shownMapsCount": 0,
            "totalFilteredMaps": 0
        }

    logging.info("DataFrame has data. Proceeding with aggregations.")
    # --- Aggregations (Definitive Final) ---
    df['day'] = df['timestamp'].dt.normalize()

    # 1. Calculate the average player count per map for each day. Used for ranking and the percentage chart's numerators.
    daily_avg_map_players = df.groupby(['day', 'map_clean'])['players'].mean().unstack(fill_value=0)

    # 2. Calculate the denominator for the percentage chart. This is the sum of per-map daily averages.
    # This calculation method is what guarantees the percentages for each day will sum to 100.
    daily_total_for_chart_denominator = daily_avg_map_players.sum(axis=1)

    # 3. Calculate the intuitive daily total for the raw player count chart.
    # This is the average of the total players summed across all maps for each snapshot within a day.
    # This gives a clear "average total players online" metric for each day.
    intuitive_daily_totals = df.groupby(['day', 'snapshot_id'])['players'].sum(numeric_only=True).groupby('day').mean()

    # 4. Calculate snapshot counts per day for weighting the ranking table.
    snapshot_counts = df.groupby('day')['snapshot_id'].nunique()

    # 5. Calculate the main KPI: the average players per snapshot over the entire filtered period.
    total_players_sum = df['players'].sum()
    total_snapshots = df['snapshot_id'].nunique()
    avg_daily_kpi = float(round(total_players_sum / total_snapshots, percision)) if total_snapshots > 0 else 0.0

    # --- Weighted Averages & Ranking ---
    # Use the denominator's index for labels, as it covers all days with activity.
    labels_dt = sorted(daily_total_for_chart_denominator.index.tolist())
    raw_weights = {d: (snapshot_counts.get(d, 0) ** bias_exponent) for d in labels_dt}
    total_weight = sum(raw_weights.values()) or 1

    # 6. Calculate the overall weighted average player count for *each map* for the ranking table.
    daily_avg_dict = daily_avg_map_players.to_dict('index')
    map_weighted = {}
    for d in labels_dt:
        w = raw_weights.get(d, 0) / total_weight
        if d in daily_avg_dict:
            for m, day_avg in daily_avg_dict[d].items():
                map_weighted[m] = map_weighted.get(m, 0) + day_avg * w

    map_avg = {m: 0.0 if math.isnan(v) else float(round(v, percision)) for m, v in map_weighted.items()}
    top_maps = sorted(map_avg, key=map_avg.get, reverse=True)[:maps_to_show]
    
    total_avg_all = float(sum(map_avg.values())) or 1.0
    ranking = [{"label": m, "avg": map_avg[m], "pop": float(round(map_avg[m] / total_avg_all * 100, percision))} for m in top_maps]

    # --- Chart Datasets ---
    datasets = []
    for idx, m in enumerate(top_maps):
        data = []
        for d in labels_dt:
            daily_avg_for_map = daily_avg_dict.get(d, {}).get(m, 0)
            # Use the chart-specific total as the denominator.
            denominator = daily_total_for_chart_denominator.get(d, 1)
            percent = float(round(daily_avg_for_map / denominator * 100, percision)) if denominator > 0 else 0.0
            data.append(percent)
        datasets.append({"label": m, "data": data, "borderColor": get_color(idx, len(top_maps)+1, color_intensity), "fill": False})

    # "Other maps" calculation also uses the chart-specific total.
    if len(map_avg) > len(top_maps):
        others_data = []
        for d in labels_dt:
            denominator = daily_total_for_chart_denominator.get(d, 1)
            if denominator > 0:
                sum_of_top_maps_avg = sum(daily_avg_dict.get(d, {}).get(m, 0) for m in top_maps)
                rem_avg = max(denominator - sum_of_top_maps_avg, 0)
                others_data.append(float(round(rem_avg / denominator * 100, percision)))
            else:
                others_data.append(0.0)
        datasets.append({"label": "Other maps", "data": others_data, "borderColor": "#888888", "fill": False})

    result = {
        "labels": [d.strftime('%Y-%m-%d') for d in labels_dt],
        "datasets": datasets,
        "ranking": ranking,
        "averageDailyPlayerCount": avg_daily_kpi,
        "totalPlayers": float(round(total_avg_all, percision)),
        "dailyTotals": [float(round(intuitive_daily_totals.get(d, 0), 1)) for d in labels_dt],
        "snapshotCounts": [int(snapshot_counts.get(d, 0)) for d in labels_dt],
        "shownMapsCount": len(top_maps),
        "totalFilteredMaps": len(map_avg)
    }
    
    g_chart_data_cache[cache_key] = result
    logging.info("--- Finished get_chart_data ---")
    return result

# ─── scanning logic (slow-only) ───────────────────────────────────────────────
scanning_stop_event = threading.Event()
scanning_thread = None
scanning_mode = "None"
current_scanned_ip = ""
last_error_message = ""

def IpReader(ip):
    """Query single game server, return CSV row or None,
       with adaptive per-IP timeouts."""
    global last_error_message, scan_error_count

    ip_str = ip[0]
    addr = (ip_str, int(ip[1]))
    timeout = ip_timeouts.get(ip_str, config["servertimeout"])

    try:
        info = a2s.info(addr, timeout=min(timeout, MAX_SINGLE_IP_TIMEOUT))
        players = info.player_count - info.bot_count

        if players < 1:
            # treat “no players” as a successful quick read → shrink timeout
            new_t = max(0.1, timeout - 0.1)
            ip_timeouts[ip_str] = new_t
            save_timeouts()
            return None

        # successful query → shrink timeout
        new_t = max(0.1, timeout - 0.1)
        ip_timeouts[ip_str] = new_t
        save_timeouts()

        row = [
            ip_str, str(ip[1]), info.map_name, str(players),
            datetime.now().strftime(ReaderTimeFormat), "00"
        ]
        return row

    except socket.timeout:
        # real timeout → grow timeout
        new_t = min(2.0, timeout + 0.5)
        ip_timeouts[ip_str] = new_t
        save_timeouts()

        scan_error_count += 1
        last_error_message = f"Timeout after {timeout}s"
        return None

    except Exception as e:
        scan_error_count += 1
        last_error_message = str(e)
        return None

def SlowScan():
    ips = []
    global last_error_message
    try:
        with master_server.MasterServerQuerier(timeout=config["timeout_master"]) as msq:
            ips = list(msq.find(
                gamedir=config["Game"], empty=True,
                secure=True, region=config["regionserver"]
            ))
    except Exception as e:
        last_error_message = f"Master server: {e}"
    return ips

def CSVWriter(rows, rawfile):
    if not rows:
        return

    # Append to the CSV, creating it if it doesn't exist.
    idx = int(time.time())
    with open(rawfile, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow(r + [str(idx)])

def scan_loop():
    rawfile = os.path.join(BASE_DIR, config["Filename"])
    interval = 10 * 60  # 10 minutes

    while not scanning_stop_event.is_set():
        scanning_mode = "Slow"
        scan_error_count = 0

        # perform a slow scan
        rows = IpReaderMulti(SlowScan())
        CSVWriter(rows, rawfile)

        # wait until next scan or stop event
        scanning_stop_event.wait(interval)

    scanning_mode = "None"
    current_scanned_ip = ""

def IpReaderMulti(lst):
    out = []
    total = len(lst)
    for i, ip in enumerate(lst, 1):
        global current_scanned_ip
        current_scanned_ip = f"{i}/{total} {ip[0]}:{ip[1]}"
        row = IpReader(ip)
        if row:
            out.append(row)
    current_scanned_ip = ""
    return out

# Import and register routes from external module
import routes, sys
app.register_blueprint(routes.create_blueprint(sys.modules[__name__]))

# ─── run waitress ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    host = "0.0.0.0" if PUBLIC_MODE else "127.0.0.1"
    port = int(os.getenv("PORT", 5000))
    serve(app, host=host, port=port, threads=8)
