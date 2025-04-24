"""
Full back-end for SourceMapStats – Flask + A2S + Chart.js

Key points of this version
──────────────────────────
• Each **scan index** in output.csv is treated as a “snapshot”.
• Snapshots are grouped by calendar **day**.
• For every day we compute the *average* player count per map
  ( = mean of all snapshots from that day ).
• The line-chart shows these **daily averages as % share**.
• The global ranking and “average player count” numbers are based
  on the average of those **daily averages**, not on raw snapshots.
• Any “Days Per Point / AverageDays” option has been removed.
"""

import sys
import os
import csv
import re
import math
import time
import threading
import json
from datetime import datetime
from functools import wraps
import ast, os, csv, time
from flask import Flask, jsonify, request, g
from waitress import serve

# ─── add local libs (pythonvalve + a2s) ───────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, "pythonvalve"))
sys.path.insert(0, os.path.join(BASE_DIR, "a2s"))
import a2s
from pythonvalve.valve.source import master_server

# ─── basic constants ──────────────────────────────────────────────────────────
PUBLIC_MODE           = False           # False → bind 127.0.0.1
MAX_SINGLE_IP_TIMEOUT = 1.0             # hard clamp per server query
ReaderTimeFormat      = '%Y-%m-%d-%H:%M:%S'


# ─── error counter ───────────────────────────────────────────────────────────
scan_error_count = 0

# ─── flask app ────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static')

# ─── config (editable at runtime via /api/update_params) ──────────────────────
config = {
    "Game":               "tf",
    "Filename":           "output.csv",
    "MapsToShow":         15,
    "ColorIntensity":     3,
    "Start_Date":         "2001-10-02",
    "End_Date":           "2040-10-02",
    "WordFilter":         "final|redux|rc|test|fix|skial|censored|blw|vrs|alpha|beta|fin",
    "OnlyMapsContaining": ["dr_"],
    "IpBlackList":        ['94.226.97.69'],
    "Percision":          2,
    "timeout_query":      0.5,
    "timeout_master":     60,
    "regionserver":       "all",
    "FastWriteDelay":     10,      # minutes between fast scans
    "RuntimeMinutes":     60,
    "RunForever":         True,
    "servertimeout":      0.5
}

# ─── API-key support ──────────────────────────────────────────────────────────
keys_file = os.path.join(BASE_DIR, 'config_keys.json')
if not os.path.exists(keys_file):
    raise RuntimeError("Missing config_keys.json!")
with open(keys_file, 'r') as f:
    ACCEPTED_KEYS = set(json.load(f).get("accepted_keys", []))

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

# ─── rate limiting (30 requests per 15 s / IP) ────────────────────────────────
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
def suffix_filter(name: str) -> str:
    return re.sub(r'(_)?(' + config["WordFilter"] + r')$', '', name,
                  flags=re.IGNORECASE).strip()

def get_color(i: int, total: int, intensity: int) -> str:
    ang = i * intensity * 2 * math.pi / max(total, 1)
    r = int((math.sin(ang) + 1) / 2 * 255)
    g = int((math.sin(ang + 2 * math.pi / 3) + 1) / 2 * 255)
    b = int((math.sin(ang + 4 * math.pi / 3) + 1) / 2 * 255)
    return f"#{r:02x}{g:02x}{b:02x}"

def is_ip_address(ip: str) -> bool:
    return re.match(r"^(([0-1]?\d?\d|2[0-4]\d|25[0-5])\.){3}"
                    r"([0-1]?\d?\d|2[0-4]\d|25[0-5])$", ip) is not None

# ─── data-processing functions ────────────────────────────────────────────────
def RawData():
    path = os.path.join(BASE_DIR, config["Filename"])
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []
    out = []
    import ast
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.reader(f):
            # if the row’s first cell is a Python list literal, expand each cell
            if row and row[0].startswith('[') and row[0].endswith(']'):
                for cell in row:
                    try:
                        parsed = ast.literal_eval(cell)
                        # only accept lists of length >=7 and non-blacklisted IPs
                        if isinstance(parsed, list) and len(parsed) >= 7 and parsed[0] not in config["IpBlackList"]:
                            out.append(parsed)
                    except Exception:
                        continue
                continue
            # otherwise, normal filtering
            if len(row) < 7 or row[0] in config["IpBlackList"]:
                continue
            out.append(row)
    return out


def filter_by_timerange(rows):
    s = datetime.strptime(config["Start_Date"], '%Y-%m-%d')
    e = datetime.strptime(config["End_Date"],   '%Y-%m-%d')
    out = []
    for r in rows:
        try:
            dt = datetime.strptime(r[4], ReaderTimeFormat)
            if s <= dt <= e:
                out.append(r)
        except:
            pass
    return out

def group_snapshots(rows):
    """returns list of snapshots sorted by timestamp"""
    snaps = {}
    for r in rows:
        idx = r[6]
        ts = datetime.strptime(r[4], ReaderTimeFormat)
        m = suffix_filter(r[2])
        try:
            cnt = int(r[3])
        except:
            cnt = 0
        s = snaps.setdefault(idx, {"time": ts, "counts": {}})
        s["counts"][m] = s["counts"].get(m, 0) + cnt
    return sorted(snaps.values(), key=lambda x: x["time"])


def daily_averages(snapshots):
    """
    -> per_day_avg: { day: { map: avg_players_per_snapshot } }
    -> per_day_total: { day: avg_total_players_per_snapshot }
    -> snapshot_counts: { day: number_of_snapshots_that_day }
    """
    daily = {}
    for s in snapshots:
        day = s["time"].strftime('%Y-%m-%d')
        daily.setdefault(day, []).append(s["counts"])

    per_day_avg    = {}
    per_day_total  = {}
    snapshot_counts = {}

    for day, counts_list in daily.items():
        n = len(counts_list)
        snapshot_counts[day] = n
        # all maps seen that day
        maps = set().union(*counts_list)

        # avg per‐map that day
        per_day_avg[day] = {
            m: round(
                sum(dct.get(m, 0) for dct in counts_list) / n,
                config["Percision"]
            )
            for m in maps
        }

        # avg total players that day
        per_day_total[day] = round(
            sum(sum(dct.values()) for dct in counts_list) / n,
            config["Percision"]
        )

    return per_day_avg, per_day_total, snapshot_counts


def get_chart_data():
    rows = filter_by_timerange(RawData())
    subs = [s.lower() for s in config["OnlyMapsContaining"]]
    rows = [r for r in rows
            if any(sub in suffix_filter(r[2]).lower() for sub in subs)]
    if not rows:
        return {
            "labels": [], "datasets": [], "ranking": [],
            "averageDailyPlayerCount": 0, "totalPlayers": 0,
            "dailyTotals": [], "snapshotCounts": [],
            "shownMapsCount": 0, "totalFilteredMaps": 0
        }

    # 1) group + per-day averages, totals, and snapshot counts
    snapshots = group_snapshots(rows)
    daily_avg, daily_tot, snapshot_counts = daily_averages(snapshots)

    # 2) sort days
    labels = sorted(daily_avg.keys())

    # 3) compute raw weights (bias toward days with more snapshots)
    #    you can tweak config["BiasExponent"] (default 1) to increase or soften the bias
    exp = config.get("BiasExponent", 1)
    raw_weights = {d: (snapshot_counts[d] ** exp) for d in labels}
    total_weight = sum(raw_weights.values()) or 1

    # 4) weighted overall average daily player count
    weighted_sum_tot = sum(daily_tot[d] * raw_weights[d] for d in labels)
    avg_daily = round(weighted_sum_tot / total_weight, config["Percision"])

    # 5) weighted per-map averages
    map_weighted = {}
    for d in labels:
        w = raw_weights[d] / total_weight
        for m, day_avg in daily_avg[d].items():
            map_weighted[m] = map_weighted.get(m, 0) + day_avg * w
    map_avg = {m: round(v, config["Percision"]) for m, v in map_weighted.items()}

    # 6) pick top N maps
    top_maps = sorted(map_avg, key=lambda m: map_avg[m], reverse=True)[:config["MapsToShow"]]

    # 7) build ranking for those top N
    total_avg_all = sum(map_avg.values()) or 1
    ranking = [
        {
            "label": m,
            "avg": map_avg[m],
            "pop": round(map_avg[m] / total_avg_all * 100, config["Percision"])
        }
        for m in top_maps
    ]

    # 8) build the share‐per‐day datasets for top N
    datasets = []
    for idx, m in enumerate(top_maps):
        pct_data = []
        for d in labels:
            tot = daily_tot[d]
            cnt = daily_avg[d].get(m, 0)
            pct_data.append(
                round(cnt / tot * 100, config["Percision"]) if tot else 0
            )
        datasets.append({
            "label": m,
            "data": pct_data,
            "borderColor": get_color(idx, len(top_maps)+1, config["ColorIntensity"]),
            "fill": False
        })

    # 9) “Other maps” remainder
    if len(map_avg) > len(top_maps):
        others_data = []
        for d in labels:
            tot = daily_tot[d]
            top_sum = sum(daily_avg[d].get(m, 0) for m in top_maps)
            rem = max(tot - top_sum, 0)
            others_data.append(
                round(rem / tot * 100, config["Percision"]) if tot else 0
            )
        datasets.append({
            "label": "Other maps",
            "data": others_data,
            "borderColor": "#888888",
            "fill": False
        })

    # 10) small‐chart data
    daily_totals = [daily_tot[d] for d in labels]
    snapshotCounts_list = [snapshot_counts[d] for d in labels]

    return {
        "labels": labels,
        "datasets": datasets,
        "ranking": ranking,
        "averageDailyPlayerCount": avg_daily,
        "totalPlayers": round(sum(daily_tot.values()), config["Percision"]),
        "dailyTotals": daily_totals,
        "snapshotCounts": snapshotCounts_list,
        "shownMapsCount": len(top_maps),
        "totalFilteredMaps": len(map_avg)
    }


# ─── scanning logic (slow/fast scans) ─────────────────────────────────────────
scanning_stop_event = threading.Event()
scanning_thread = None
scanning_mode = "None"
current_scanned_ip = ""
last_error_message = ""

def IpReader(ip):
    """Query single game server, return CSV row or None"""
    global last_error_message, scan_error_count
    try:
        addr = (ip[0], int(ip[1]))
        info = a2s.info(addr,
            timeout=min(config["servertimeout"], MAX_SINGLE_IP_TIMEOUT)
        )
        players = info.player_count - info.bot_count
        if players < 1:
            return None
        row = [
            ip[0], str(ip[1]), info.map_name, str(players),
            datetime.now().strftime(ReaderTimeFormat), "00"
        ]
        return row
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

def FastScan(rawfile):
    """Read previous CSV output and return a unique list of (ip, port). 
    Expands any Python-list literals into separate rows."""
    ips = []
    import ast

    if os.path.exists(rawfile):
        with open(rawfile, newline='', encoding='utf-8') as f:
            for r in csv.reader(f):
                # Case A: entire row is one or more list-literals
                if r and r[0].startswith('[') and r[0].endswith(']'):
                    for cell in r:
                        try:
                            parsed = ast.literal_eval(cell)
                            # parsed should be a list-like row: [ip, port, ...]
                            if (
                                isinstance(parsed, list) and 
                                len(parsed) >= 2 and 
                                is_ip_address(parsed[0])
                            ):
                                port = int(parsed[1])
                                ips.append((parsed[0], port))
                        except Exception:
                            continue
                    continue

                # Case B: normal, well-formed CSV row
                if len(r) >= 2:
                    ip, port_str = r[0], r[1]
                    if is_ip_address(ip):
                        try:
                            port = int(port_str)
                        except ValueError:
                            continue
                        ips.append((ip, port))

    # preserve order, remove duplicates
    return list(dict.fromkeys(ips))




def normalize_rawfile(path):
    """Flatten any Python-list literals in the CSV into real rows, preserving any trailing cells (e.g. epoch index)."""
    tmp = path + '.tmp'
    with open(path, newline='', encoding='utf-8') as fin, \
         open(tmp, 'w', newline='', encoding='utf-8') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)

        for row in reader:
            parsed_lists = []
            trailing    = []
            for cell in row:
                txt = cell.strip()
                if txt.startswith('[') and txt.endswith(']'):
                    try:
                        lst = ast.literal_eval(txt)
                        if isinstance(lst, list):
                            parsed_lists.append(lst)
                            continue
                    except Exception:
                        pass
                # not a list-literal → keep for trailing (e.g. epoch index)
                trailing.append(cell)

            if parsed_lists:
                # for each parsed list, write it + any trailing cells
                for lst in parsed_lists:
                    writer.writerow(lst + trailing)
            else:
                # normal row, write unchanged
                writer.writerow(row)

    os.replace(tmp, path)


def CSVWriter(rows, rawfile):
    if not rows:
        return

    # 1) normalize existing file so no “[…]” cells remain
    if os.path.exists(rawfile):
        normalize_rawfile(rawfile)
    else:
        open(rawfile, 'w').close()

    # 2) append new rows with fresh epoch index
    idx = int(time.time())
    with open(rawfile, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow(r + [str(idx)])

def scan_loop():
    global scanning_mode, current_scanned_ip, scan_error_count
    rawfile = os.path.join(BASE_DIR, config["Filename"])
    fast_delay = config["FastWriteDelay"] * 60
    next_fast = 0
    fast_count = 0

    while not scanning_stop_event.is_set():
        now = time.time()

        if fast_count >= 15:
            # slow scan
            scanning_mode = "Slow"
            scan_error_count = 0
            rows = IpReaderMulti(SlowScan())
            CSVWriter(rows, rawfile)
            fast_count = 0
            next_fast = now + fast_delay

        elif now >= next_fast:
            # fast scan
            scanning_mode = "Fast"
            scan_error_count = 0
            rows = IpReaderMulti(FastScan(rawfile))
            CSVWriter(rows, rawfile)
            fast_count += 1
            next_fast = now + fast_delay

        else:
            # cooldown
            scanning_mode = "Cooldown"
            scanning_stop_event.wait(next_fast - now)

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

# ─── Flask routes ─────────────────────────────────────────────────────────────
@app.route('/api/validate_key')
@require_api_key
@rate_limiter
def validate_key():
    return jsonify({"valid": True})

@app.route('/api/heartbeat')
def heartbeat():
    ip = request.remote_addr or 'unknown'
    now = time.time()
    lst = REQUESTS_PER_IP.get(ip, [])
    lst = [t for t in lst if t >= now - WINDOW]
    return jsonify({
        "heartbeat": True,
        "requests_left": MAX_REQ - len(lst),
        "ratelimit_reset": int((lst and (WINDOW - (now - lst[0]))) or 0)
    })

@app.route('/api/start_scan', methods=['POST'])
@require_api_key
@rate_limiter
def start_scan():
    global scanning_thread, scanning_stop_event
    if scanning_thread and scanning_thread.is_alive():
        return jsonify({"status": "Scanning already in progress"})
    scanning_stop_event.clear()
    scanning_thread = threading.Thread(target=scan_loop, daemon=True)
    scanning_thread.start()
    return jsonify({"status": "Scanning started"})

@app.route('/api/stop_scan', methods=['POST'])
@require_api_key
@rate_limiter
def stop_scan():
    scanning_stop_event.set()
    return jsonify({"status": "Scanning stop requested"})

@app.route('/api/update_params', methods=['POST'])
@rate_limiter
def update_params():
    allowed = {
        "MapsToShow", "Percision", "Start_Date", "End_Date",
        "OnlyMapsContaining", "FastWriteDelay", "RuntimeMinutes",
        "ColorIntensity", "Game", "regionserver", "RunForever"
    }
    data = {k: v for k, v in (request.get_json() or {}).items() if k in allowed}
    for k, v in data.items():
        if k in {"MapsToShow", "Percision", "FastWriteDelay", "RuntimeMinutes", "ColorIntensity"}:
            try:
                config[k] = int(v)
            except:
                pass
        elif k in {"Start_Date", "End_Date"}:
            try:
                datetime.strptime(v, '%Y-%m-%d')
                config[k] = v
            except:
                pass
        elif k == "OnlyMapsContaining":
            config[k] = [s.strip() for s in (v if isinstance(v, str) else ','.join(v)).split(',') if s.strip()]
        elif k in {"Game", "regionserver"}:
            config[k] = str(v)
        elif k == "RunForever":
            config[k] = bool(v)
    return jsonify({"status": "Parameters updated", "config": config})

@app.route('/api/data')
@rate_limiter
def api_data():
    return jsonify(get_chart_data())

@app.route('/api/status')
@rate_limiter
def api_status():
    return jsonify({
        "scanning_mode":      scanning_mode,
        "current_scanned_ip": current_scanned_ip,
        "last_error":         last_error_message,
        "error_count":        scan_error_count
    })


@app.route('/api/csv_status')
@rate_limiter
def csv_status():
    path = os.path.join(BASE_DIR, config["Filename"])
    return jsonify({
        "exists": os.path.exists(path),
        "empty": (os.path.getsize(path) == 0) if os.path.exists(path) else True
    })

@app.route('/')
def index():
    return app.send_static_file('index.html')

# ─── run waitress ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    host = "0.0.0.0" if PUBLIC_MODE else "127.0.0.1"
    port = int(os.getenv("PORT", 5000))
    serve(app, host=host, port=port, threads=8)
