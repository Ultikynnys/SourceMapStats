import sys
import os
from functools import wraps
import csv
import re
import math
import time
import threading
from datetime import datetime, timedelta, timezone
import numpy as np

from flask import Flask, jsonify, request
import json
import socket
import requests
from waitress import serve

dirname = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(dirname, "pythonvalve"))
sys.path.insert(0, os.path.join(dirname, "a2s"))

import a2s
from pythonvalve.valve.source import master_server

################################################
# --------------[ Flask Setup ]----------------#
################################################

app = Flask(__name__, static_folder='static')

# ------------------------------------------------
# Load and Validate Accepted API Keys Securely
# ------------------------------------------------

dirname = os.path.dirname(os.path.abspath(__file__))

keys_file = os.path.join(dirname, 'config_keys.json')
if not os.path.exists(keys_file):
    raise RuntimeError("Missing config_keys.json file with accepted API keys!")

with open(keys_file, 'r') as f:
    keys_data = json.load(f)

ACCEPTED_KEYS = set(keys_data.get("accepted_keys", []))

# ------------------------------------------------
# Security Helpers
# ------------------------------------------------

def sanitize_api_key(key):
    """
    Remove any characters that are not alphanumeric, dash, or underscore.
    """
    return re.sub(r'[^a-zA-Z0-9-_]', '', key) if key else key

def is_ip_address(ip):
    """
    Checks if the provided string is a valid IPv4 or IPv6 address.
    """
    pattern = re.compile(
        r"^(([0-1]?\d?\d|2[0-4]\d|25[0-5])(\.|$)){4}|([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
    )
    return pattern.match(ip) is not None

def reject_unknown_keys(allowed_keys, incoming_dict):
    """
    Remove keys from 'incoming_dict' that are not in 'allowed_keys'.
    """
    return {k: v for k, v in incoming_dict.items() if k in allowed_keys}

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        provided_key = request.headers.get('X-API-KEY')
        provided_key = sanitize_api_key(provided_key)
        if provided_key not in ACCEPTED_KEYS:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

################################################
# --------------[ IP Rate Limiting ]-----------#
################################################

REQUESTS_PER_IP = {}  # { ip_string : [list_of_timestamps] }
MAX_REQUESTS = 10
WINDOW_SECONDS = 60

def rate_limiter(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        # 1) Check if a valid API key was provided
        provided_key = request.headers.get('X-API-KEY', '')
        # If it's a valid key, skip rate limiting entirely
        if provided_key in ACCEPTED_KEYS:
            return f(*args, **kwargs)

        # Otherwise, apply IP-based limit
        now = time.time()
        remote_addr = request.remote_addr or 'unknown'

        timestamps = REQUESTS_PER_IP.setdefault(remote_addr, [])
        cutoff = now - WINDOW_SECONDS
        while timestamps and timestamps[0] < cutoff:
            timestamps.pop(0)

        if len(timestamps) >= MAX_REQUESTS:
            next_reset = timestamps[0] + WINDOW_SECONDS
            cooldown = max(0, next_reset - now)

            print(
                f"[RATE-LIMIT] IP {remote_addr} is rate-limited. "
                f"Requests in current window: {len(timestamps)}, "
                f"cooldown: {cooldown:.2f}s left."
            )
            return jsonify({
                "error": "Too many requests, slow down.",
                "cooldown": cooldown
            }), 429

        timestamps.append(now)
        return f(*args, **kwargs)
    return wrapper

################################################
# --------------[ Track Requests ]-------------#
################################################

RECENT_REQUESTS_LOG = {}
MAX_STORED_REQUESTS_PER_IP = 50

@app.before_request
def log_incoming_request():
    """
    Before every request, log the IP, timestamp, method, and path in RECENT_REQUESTS_LOG.
    """
    ip = request.remote_addr or 'unknown'
    req_info = (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                request.method,
                request.path)

    if ip not in RECENT_REQUESTS_LOG:
        RECENT_REQUESTS_LOG[ip] = []
    RECENT_REQUESTS_LOG[ip].append(req_info)

    if len(RECENT_REQUESTS_LOG[ip]) > MAX_STORED_REQUESTS_PER_IP:
        RECENT_REQUESTS_LOG[ip].pop(0)

################################################
# --------------[ Global Config ]--------------#
################################################

config = {
    "Game": "tf",
    "Filename": "output.csv",
    "Filelog": "stats.log",
    "Filenamepng": "output.png",
    "MapsToShow": 15,
    "ColorIntensity": 3,
    "Start_Date": "2001-10-02",
    "End_Date": "2040-10-02",
    "NoFilter": False,
    "VersionFilter": "abcvdf",
    "WordFilter": "final|redux|rc|test|fix|skial|censored|blw|vrs|alpha|beta|fin",
    "OnlyMapsContaining": ["dr_"],
    "IpBlackList": ['94.226.97.69'],
    "OutputDimensions": (12, 6),
    "LabelTransparency": 0.5,
    "Percision": 2,
    "timeout_query": 0.5,
    "timeout_master": 60,
    "regionserver": "all",
    "RuntimeMinutes": 60,
    "RunForever": True,
    "AverageDays": 1,
    "ColorForOtherMaps": (0.5, 0.5, 0.5),
    "XaxisDates": 5,
    "IpcountList": 5,
    "FastWriteDelay": 10
}

if "servertimeout" not in config:
    config["servertimeout"] = config["timeout_query"]

ReaderTimeFormat = '%Y-%m-%d-%H:%M:%S'

scanning_status = "Idle"
scanning_mode = "None"
current_scanned_ip = ""
last_error_message = ""
timeout_error_count = 0
error_threshold = 5

x = y = z = w = 0
internalips = []
averagelist = []
CurrentScanIndex = 0

################################################
# -------------[ Utility Functions ]-----------#
################################################

def arrayrectifier(arrlist):
    largest = max(len(arr) for arr in arrlist) if arrlist else 0
    for arr in arrlist:
        while len(arr) < largest:
            arr.append(0)
    return arrlist

def dictmerger(dictlist):
    merged = {}
    for d in dictlist:
        for key, value in d.items():
            merged[key] = merged.get(key, 0) + value
    return merged

def dictmax(d, amount):
    sorted_items = sorted(d.items(), key=lambda item: item[1], reverse=True)
    return [k for k, v in sorted_items[:amount]]

def dictpadder(d, keylist):
    new_d = d.copy()
    for key in keylist:
        if key not in new_d:
            new_d[key] = 0
    return new_d

def dictlimx(d, keylist):
    return {k: d[k] for k in keylist if k in d}

def weakfiller(d, stringlist):
    newlist = []
    for substring in stringlist:
        if substring == "":
            newlist.extend(d.keys())
        else:
            for key in d.keys():
                if substring.lower() in key.lower():
                    newlist.append(key)
    return list(set(newlist))

def get_color(index, total, intensity):
    angle = index * intensity * 2 * math.pi / total
    r = int((math.sin(angle) + 1) / 2 * 255)
    g = int((math.sin(angle + 2*math.pi/3) + 1) / 2 * 255)
    b = int((math.sin(angle + 4*math.pi/3) + 1) / 2 * 255)
    return f"#{r:02x}{g:02x}{b:02x}"

def suffix_filter(mapname):
    pattern = re.compile(r'(_)?(' + config["WordFilter"] + r')$', re.IGNORECASE)
    filtered = pattern.sub('', mapname)
    return filtered.strip()

################################################
# -------------[ Data Functions ]--------------#
################################################

def RawData():
    data = []
    base_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(base_dir, config["Filename"])
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return data
    with open(file_path, "r", newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 7:
                continue
            if row[0] in config["IpBlackList"]:
                continue
            data.append(row)
    return data

def filter_by_timerange(data):
    filtered = []
    start_date = datetime.strptime(config["Start_Date"], '%Y-%m-%d')
    end_date = datetime.strptime(config["End_Date"], '%Y-%m-%d')
    for row in data:
        try:
            dt = datetime.strptime(row[4], ReaderTimeFormat)
            if start_date <= dt <= end_date:
                filtered.append(row)
        except Exception:
            continue
    return filtered

def timechunker(data):
    if not data:
        return {}
    data_sorted = sorted(data, key=lambda r: datetime.strptime(r[4], ReaderTimeFormat))
    start_date = datetime.strptime(data_sorted[0][4], ReaderTimeFormat).date()
    chunks = {}
    for row in data_sorted:
        try:
            dt = datetime.strptime(row[4], ReaderTimeFormat).date()
        except Exception:
            continue
        delta_days = (dt - start_date).days
        chunk_index = delta_days // config["AverageDays"]
        chunk_start = start_date + timedelta(days=chunk_index * config["AverageDays"])
        chunk_key = chunk_start.strftime("%Y-%m-%d")
        chunks.setdefault(chunk_key, []).append(row)
    return chunks

def duplicate_merger(chunks):
    merged = {}
    for chunk_key, rows in chunks.items():
        map_counts = {}
        scan_indexes = set()
        for row in rows:
            mapname = suffix_filter(row[2])
            try:
                count = int(row[3])
            except:
                count = 0
            scan_index = row[6] if len(row) > 6 else "0"
            scan_indexes.add(scan_index)
            if mapname not in map_counts:
                map_counts[mapname] = 0
            map_counts[mapname] += count

        num_scans = len(scan_indexes) if scan_indexes else 1
        merged[chunk_key] = {
            mapname: (map_counts[mapname] / num_scans) for mapname in map_counts
        }
    return merged

def get_chart_data():
    """
    Retrieves the data for the chart, including labels, datasets,
    average daily count, total players, and other stats.
    """
    raw = RawData()
    filtered = filter_by_timerange(raw)
    if not filtered:
        return {
            "labels": [],
            "datasets": [],
            "averageDailyPlayerCount": 0,
            "totalPlayers": 0,
            "shownMapsCount": 0,
            "totalFilteredMaps": 0
        }

    chunks = timechunker(filtered)
    merged_chunks = duplicate_merger(chunks)

    overall = dictmerger(list(merged_chunks.values()))
    whitelist = weakfiller(overall, config["OnlyMapsContaining"])
    filtered_overall = dictlimx(overall, whitelist)

    # Sort by largest overall sum, then pick top N
    all_filtered_maps_sorted = sorted(filtered_overall.items(), key=lambda item: item[1], reverse=True)
    top_maps = [k for k, _ in all_filtered_maps_sorted[:config["MapsToShow"]]]

    # Prepare labels (the chunk keys = dates) in sorted order
    labels = sorted(merged_chunks.keys())

    # Prepare data sets for the chosen top maps
    datasets = {mapname: [] for mapname in top_maps}
    other_data = []

    for label in labels:
        chunk_data = merged_chunks[label]
        filtered_chunk_data = {m: c for m, c in chunk_data.items() if m in whitelist}
        filtered_chunk_total = sum(filtered_chunk_data.values())
        top_total = 0
        for mapname in top_maps:
            count = filtered_chunk_data.get(mapname, 0)
            datasets[mapname].append(count)
            top_total += count
        # The remainder is for any maps that didn't make the top list
        other_data.append(filtered_chunk_total - top_total)

    # Gather all rows that match the whitelist maps
    filtered_rows_all = [
        row for rows in chunks.values() for row in rows
        if suffix_filter(row[2]) in whitelist
    ]
    total_players_filtered = sum(int(row[3]) for row in filtered_rows_all)
    unique_scans_all = set(row[6] if len(row) > 6 else "0" for row in filtered_rows_all)

    # Average daily players
    if unique_scans_all:
        average_daily = round(total_players_filtered / len(unique_scans_all), config["Percision"])
    else:
        average_daily = 0

    # Summation across all chunks for "totalPlayers"
    total_players = sum(
        sum(int(row[3]) for row in rows if suffix_filter(row[2]) in whitelist)
        for rows in chunks.values()
    )
    if unique_scans_all:
        total_players = round(total_players / len(unique_scans_all), config["Percision"])
    else:
        total_players = 0

    # Build the dataset list with color assignments
    dataset_list = []
    total_top = len(top_maps)
    for idx, mapname in enumerate(top_maps):
        dataset_list.append({
            "label": mapname,
            "data": datasets[mapname],
            "borderColor": get_color(idx, total_top, config["ColorIntensity"]),
            "fill": False
        })

    if other_data and max(other_data) >= 1:
        dataset_list.append({
            "label": "Other Maps",
            "data": other_data,
            "borderColor": "#808080",
            "fill": False
        })

    return {
        "labels": labels,
        "datasets": dataset_list,
        "averageDailyPlayerCount": average_daily,
        "totalPlayers": total_players,
        "shownMapsCount": len(top_maps),
        "totalFilteredMaps": len(filtered_overall)
    }

################################################
# -------------[ Scanning Routines ]-----------#
################################################

def GlobalFlush():
    global x, y, z, w, internalips, averagelist
    internalips = []
    averagelist = []
    x = y = z = w = 0

def IpReader(IP):
    global x, y, z, w, internalips, averagelist, last_error_message, timeout_error_count
    try:
        x += 1
        server_address = (IP[0], int(IP[1]))
        info = a2s.info(server_address, timeout=config["servertimeout"])
        current_players = info.player_count - info.bot_count
        if current_players < 1:
            y += 1
            return None
        row = [IP[0], str(IP[1]), info.map_name, str(current_players)]
        averagelist.append(int(row[3]))
        row.append(datetime.now().strftime('%Y-%m-%d-%H:%M:%S'))
        region = "00"
        try:
            if is_ip_address(IP[0]):
                response = requests.get(f"http://ip-api.com/json/{IP[0]}", timeout=2)
                geo = response.json()
                region = geo.get("countryCode", "00")
        except:
            pass
        row.append(region)
        timeout_error_count = 0
        w += 1
        return row
    except (socket.timeout, Exception) as e:
        y += 1
        last_error_message = f"Error in IpReader for {IP[0]}:{IP[1]} - {e}"
        timeout_error_count += 1
        if timeout_error_count >= error_threshold:
            config["servertimeout"] += 0.5
            timeout_error_count = 0
        return None

def SlowScan():
    ips = []
    servercount = 0
    try:
        with master_server.MasterServerQuerier(timeout=config["timeout_master"]) as msq:
            addresses = msq.find(
                gamedir=config["Game"],
                empty=True,
                secure=True,
                region=config["regionserver"]
            )
            for address in addresses:
                ips.append(address)
                servercount += 1
        print(f"Servers found in MasterServer: {servercount}")
    except Exception as e:
        print("Master Server Timed out!!", e)
    return ips

def GetMaxScanIndex(rawfilename):
    global CurrentScanIndex
    CurrentScanIndex = 0
    try:
        with open(rawfilename, "r", newline='', encoding='utf-8') as filedata:
            csvreader = csv.reader(filedata)
            for row in csvreader:
                if len(row) == 7 and CurrentScanIndex < int(row[6]):
                    CurrentScanIndex = int(row[6])
    except:
        with open(rawfilename, "w", newline='', encoding='utf-8') as filedata:
            csv.writer(filedata)

def CSVWriter(rows, rawfilename):
    global CurrentScanIndex
    if not os.path.exists(rawfilename):
        with open(rawfilename, "w", newline='', encoding='utf-8') as filedata:
            pass

    with open(rawfilename, "a", newline='', encoding='utf-8') as filedata:
        writer = csv.writer(filedata)
        CurrentScanIndex += 1
        for row in rows:
            try:
                writer.writerow(row + [str(CurrentScanIndex)])
            except:
                try:
                    sanitized_map = re.sub('[^a-zA-Z0-9_]+', '', row[2])
                    row[2] = sanitized_map
                    writer.writerow(row + [str(CurrentScanIndex)])
                except:
                    print("Skipping invalid map entry entirely.")

def FastScan(rawfilename, TestIp=[('176.57.188.166', 27015)], Testmode=False):
    iplist = []
    if Testmode:
        iplist = TestIp
    else:
        if not os.path.exists(rawfilename):
            return []
        with open(rawfilename, "r", newline='', encoding='utf-8') as filedata:
            csvreader = csv.reader(filedata)
            for ip in csvreader:
                if len(ip) >= 2:
                    pair = (ip[0], int(ip[1]))
                    if pair not in iplist:
                        iplist.append(pair)
    return iplist

def IpReaderMulti(list_ips):
    ips2 = []
    GlobalFlush()
    global current_scanned_ip
    total_ips = len(list_ips)
    for idx, address in enumerate(list_ips, start=1):
        current_scanned_ip = f"scanning {idx}/{total_ips}: {address[0]}:{address[1]}"
        datastack = IpReader(address)
        if datastack is not None:
            ips2.append(datastack)
    current_scanned_ip = ""
    return ips2

def Iterator(rawfilename, delay=None, FastScansTillSlow=15):
    global scanning_mode
    if delay is None:
        delay = float(config["FastWriteDelay"])

    end_time = None if config["RunForever"] else (time.time() + config["RuntimeMinutes"]*60)
    InternalPoint = FastScansTillSlow

    while not scanning_stop_event.is_set() and (end_time is None or time.time() < end_time):
        if InternalPoint >= FastScansTillSlow:
            scanning_mode = "Slow"
            GetMaxScanIndex(rawfilename)
            print(f"SLOW SEARCH: Master Server every {FastScansTillSlow*delay} minutes (approx).")
            rows = IpReaderMulti(SlowScan())
            if rows:
                CSVWriter(rows, rawfilename)
            InternalPoint = 0
        else:
            scanning_mode = "Fast"
            GetMaxScanIndex(rawfilename)
            start_time = time.time()
            rows = IpReaderMulti(FastScan(rawfilename))
            if rows:
                CSVWriter(rows, rawfilename)
            elapsed = time.time() - start_time
            if elapsed < 10:
                InternalPoint = FastScansTillSlow
                print("Fast scan took less than 10 seconds. Doing slow scan next.")
            else:
                InternalPoint += 1
                print(f"FAST SCAN done. Sleeping for {delay} minutes.")
                for _ in range(int(delay * 60)):
                    if scanning_stop_event.is_set():
                        break
                    time.sleep(1)

    print("Scan complete or forcibly stopped.")

def scan_loop():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    rawfilename = os.path.join(base_dir, config["Filename"])
    GlobalFlush()
    Iterator(rawfilename)

scanning_thread = None
scanning_stop_event = threading.Event()

################################################
# ---------------[ Flask Routes ]--------------#
################################################

@app.route('/api/start_scan', methods=['POST'])
@require_api_key
@rate_limiter
def start_scan():
    global scanning_thread, scanning_stop_event, scanning_status
    if scanning_thread and scanning_thread.is_alive():
        return jsonify({"status": "Scanning already in progress."})
    scanning_status = "Scanning"
    scanning_stop_event.clear()
    scanning_thread = threading.Thread(target=scan_loop, daemon=True)
    scanning_thread.start()
    return jsonify({"status": "Scanning started."})

@app.route('/api/stop_scan', methods=['POST'])
@require_api_key
@rate_limiter
def stop_scan():
    global scanning_thread, scanning_stop_event, scanning_status, scanning_mode, current_scanned_ip
    scanning_stop_event.set()
    if scanning_thread:
        scanning_thread.join(timeout=2)
    scanning_status = "Idle"
    scanning_mode = "None"
    current_scanned_ip = ""
    return jsonify({"status": "Scanning stopped."})

def sanitize_int(value, min_val=0, max_val=999999):
    try:
        val = int(value)
        val = max(min_val, min(val, max_val))
        return val
    except (ValueError, TypeError):
        return None

def sanitize_date(value):
    try:
        dt = datetime.strptime(value, '%Y-%m-%d')
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None

def sanitize_basic_string(value, allow_spaces=False):
    if not value:
        return ""
    if allow_spaces:
        return re.sub(r'[^a-zA-Z0-9_\-, ]', '', value)
    else:
        return re.sub(r'[^a-zA-Z0-9_\-]', '', value)

@app.route('/api/update_params', methods=['POST'])
@require_api_key
@rate_limiter
def update_params():
    allowed_keys = set([
        "MapsToShow", "Percision", "AverageDays", "FastWriteDelay",
        "RuntimeMinutes", "ColorIntensity", "Start_Date", "End_Date",
        "OnlyMapsContaining", "regionserver", "Game", "RunForever"
    ])

    new_params = request.get_json() or {}
    new_params = reject_unknown_keys(allowed_keys, new_params)

    for key, value in new_params.items():
        if key in ["MapsToShow", "Percision", "AverageDays", "FastWriteDelay",
                   "RuntimeMinutes", "ColorIntensity"]:
            sanitized = sanitize_int(value, min_val=0, max_val=10000)
            if sanitized is not None:
                config[key] = sanitized

        elif key in ["Start_Date", "End_Date"]:
            sanitized = sanitize_date(value)
            if sanitized:
                config[key] = sanitized

        elif key == "OnlyMapsContaining":
            if isinstance(value, str):
                sanitized_str = sanitize_basic_string(value, allow_spaces=True)
                split_list = [v.strip() for v in sanitized_str.split(",") if v.strip()]
                if split_list:
                    config[key] = split_list
            elif isinstance(value, list):
                final_list = []
                for item in value:
                    final_list.append(sanitize_basic_string(str(item), allow_spaces=False))
                config[key] = final_list

        elif key == "regionserver":
            sanitized_value = sanitize_basic_string(value, allow_spaces=False)
            config[key] = sanitized_value or "all"

        elif key == "Game":
            # Whitelist check
            sanitized_game = sanitize_basic_string(value, allow_spaces=False).lower()
            valid_games = {"tf", "cstrike", "csgo", "css", "dod", "hl2mp", "left4dead", "left4dead2"}
            if sanitized_game in valid_games:
                config[key] = sanitized_game
            else:
                config[key] = "tf"

        elif key == "RunForever":
            if isinstance(value, bool):
                config[key] = value
            else:
                str_val = str(value).lower()
                config[key] = (str_val == 'true')

    return jsonify({"status": "Parameters updated", "config": config})

@app.route('/api/params', methods=['GET'])
def get_params():
    return jsonify(config)

@app.route('/api/data')
@rate_limiter
def api_data():
    data = get_chart_data()
    return jsonify(data)

@app.route('/api/status')
@rate_limiter
def api_status():
    return jsonify({
        "scanning_status": scanning_status,
        "scanning_mode": scanning_mode,
        "current_scanned_ip": current_scanned_ip,
        "last_error": last_error_message
    })

@app.route('/api/csv_status', methods=['GET'])
@rate_limiter
def csv_status():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(base_dir, config["Filename"])
    file_exists = os.path.exists(file_path)
    file_empty = False
    if file_exists:
        file_empty = (os.path.getsize(file_path) == 0)
    return jsonify({
        "exists": file_exists,
        "empty": file_empty
    })

@app.route('/api/connections', methods=['GET'])
@require_api_key
@rate_limiter
def api_connections():
    unique_ips = list(RECENT_REQUESTS_LOG.keys())
    ip_count = len(unique_ips)
    connections_data = {}
    for ip, entries in RECENT_REQUESTS_LOG.items():
        connections_data[ip] = []
        for entry in entries:
            connections_data[ip].append({
                "time": entry[0],
                "method": entry[1],
                "path": entry[2]
            })
    return jsonify({
        "unique_ip_count": ip_count,
        "details": connections_data
    })

@app.route('/')
def index():
    return app.send_static_file('index.html')

if __name__ == "__main__":
    serve(app, host="127.0.0.1", port=5000)
