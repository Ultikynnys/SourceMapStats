import os
import time
import math
import logging
import duckdb
from functools import wraps
from flask import request, jsonify, g, abort
from dotenv import load_dotenv

# Load environment variables
from config import BASE_DIR

# ─── Admin IP Whitelist ───────────────────────────────────────────────────────
_admin_ips_str = os.getenv('ADMIN_IPS', '')
ADMIN_IPS = set(ip.strip() for ip in _admin_ips_str.split(',') if ip.strip())

if ADMIN_IPS:
    logging.info(f"Admin panel enabled for IPs: {ADMIN_IPS}")
else:
    logging.info("Admin panel disabled (no ADMIN_IPS configured)")

def is_admin_ip(ip: str) -> bool:
    """Check if the given IP is in the admin whitelist."""
    if not ADMIN_IPS:
        return False
    # Also accept IPv6 localhost if IPv4 localhost is whitelisted
    if ip == '::1' and '127.0.0.1' in ADMIN_IPS:
        return True
    return ip in ADMIN_IPS

def admin_only(fn):
    """Decorator that restricts access to admin-whitelisted IPs only."""
    @wraps(fn)
    def wrapped(*args, **kwargs):
        client_ip = request.remote_addr or 'unknown'
        if not is_admin_ip(client_ip):
            abort(404)  # Return 404 to hide admin endpoints from non-admins
        return fn(*args, **kwargs)
    return wrapped

# ─── GeoIP ────────────────────────────────────────────────────────────────────
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

def sanitize_server_name(name: str) -> str:
    """Removes block characters and other noise from server names."""
    if not name:
        return ""
    import re
    # Remove block elements (U+2580 - U+259F)
    # Also remove generic control characters
    # Case specifically for '█' (U+2588) as requested
    
    # Remove all characters in the Block Elements unicode block
    name = re.sub(r'[\u2580-\u259F]', '', name)
    
    # Remove common control characters but keep text
    name = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', name)
    
    # Trim whitespace
    return name.strip()

# ─── IP Validation ────────────────────────────────────────────────────────────
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

# ─── Request Tracking (for Admin Panel) ───────────────────────────────────────
from datetime import datetime, timedelta

ADMIN_DB_FILE = os.path.join(BASE_DIR, "admin_stats.duckdb")

def init_admin_db():
    try:
        with duckdb.connect(ADMIN_DB_FILE) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS request_log (
                    id INTEGER PRIMARY KEY,
                    timestamp TIMESTAMP,
                    ip TEXT,
                    endpoint TEXT,
                    full_path TEXT
                );
                CREATE SEQUENCE IF NOT EXISTS seq_req_id START 1;
            """)
            
            # Auto-repair sequence on startup (prod safety)
            # Find max ID and ensure sequence is ahead of it
            try:
                max_id = con.execute("SELECT max(id) FROM request_log").fetchone()[0] or 0
                con.execute(f"DROP SEQUENCE IF EXISTS seq_req_id")
                con.execute(f"CREATE SEQUENCE seq_req_id START {max_id + 1}")
                logging.info(f"Admin DB sequence reset to {max_id + 1}")
            except Exception as e:
                logging.error(f"Failed to reset/rebuild admin DB: {e}")
    except Exception as e:
        logging.error(f"Failed to init admin DB: {e}")
    
    # Cleanup and Rebuild on boot - MUST be outside the with block
    # so the connection is closed before rebuild tries to ATTACH the file
    try:
        rebuild_admin_db(days_to_keep=30)
    except Exception as e:
        logging.error(f"Failed to rebuild admin DB on init: {e}")

def rebuild_admin_db(days_to_keep=30):
    """Rebuilds the admin DB to enforce vacuuming and minimal file size."""
    try:
        logging.info("Starting admin DB rebuild/compaction...")
        
        # Temp file path
        base, ext = os.path.splitext(ADMIN_DB_FILE)
        temp_file = f"{base}_new{ext}"
        
        if os.path.exists(temp_file):
            os.remove(temp_file)
            
        cutoff = datetime.now() - timedelta(days=days_to_keep)
        
        # 1. Open new DB and create schema
        with duckdb.connect(temp_file) as con_new:
            con_new.execute("""
                CREATE TABLE request_log (
                    id INTEGER PRIMARY KEY,
                    timestamp TIMESTAMP,
                    ip TEXT,
                    endpoint TEXT,
                    full_path TEXT
                );
                CREATE SEQUENCE seq_req_id START 1;
            """)
            
            # 2. Attach old DB and copy valid data
            con_new.execute(f"ATTACH '{ADMIN_DB_FILE}' AS old_db")
            
            # Copy data older than cutoff
            con_new.execute("""
                INSERT INTO request_log 
                SELECT * FROM old_db.request_log 
                WHERE timestamp >= ?
            """, [cutoff])
            
            copied_count = con_new.execute("SELECT count(*) FROM request_log").fetchone()[0]
            
            # Reset sequence
            max_id = con_new.execute("SELECT max(id) FROM request_log").fetchone()[0] or 0
            con_new.execute(f"DROP SEQUENCE IF EXISTS seq_req_id")
            con_new.execute(f"CREATE SEQUENCE seq_req_id START {max_id + 1}")
            
            con_new.execute("DETACH old_db")
            
        # 3. Swap files
        # Small race condition possible here if a write happens exactly now, 
        # but admin stats are tolerant of minor loss (and scanning is single threaded usually)
        import shutil
        shutil.move(temp_file, ADMIN_DB_FILE)
        logging.info(f"Admin DB rebuild complete. Retained {copied_count} rows.")
        
    except Exception as e:
        logging.error(f"Failed to rebuild admin DB: {e}")

# Initialize on import
init_admin_db()

def track_request(ip: str, endpoint: str):
    """Track a request for admin statistics."""
    try:
        try:
            full_path = request.full_path if request.full_path else request.path
            if full_path.endswith('?'):
                full_path = full_path[:-1]
        except:
            full_path = endpoint

        now = datetime.now()
        with duckdb.connect(ADMIN_DB_FILE) as con:
            con.execute(
                "INSERT INTO request_log (id, timestamp, ip, endpoint, full_path) VALUES (nextval('seq_req_id'), ?, ?, ?, ?)",
                [now, ip, endpoint, full_path]
            )
    except Exception as e:
        logging.error(f"Failed to track request: {e}")

def get_request_stats(page=1, limit=50, date_filter=None):
    """Get request statistics for the admin panel with pagination (by IP) and date filter."""
    try:
        page = max(1, int(page))
        limit = max(10, min(100, int(limit)))
        offset = (page - 1) * limit
        
        today = datetime.now().strftime('%Y-%m-%d')
        target_date = date_filter if date_filter else today
        
        with duckdb.connect(ADMIN_DB_FILE, read_only=True) as con:
            # 1. Total Unique IPs (for pagination)
            total_ips = con.execute(
                "SELECT count(DISTINCT ip) FROM request_log WHERE strftime('%Y-%m-%d', timestamp) = ?",
                [target_date]
            ).fetchone()[0]
            
            # 2. Paginated IPs (sorted by specific request count desc)
            ip_rows = con.execute(
                """
                SELECT ip, count(*) as req_count 
                FROM request_log 
                WHERE strftime('%Y-%m-%d', timestamp) = ?
                GROUP BY ip
                ORDER BY req_count DESC
                LIMIT ? OFFSET ?
                """,
                [target_date, limit, offset]
            ).fetchall()
            
            # 3. For each IP, fetch detailed stats (endpoints and recent logs)
            ip_breakdown = []
            
            for row in ip_rows:
                ip = row[0]
                total_requests = row[1]
                
                # Fetch recent logs for this IP (limit 20 for preview)
                logs_rows = con.execute(
                    """
                    SELECT endpoint, full_path, timestamp 
                    FROM request_log 
                    WHERE ip = ? AND strftime('%Y-%m-%d', timestamp) = ?
                    ORDER BY timestamp DESC
                    LIMIT 20
                    """,
                    [ip, target_date]
                ).fetchall()
                
                # Calculate endpoint distribution
                endpoint_counts = {}
                # We can do a quick sub-query or aggregate in python. 
                # Since we limited logs to 20, the distribution might be inaccurate if we only used those.
                # Let's do a proper aggregate query for this IP
                ep_rows = con.execute(
                    """
                    SELECT endpoint, count(*) 
                    FROM request_log 
                    WHERE ip = ? AND strftime('%Y-%m-%d', timestamp) = ?
                    GROUP BY endpoint
                    """,
                    [ip, target_date]
                ).fetchall()
                for ep_r in ep_rows:
                    endpoint_counts[ep_r[0]] = ep_r[1]

                recent_requests = []
                for lr in logs_rows:
                    recent_requests.append({
                        'endpoint': lr[0],
                        'full_path': lr[1],
                        'timestamp': lr[2].strftime('%H:%M:%S')
                    })
                
                ip_breakdown.append({
                    'ip': ip,
                    'total_requests': total_requests,
                    'endpoints': endpoint_counts,
                    'recent_requests': recent_requests
                })
            
            # Total stats for header
            total_requests_today = con.execute(
                 "SELECT count(*) FROM request_log WHERE strftime('%Y-%m-%d', timestamp) = ?",
                 [target_date]
            ).fetchone()[0]
            
            return {
                'date': target_date,
                'total_requests': total_requests_today,
                'unique_ips_today': total_ips,
                'page': page,
                'limit': limit,
                'total_pages': math.ceil(total_ips / limit) if total_ips > 0 else 1,
                'ip_breakdown': ip_breakdown # Use the old key name to match potentially existing frontend code structure logic
            }

    except Exception as e:
        logging.error(f"Failed to get request stats: {e}")
        return {'total_requests': 0, 'ip_breakdown': []}

    except Exception as e:
        logging.error(f"Failed to get request stats: {e}")
        return {'total_requests': 0, 'logs': []}

last_admin_cleanup = 0
ADMIN_CLEANUP_INTERVAL = 3600 # 1 hour

def cleanup_old_stats(days_to_keep=30):
    """Remove stats older than N days and vacuum the DB."""
    global last_admin_cleanup
    now = time.time()
    
    # Only run once per hour
    if now - last_admin_cleanup < ADMIN_CLEANUP_INTERVAL:
        return

    # Use rebuild strategy instead of in-place vacuum for max effectiveness
    rebuild_admin_db(days_to_keep=days_to_keep)
    last_admin_cleanup = now

# ─── Rate Limiting ────────────────────────────────────────────────────────────
REQUESTS_PER_IP = {}
MAX_REQ = 60
WINDOW = 15
CLEANUP_INTERVAL = 60
last_cleanup = time.time()

def rate_limiter(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        global last_cleanup
        
        # Periodic cleanup of old entries to prevent memory leak
        now = time.time()
        if now - last_cleanup > CLEANUP_INTERVAL:
            keys_to_delete = []
            for k, lst in REQUESTS_PER_IP.items():
                # Remove timestamps older than WINDOW
                valid_timestamps = [t for t in lst if t > now - WINDOW]
                if not valid_timestamps:
                    keys_to_delete.append(k)
                else:
                    REQUESTS_PER_IP[k] = valid_timestamps
            
            for k in keys_to_delete:
                del REQUESTS_PER_IP[k]
            last_cleanup = now

        ip = request.remote_addr or 'unknown'
        endpoint = request.endpoint or request.path
        
        # Track request for admin statistics
        track_request(ip, endpoint)
        
        # Also cleanup old stats periodically
        cleanup_old_stats()
        
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
        r = fn(*args, **kwargs)
        
        # Ensure it's a response object before checking headers. 
        # But fn might return tuple (json, 429). Flask handles tuple returns automatically, 
        # but here we need to attach headers.
        # We can use current_app.make_response if we had it, or assume `fn` returns a response.
        # Actually routes usually return tuples.
        # We need to rely on the caller or use `make_response` from flask
        from flask import current_app
        response = current_app.make_response(r)
        
        response.headers.update({
            "X-RateLimit-Limit": MAX_REQ,
            "X-RateLimit-Remaining": g.rate_remaining,
            "X-RateLimit-Reset": g.rate_reset
        })
        return response
    return wrapped

# ─── Color Support ────────────────────────────────────────────────────────────
def get_color(i: int, total: int, intensity: int) -> str:
    ang = i * intensity * 2 * math.pi / max(total, 1)
    r = int((math.sin(ang) + 1) / 2 * 255)
    g = int((math.sin(ang + 2 * math.pi / 3) + 1) / 2 * 255)
    b = int((math.sin(ang + 4 * math.pi / 3) + 1) / 2 * 255)
    return f"rgb({r},{g},{b})"

# ─── Chart Data Helpers ───────────────────────────────────────────────────────
def parse_chart_params(request_args) -> dict:
    """
    Parses and sanitizes chart data parameters from a request object (or dict).
    Returns a dictionary of cleaned parameters ready for get_chart_data.
    """
    from datetime import datetime, timezone, timedelta

    # Helper to parse days_to_show early for calculating default start_date
    def _get_days():
        try:
            return max(1, min(365, int(request_args.get('days_to_show', 7))))
        except Exception:
            return 7
    
    days = _get_days()
    # Default start_date should show the last N days ENDING at today, not starting today
    today_dt = datetime.now(timezone.utc)
    default_start = (today_dt - timedelta(days=days - 1)).strftime('%Y-%m-%d')
    start_date_str = request_args.get('start_date', default_start)
    
    # Clamp numeric inputs to reasonable ranges to protect the server.
    def _to_int(name, default):
        try:
            return int(request_args.get(name, default))
        except Exception:
            return default
    def _to_float(name, default):
        try:
            return float(request_args.get(name, default))
        except Exception:
            return default

    days_to_show = _to_int('days_to_show', 7)
    maps_to_show = _to_int('maps_to_show', 10)
    percision = _to_int('percision', 2)
    color_intensity = _to_int('color_intensity', 50)
    bias_exponent = _to_float('bias_exponent', 1.2)

    only_maps_containing_str = request_args.get('only_maps_containing', '')
    only_maps_containing = [s.strip() for s in only_maps_containing_str.split(',') if s.strip()]

    append_maps_containing_str = request_args.get('append_maps_containing', '')
    append_maps_containing = [s.strip() for s in append_maps_containing_str.split(',') if s.strip()]

    top_servers = _to_int('top_servers', 10)

    # Server filter: 'ALL' or 'IP:PORT'
    server_filter = request_args.get('server_filter', 'ALL').strip() or 'ALL'

    # Apply clamping
    days_to_show = max(1, min(365, days_to_show))
    maps_to_show = max(1, min(50, maps_to_show))
    percision = max(0, min(6, percision))
    color_intensity = max(1, min(50, color_intensity))
    bias_exponent = max(0.1, min(8.0, bias_exponent))
    top_servers = max(1, min(50, top_servers))

    return {
        'start_date_str': start_date_str,
        'days_to_show': days_to_show,
        'maps_to_show': maps_to_show,
        'percision': percision,
        'color_intensity': color_intensity,
        'bias_exponent': bias_exponent,
        'only_maps_containing': only_maps_containing,
        'append_maps_containing': append_maps_containing,
        'top_servers': top_servers,
        'server_filter': server_filter
    }

