import os
import time
import math
import logging
from functools import wraps
from flask import request, jsonify, g, abort
from dotenv import load_dotenv

# Load environment variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

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
from datetime import datetime
from collections import defaultdict, deque

# Structure: { 'YYYY-MM-DD': { ip: { 'endpoints': {endpoint: count}, 'requests': deque(maxlen=10) } } }
# But defaultdict is tricky with nested complex types. Let's use a standard dict insertion if missing.
# Revised structure: daily_request_stats[date][ip] = { 'endpoints': defaultdict(int), 'requests': deque(maxlen=10) }

daily_request_stats = defaultdict(lambda: defaultdict(lambda: {
    'endpoints': defaultdict(int),
    'requests': deque(maxlen=10)
}))
daily_stats_lock = None  # Will be initialized when threading is available

def track_request(ip: str, endpoint: str):
    """Track a request for admin statistics."""
    today = datetime.now().strftime('%Y-%m-%d')
    entry = daily_request_stats[today][ip]
    
    entry['endpoints'][endpoint] += 1
    
    # Track detailed request info
    try:
        full_path = request.full_path if request.full_path else request.path
        # strip trailing ? if empty query params
        if full_path.endswith('?'):
            full_path = full_path[:-1]
    except:
        full_path = endpoint

    entry['requests'].appendleft({
        'timestamp': datetime.now().strftime('%H:%M:%S'),
        'endpoint': endpoint,
        'full_path': full_path
    })

def get_request_stats():
    """Get request statistics for the admin panel."""
    today = datetime.now().strftime('%Y-%m-%d')
    today_data = daily_request_stats.get(today, {})
    
    # Calculate totals
    total_requests = 0
    unique_ips = set()
    ip_breakdown = []
    
    for ip, data in today_data.items():
        unique_ips.add(ip)
        ip_total = sum(data['endpoints'].values())
        total_requests += ip_total
        
        # Convert deque to list for JSON serialization
        recent_requests = list(data['requests'])
        
        ip_breakdown.append({
            'ip': ip,
            'total_requests': ip_total,
            'endpoints': dict(data['endpoints']),
            'recent_requests': recent_requests
        })
    
    # Sort by total requests descending
    ip_breakdown.sort(key=lambda x: x['total_requests'], reverse=True)
    
    return {
        'date': today,
        'total_requests': total_requests,
        'unique_ips': len(unique_ips),
        'ip_breakdown': ip_breakdown
    }

def cleanup_old_stats(days_to_keep=7):
    """Remove stats older than N days."""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
    keys_to_remove = [k for k in daily_request_stats if k < cutoff]
    for k in keys_to_remove:
        del daily_request_stats[k]

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

