import sys
import os
import threading
import logging
import mimetypes
import numpy as np
from flask import Flask, request
from flask.json.provider import DefaultJSONProvider
from waitress import serve
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix

# ─── logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ─── add local libs (pythonvalve + a2s) ───────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Ensure local libs are importable
sys.path.insert(0, os.path.join(BASE_DIR, "pythonvalve"))
sys.path.insert(0, os.path.join(BASE_DIR, "a2s"))

# Load environment variables
load_dotenv(os.path.join(BASE_DIR, '.env'))

# Modules
# Note: database and scanner might use a2s, so we imported it implicitly by fixing sys.path
import database
import scanner
import routes

# ─── basic constants ──────────────────────────────────────────────────────────
PUBLIC_MODE = True  # False → bind 127.0.0.1

# Ensure correct MIME types
mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('text/css', '.css')
mimetypes.add_type('image/svg+xml', '.svg')

# ─── flask app ────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

class RealIpMiddleware:
    """Middleware to force REMOTE_ADDR to use X-Real-IP if present."""
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        if 'HTTP_X_REAL_IP' in environ:
            environ['REMOTE_ADDR'] = environ['HTTP_X_REAL_IP']
        return self.app(environ, start_response)

app.wsgi_app = RealIpMiddleware(app.wsgi_app)

app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# Register Blueprint
app.register_blueprint(routes.bp)

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

if __name__ == '__main__':
    logging.info("Starting up...")
    
    # Initialize DB
    database.init_db()
    
    # Initialize served cache
    logging.info("Pre-initializing served cache...")
    database.refresh_served_cache()
    
    # Start the scanning loop in a background thread
    scan_thread = threading.Thread(target=scanner.scan_loop, daemon=True)
    scan_thread.start()

    host = "0.0.0.0" if PUBLIC_MODE else "127.0.0.1"
    port = int(os.getenv("PORT", 5000))
    
    logging.info(f"Serving on {host}:{port}")
    serve(app, host=host, port=port, threads=8)
