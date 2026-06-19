import os
import duckdb
import time
import threading
import logging
import shutil
import re
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from utils import get_color, get_country

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "sourcemapstats.duckdb")
DB_REPLICA_FILE = os.path.join(BASE_DIR, "sourcemapstats_replica.duckdb")
RAW_FILE = os.path.join(BASE_DIR, "output.csv") # For migration only

ReaderTimeFormat = "%Y-%m-%d-%H:%M:%S"
CACHE_EXPIRY_SECONDS = 300
CHART_WORKERS = 4
query_executor = ThreadPoolExecutor(max_workers=CHART_WORKERS)

def _parse_datetime(value):
    if not isinstance(value, str):
        return value
    try:
        return datetime.strptime(value, ReaderTimeFormat)
    except ValueError:
        return datetime.fromisoformat(value)

def _regex_filter_pattern(values):
    safe_tokens = [re.escape(s)[:50] for s in values or [] if isinstance(s, str) and s]
    if not safe_tokens:
        return None
    return '(?i)' + '|'.join(safe_tokens)

# ─── data cache ───────────────────────────────────────────────────────────────
g_chart_data_cache = {}
g_known_server_names = {} # (ip, port) -> name

# ─── served data cache (decoupled from DB) ────────────────────────────────────
g_served_data = {
    'freshness': None,           
    'date_range': {'min_date': None, 'max_date': None},
    'last_updated': 0,           
    'default_chart_data': None,  
}
g_served_lock = threading.RLock() 
g_replica_lock = threading.RLock() 

DEFAULT_CHART_PARAMS = {
    'days_to_show': 7,
    'maps_to_show': 15,
    'only_maps_containing': [],
    'percision': 2,
    'color_intensity': 3,
    'bias_exponent': 1.2,
    'top_servers': 10,
    'append_maps_containing': None,
    'server_filter': 'ALL',
    'only_servers_containing': [],
}

def init_db(db_path=DB_FILE):
    try:
        with duckdb.connect(db_path) as con:
            # 1. Create Normalized Tables
            con.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_servers START 1;
                CREATE SEQUENCE IF NOT EXISTS seq_maps START 1;
                CREATE SEQUENCE IF NOT EXISTS seq_snapshots START 1;
            """)
            
            con.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_servers'),
                    ip TEXT,
                    port INTEGER,
                    country_code TEXT,
                    UNIQUE(ip, port)
                )
            """)
            
            con.execute("""
                CREATE TABLE IF NOT EXISTS maps (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_maps'),
                    name TEXT UNIQUE
                )
            """)
            
            con.execute("""
                CREATE TABLE IF NOT EXISTS snaps (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_snapshots'),
                    guid TEXT UNIQUE,
                    timestamp TIMESTAMP
                )
            """)
            
            # Optimized samples table
            con.execute("""
                CREATE TABLE IF NOT EXISTS samples_v2 (
                    snapshot_id INTEGER,
                    server_id INTEGER,
                    map_id INTEGER,
                    players INTEGER,
                    FOREIGN KEY (snapshot_id) REFERENCES snaps(id),
                    FOREIGN KEY (server_id) REFERENCES servers(id),
                    FOREIGN KEY (map_id) REFERENCES maps(id)
                )
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS samples_v3 (
                    snapshot_id INTEGER,
                    server_id INTEGER,
                    map_id INTEGER,
                    players INTEGER
                )
            """)

            con.execute("""
                CREATE OR REPLACE VIEW samples_all AS
                SELECT snapshot_id, server_id, map_id, players FROM samples_v2
                UNION ALL
                SELECT snapshot_id, server_id, map_id, players FROM samples_v3
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS sample_rollups_2h (
                    bucket TIMESTAMP,
                    server_id INTEGER,
                    map_id INTEGER,
                    players BIGINT,
                    PRIMARY KEY (bucket, server_id, map_id)
                )
            """)

            # Cooldowns and Names tables remain mostly same
            con.execute("""
                CREATE TABLE IF NOT EXISTS server_cooldowns (
                    ip TEXT,
                    port INTEGER,
                    timeout DOUBLE,
                    failures INTEGER,
                    skip_until DOUBLE,
                    updated_at TIMESTAMP,
                    PRIMARY KEY (ip, port)
                )
            """)
            
            con.execute("""
                CREATE TABLE IF NOT EXISTS server_names (
                    ip TEXT,
                    port INTEGER,
                    name TEXT,
                    updated_at TIMESTAMP,
                    PRIMARY KEY (ip, port)
                )
            """)
            
            con.execute("CREATE INDEX IF NOT EXISTS idx_snaps_timestamp ON snaps(timestamp)")
            # No timestamp index on samples needed anymore, filtering by snapshot_id (which is filtered by snaps.timestamp)

            # Migration Check: 'samples' (old) exists but 'samples_v2' is empty?
            try:
                row_old = con.execute("SELECT count(*) FROM samples").fetchone()
                count_old = row_old[0] if row_old else 0
                row_new = con.execute("SELECT count(*) FROM samples_v2").fetchone()
                count_new = row_new[0] if row_new else 0
            except:
                count_old = 0
                count_new = 0
            
            if count_old > 0 and count_new == 0:
                logging.info(f"detected legacy data ({count_old} rows). Starting migration to normalized schema...")
                try:
                    # 1. Populate Dimension Tables
                    logging.info("Migrating servers...")
                    con.execute("INSERT OR IGNORE INTO servers (ip, port, country_code) SELECT DISTINCT ip, port, country_code FROM samples")
                    
                    logging.info("Migrating maps...")
                    con.execute("INSERT OR IGNORE INTO maps (name) SELECT DISTINCT map FROM samples")
                    
                    logging.info("Migrating snapshots...")
                    # Sync from old snapshots table if possible, else from samples
                    con.execute("INSERT OR IGNORE INTO snaps (guid, timestamp) SELECT snapshot_id, timestamp FROM snapshots")
                    # Fallback for missing snapshots
                    con.execute("INSERT OR IGNORE INTO snaps (guid, timestamp) SELECT DISTINCT snapshot_id, timestamp FROM samples WHERE snapshot_id NOT IN (SELECT guid FROM snaps)")
                    
                    # 2. Populate Fact Table
                    logging.info("Migrating samples (this may take a moment)...")
                    con.execute("""
                        INSERT INTO samples_v2 (snapshot_id, server_id, map_id, players)
                        SELECT 
                            sn.id, s.id, m.id, old.players
                        FROM samples old
                        JOIN snaps sn ON old.snapshot_id = sn.guid
                        JOIN servers s ON old.ip = s.ip AND old.port = s.port
                        JOIN maps m ON old.map = m.name
                    """)
                    
                    logging.info("Migration complete. Optimizing storage...")
                    # Drop old tables
                    con.execute("DROP TABLE samples")
                    con.execute("DROP TABLE snapshots")
                    con.execute("CHECKPOINT")
                    con.execute("VACUUM")
                    logging.info("Database optimized.")
                except Exception as e:
                    logging.error(f"Migration failed: {e}")

            try:
                rollup_count = con.execute("SELECT count(*) FROM sample_rollups_2h").fetchone()[0] or 0
                sample_count = con.execute("SELECT count(*) FROM samples_all").fetchone()[0] or 0
                if sample_count > 0 and rollup_count == 0:
                    logging.info("Backfilling 2-hour sample rollups from existing raw sample data...")
                    backfill_start = time.time()
                    con.execute("""
                        INSERT INTO sample_rollups_2h (bucket, server_id, map_id, players)
                        SELECT
                            time_bucket(INTERVAL '2 hours', sn.timestamp) AS bucket,
                            sa.server_id,
                            sa.map_id,
                            SUM(sa.players)::BIGINT AS players
                        FROM samples_all sa
                        JOIN snaps sn ON sa.snapshot_id = sn.id
                        GROUP BY 1, 2, 3
                    """)
                    logging.info("Backfilled 2-hour sample rollups in %.2fs", time.time() - backfill_start)
            except Exception as e:
                logging.warning(f"Failed to backfill sample rollups: {e}")
             
    except Exception as e:
        logging.error(f"Failed to initialize DuckDB: {e}")
    
    # Always sync replica on startup to ensure schema changes propagate
    if os.path.exists(db_path) and db_path == DB_FILE:
        update_replica_db()
        load_server_names_from_db()

def rebuild_database():
    """
    Creates a new database file, copies all data from the old one,
    and replaces the old file. This forces full compaction.
    """
    NEW_DB = os.path.join(BASE_DIR, "sourcemapstats_new.duckdb")
    
    if os.path.exists(NEW_DB):
        try:
            os.remove(NEW_DB)
        except Exception:
            logging.error(f"Cannot remove temp DB {NEW_DB}, aborting rebuild.")
            return

    logging.info("Rebuilding database to reclaim space...")

    try:
        with g_replica_lock:
            # Initialize schema in new DB using the common init function
            init_db(NEW_DB)

            with duckdb.connect(NEW_DB) as con_new:
                # Attach old DB to read from it. This must be serialized with chart reads.
                con_new.execute(f"ATTACH '{DB_FILE}' AS old_db")

                logging.info("Copying tables...")

                # Copy Table Data
                tables_to_copy = ['servers', 'maps', 'snaps', 'samples_v2', 'samples_v3', 'sample_rollups_2h', 'server_cooldowns', 'server_names']

                for tbl in tables_to_copy:
                    try:
                        con_new.execute(f"INSERT INTO {tbl} SELECT * FROM old_db.{tbl}")
                    except Exception as e:
                        logging.warning(f"Could not copy table {tbl}: {e}")

                con_new.execute("DETACH old_db")

                # Reset sequences to match max id (DuckDB doesn't always have setval, so we recreate)
                max_server_id = con_new.execute("SELECT max(id) FROM servers").fetchone()[0] or 0
                con_new.execute(f"DROP SEQUENCE IF EXISTS seq_servers")
                con_new.execute(f"CREATE SEQUENCE seq_servers START {max_server_id + 1}")

                max_map_id = con_new.execute("SELECT max(id) FROM maps").fetchone()[0] or 0
                con_new.execute(f"DROP SEQUENCE IF EXISTS seq_maps")
                con_new.execute(f"CREATE SEQUENCE seq_maps START {max_map_id + 1}")

                max_snap_id = con_new.execute("SELECT max(id) FROM snaps").fetchone()[0] or 0
                con_new.execute(f"DROP SEQUENCE IF EXISTS seq_snapshots")
                con_new.execute(f"CREATE SEQUENCE seq_snapshots START {max_snap_id + 1}")

            BACKUP_DB = DB_FILE + ".bak"
            if os.path.exists(BACKUP_DB):
                os.remove(BACKUP_DB)

            os.rename(DB_FILE, BACKUP_DB)
            os.rename(NEW_DB, DB_FILE)

            logging.info(f"Database rebuild complete. Old size: {os.path.getsize(BACKUP_DB) / 1024 / 1024:.2f}MB, New size: {os.path.getsize(DB_FILE) / 1024 / 1024:.2f}MB")

            # Sync replica immediately
            update_replica_db()

    except Exception as e:
        logging.error(f"Rebuild failed: {e}")
        # Cleanup
        if os.path.exists(NEW_DB):
            os.remove(NEW_DB)


def maintenance():
    """Run VACUUM and CHECKPOINT to reclaim disk space."""
    try:
        logging.info("Starting database maintenance (VACUUM)...")
        with g_replica_lock:
            with duckdb.connect(DB_FILE) as con:
                con.execute("CHECKPOINT")
                con.execute("VACUUM")
        logging.info("Database maintenance complete.")
        update_replica_db()
    except Exception as e:
        logging.error(f"Database maintenance failed: {e}")

def load_server_names_from_db():
    try:
        with duckdb.connect(DB_FILE) as con:
            rows = con.execute("SELECT ip, port, name FROM server_names").fetchall()
            for r in rows:
                g_known_server_names[(r[0], r[1])] = r[2]
        logging.info(f"Loaded {len(g_known_server_names)} server names into cache.")
    except Exception as e:
        logging.warning(f"Failed to load server names cache: {e}")

def update_replica_db():
    try:
        if os.path.exists(DB_FILE):
            # Synchronization: Prevent updating while reader is active
            # If a long chart query is running, this will block until it finishes.
            copy_start = time.time()
            db_size_mb = os.path.getsize(DB_FILE) / 1024 / 1024
            with g_replica_lock:
                lock_wait = time.time() - copy_start
                file_copy_start = time.time()
                shutil.copy2(DB_FILE, DB_REPLICA_FILE)
                file_copy_duration = time.time() - file_copy_start
                total_duration = time.time() - copy_start
                logging.info(
                    "Replica DB updated in %.2fs (copy=%.2fs, lock_wait=%.2fs, size=%.2fMB).",
                    total_duration,
                    file_copy_duration,
                    lock_wait,
                    db_size_mb,
                )
                return {
                    'total': total_duration,
                    'copy': file_copy_duration,
                    'lock_wait': lock_wait,
                    'size_mb': db_size_mb,
                }
    except Exception as e:
        logging.warning(f"Failed to update replica DB: {e}")
    return None

def load_cooldowns_from_db():
    cooldowns = {}
    MAX_TIMEOUT_CAP = 5.0
    try:
        with duckdb.connect(DB_FILE) as con:
            rows = con.execute(
                "SELECT ip, port, timeout, failures, skip_until FROM server_cooldowns"
            ).fetchall()
            for ip, port, timeout, failures, skip_until in rows:
                cooldowns[(ip, port)] = {
                    'timeout': min(timeout, MAX_TIMEOUT_CAP),
                    'failures': min(failures, 4),
                    'skip_until': skip_until
                }
            if cooldowns:
                logging.info(f"Loaded {len(cooldowns)} server cooldowns from database")
    except Exception as e:
        logging.debug(f"Could not load cooldowns from DB: {e}")
    return cooldowns

def save_cooldowns_to_db(cooldowns):
    if not cooldowns:
        return
    try:
        with g_replica_lock:
            with duckdb.connect(DB_FILE) as con:
                _save_cooldowns_to_db_locked(con, cooldowns)
    except Exception as e:
        logging.debug(f"Could not save cooldowns to DB: {e}")

def _save_cooldowns_to_db_locked(con, cooldowns):
    now = datetime.now()
    data = [
        {
            'ip': ip,
            'port': port,
            'timeout': d['timeout'],
            'failures': d['failures'],
            'skip_until': d['skip_until'],
            'updated_at': now
        }
        for (ip, port), d in cooldowns.items()
    ]
    if not data:
        return

    df = pd.DataFrame(data)
    con.execute(
        """
        INSERT OR REPLACE INTO server_cooldowns 
        SELECT * FROM df
        """
    )

def save_server_names_to_db(rows):
    if not rows:
        return
    
    server_updates = []
    now = datetime.now()
    
    for row in rows:
        try:
            if len(row) >= 6:
                ip = row[0]
                port = int(row[1])
                name = row[5]
                if name:
                    # Only update if new or changed
                    key = (ip, port)
                    if g_known_server_names.get(key) != name:
                        server_updates.append({'ip': ip, 'port': port, 'name': name, 'updated_at': now})
                        g_known_server_names[key] = name
        except Exception:
            continue

    if not server_updates:
        return

    try:
        with g_replica_lock:
            with duckdb.connect(DB_FILE) as con:
                _save_server_names_to_db_locked(con, server_updates)
    except Exception as e:
        logging.error(f"Failed to update server names in DuckDB: {e}")

def _save_server_names_to_db_locked(con, server_updates):
    df = pd.DataFrame(server_updates)
    con.execute(
        """
        INSERT OR REPLACE INTO server_names 
        SELECT * FROM df
        """
    )
    logging.info(f"Updated names for {len(server_updates)} servers.")

def record_snapshot(snapshot_id, snapshot_dt_str):
    try:
        with g_replica_lock:
            with duckdb.connect(DB_FILE) as con:
                _record_snapshot_locked(con, snapshot_id, snapshot_dt_str)
    except Exception as e:
        logging.error(f"Failed to record snapshot: {e}")

def _record_snapshot_locked(con, snapshot_id, snapshot_dt_str):
    ts = _parse_datetime(snapshot_dt_str)
    con.execute(
        "INSERT OR IGNORE INTO snaps (guid, timestamp) VALUES (?, ?)",
        [snapshot_id, ts]
    )

def write_samples(rows):
    if not rows:
        return

    total_start = time.time()
    parse_start = time.time()
    prepared_rows = []
    for row in rows:
        try:
            ip = row[0]
            port = int(row[1]) if row[1] is not None else 0
            map_name = row[2]
            players = int(row[3]) if row[3] is not None else 0
            ts_raw = row[4]
            ts = _parse_datetime(ts_raw)
            snapshot_id = row[6] if len(row) > 6 else None
            country_code = get_country(ip)
            prepared_rows.append((ip, port, map_name, players, ts, country_code, snapshot_id))
        except Exception as e:
            logging.debug(f"Skipping row due to parse error: {row} ({e})")
    parse_duration = time.time() - parse_start

    if not prepared_rows:
        logging.info(
            "[DB] write_samples skipped: input_rows=%d parsed_rows=0 parse=%.4fs",
            len(rows),
            parse_duration,
        )
        return {
            'input_rows': len(rows),
            'parsed_rows': 0,
            'parse': parse_duration,
            'total': time.time() - total_start,
        }

    try:
        with g_replica_lock:
            with duckdb.connect(DB_FILE) as con:
                db_start = time.time()

                df_start = time.time()
                df = pd.DataFrame(
                    prepared_rows,
                    columns=['ip', 'port', 'map', 'players', 'timestamp', 'country_code', 'guid']
                )
                con.register('raw_samples_df', df)
                df_duration = time.time() - df_start

                maps_start = time.time()
                con.execute("INSERT OR IGNORE INTO maps (name) SELECT DISTINCT map FROM raw_samples_df")
                maps_duration = time.time() - maps_start

                servers_start = time.time()
                con.execute("""
                    INSERT OR IGNORE INTO servers (ip, port, country_code)
                    SELECT ip, port, max(country_code)
                    FROM raw_samples_df
                    GROUP BY ip, port
                """)
                servers_duration = time.time() - servers_start

                samples_start = time.time()
                con.execute("""
                    INSERT INTO samples_v3 (snapshot_id, server_id, map_id, players)
                    SELECT 
                        sn.id, s.id, m.id, t.players
                    FROM raw_samples_df t
                    JOIN snaps sn ON t.guid = sn.guid
                    JOIN servers s ON t.ip = s.ip AND t.port = s.port
                    JOIN maps m ON t.map = m.name
                """)
                samples_duration = time.time() - samples_start

                rollups_start = time.time()
                con.execute("""
                    CREATE TEMPORARY TABLE rollup_delta AS
                    SELECT
                        time_bucket(INTERVAL '2 hours', sn.timestamp) AS bucket,
                        s.id AS server_id,
                        m.id AS map_id,
                        SUM(t.players)::BIGINT AS players
                    FROM raw_samples_df t
                    JOIN snaps sn ON t.guid = sn.guid
                    JOIN servers s ON t.ip = s.ip AND t.port = s.port
                    JOIN maps m ON t.map = m.name
                    GROUP BY 1, 2, 3
                """)
                con.execute("""
                    INSERT OR REPLACE INTO sample_rollups_2h (bucket, server_id, map_id, players)
                    SELECT
                        d.bucket,
                        d.server_id,
                        d.map_id,
                        d.players + COALESCE(r.players, 0)
                    FROM rollup_delta d
                    LEFT JOIN sample_rollups_2h r
                        ON r.bucket = d.bucket
                        AND r.server_id = d.server_id
                        AND r.map_id = d.map_id
                """)
                con.execute("DROP TABLE rollup_delta")
                rollups_duration = time.time() - rollups_start

                unregister_start = time.time()
                con.unregister('raw_samples_df')
                unregister_duration = time.time() - unregister_start
                db_duration = time.time() - db_start

                logging.info(
                    "[DB] write_samples timings: input_rows=%d parsed_rows=%d parse=%.4fs df=%.4fs maps=%.4fs servers=%.4fs samples=%.4fs rollups=%.4fs unregister=%.4fs db=%.4fs total=%.4fs",
                    len(rows),
                    len(prepared_rows),
                    parse_duration,
                    df_duration,
                    maps_duration,
                    servers_duration,
                    samples_duration,
                    rollups_duration,
                    unregister_duration,
                    db_duration,
                    time.time() - total_start,
                )
                return {
                    'input_rows': len(rows),
                    'parsed_rows': len(prepared_rows),
                    'parse': parse_duration,
                    'df': df_duration,
                    'maps': maps_duration,
                    'servers': servers_duration,
                    'samples': samples_duration,
                    'rollups': rollups_duration,
                    'unregister': unregister_duration,
                    'db': db_duration,
                    'total': time.time() - total_start,
                }

    except Exception as e:
        logging.error(f"Failed to write to DuckDB: {e}")
    return None

def get_data_freshness():
    with g_served_lock:
        return g_served_data.get('freshness')

def get_date_range():
    with g_served_lock:
        return g_served_data.get('date_range', {'min_date': None, 'max_date': None})

def _update_served_cache_from_db():
    freshness = None
    date_range = {'min_date': None, 'max_date': None}
    
    try:
        with g_replica_lock:
            with duckdb.connect(DB_FILE, read_only=True) as con:
                row = con.execute("SELECT max(timestamp) FROM snaps").fetchone()
                latest = row[0] if row else None
                if latest:
                    latest_dt = _parse_datetime(latest)
                    freshness = latest_dt.strftime(ReaderTimeFormat)
                
                row = con.execute("SELECT min(timestamp), max(timestamp) FROM snaps").fetchone()
                if row and row[0] is not None and row[1] is not None:
                    min_dt, max_dt = _parse_datetime(row[0]), _parse_datetime(row[1])
                    date_range = {'min_date': min_dt.strftime('%Y-%m-%d'), 'max_date': max_dt.strftime('%Y-%m-%d')}
    except Exception as e:
        logging.debug(f"Failed to update served cache: {e}")
    
    with g_served_lock:
        g_served_data['freshness'] = freshness
        g_served_data['date_range'] = date_range
        g_served_data['last_updated'] = time.time()
        g_chart_data_cache.clear()
    
    logging.debug(f"Served cache updated: freshness={freshness}")

def _precompute_default_chart_data():
    logging.info("[Cache] Pre-computing default chart data...")
    try:
        from datetime import timezone
        days = DEFAULT_CHART_PARAMS['days_to_show']
        start_date = (datetime.now(timezone.utc) - pd.Timedelta(days=days - 1)).strftime('%Y-%m-%d')
        
        chart_data = get_chart_data(
            start_date_str=start_date,
            days_to_show=days,
            only_maps_containing=DEFAULT_CHART_PARAMS['only_maps_containing'],
            maps_to_show=DEFAULT_CHART_PARAMS['maps_to_show'],
            percision=DEFAULT_CHART_PARAMS['percision'],
            color_intensity=DEFAULT_CHART_PARAMS['color_intensity'],
            bias_exponent=DEFAULT_CHART_PARAMS['bias_exponent'],
            top_servers=DEFAULT_CHART_PARAMS['top_servers'],
            append_maps_containing=DEFAULT_CHART_PARAMS['append_maps_containing'],
            server_filter=DEFAULT_CHART_PARAMS['server_filter'],
            only_servers_containing=DEFAULT_CHART_PARAMS['only_servers_containing'],
        )
        with g_served_lock:
            g_served_data['default_chart_data'] = chart_data
        logging.info("[Cache] Default chart data pre-computed successfully")
    except Exception as e:
        logging.error(f"[Cache] Failed to pre-compute chart data: {e}")

def refresh_served_cache():
    _update_served_cache_from_db()
    _precompute_default_chart_data()

def get_chart_data(start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent, top_servers=10, append_maps_containing=None, server_filter=None, only_servers_containing=None):
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
        server_filter or 'ALL',
        tuple(only_servers_containing or [])
    )

    cached_result = g_chart_data_cache.get(cache_key)
    if cached_result and (time.time() - cached_result['timestamp']) < CACHE_EXPIRY_SECONDS:
        logging.debug("Returning cached chart data.")
        return cached_result['data']

    future = query_executor.submit(
        _get_chart_data_worker,
        start_date_str,
        days_to_show,
        only_maps_containing,
        maps_to_show,
        percision,
        color_intensity,
        bias_exponent,
        top_servers,
        append_maps_containing,
        server_filter,
        only_servers_containing,
        cache_key
    )
    return future.result()

def _get_chart_data_worker(*args, **kwargs):
    # Wrapper to enforce locking without re-indenting the massive body
    with g_replica_lock:
        return _get_chart_data_worker_impl(*args, **kwargs)

def _get_chart_data_worker_impl(start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent, top_servers, append_maps_containing, server_filter, only_servers_containing, cache_key):
    logging.debug("Generating new chart data (Worker - Optimized SQL)...")
    _start_time = time.time()
    _step_time = _start_time

    try:
        logging.debug("[Chart] Connecting to database...")
        with duckdb.connect(DB_FILE, read_only=True) as con:
            row = con.execute("SELECT max(timestamp) FROM snaps").fetchone()
            max_date_in_data = row[0] if row else None
            if not max_date_in_data:
                return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

            max_date_in_data = _parse_datetime(max_date_in_data)

            start_date = pd.to_datetime(start_date_str) if start_date_str else (pd.Timestamp(max_date_in_data) - pd.Timedelta(days=days_to_show))

            if pd.isna(pd.Timestamp(max_date_in_data)) or (pd.Timestamp(max_date_in_data).date() < pd.Timestamp(start_date).date()):
                start_date = (pd.Timestamp(max_date_in_data) if not pd.isna(pd.Timestamp(max_date_in_data)) else pd.Timestamp.now()) - pd.Timedelta(days=days_to_show)
                logging.warning(f"Start date is out of range. Defaulting to last {days_to_show} days from max date: {start_date.date()}")

            end_date = pd.Timestamp(start_date) + pd.Timedelta(days=int(days_to_show))
            
            # Bucketing interval
            interval = '2 hours'
            window_start = pd.Timestamp(start_date).to_pydatetime()
            window_end = pd.Timestamp(end_date).to_pydatetime()
            use_rollups = con.execute(
                "SELECT EXISTS(SELECT 1 FROM sample_rollups_2h WHERE bucket >= ? AND bucket < ?)",
                [window_start, window_end]
            ).fetchone()[0]
            if use_rollups:
                logging.info("[Chart] Using 2h rollup table for aggregation")

            # Build Filter Clauses for SQL
            where_clauses = ["r.bucket >= ? AND r.bucket < ?"] if use_rollups else ["sn.timestamp >= ? AND sn.timestamp < ?"]
            query_params = [window_start, window_end]

            # 1. Map Name Filter
            pattern = _regex_filter_pattern(only_maps_containing)
            if pattern:
                where_clauses.append("REGEXP_MATCHES(m.name, ?)")
                query_params.append(pattern)

            # 2. Server Filter (IP:Port)
            if server_filter and isinstance(server_filter, str) and server_filter.upper() != 'ALL':
                try:
                    ip_str, port_str = server_filter.split(':', 1)
                    ip_str = ip_str.strip()
                    port_val = int(port_str.strip())
                    if ip_str and port_val >= 0:
                        where_clauses.append("s.ip = ? AND s.port = ?")
                        query_params.extend([ip_str, port_val])
                except Exception:
                    pass

            # 3. Server Name Filter
            pattern = _regex_filter_pattern(only_servers_containing)
            if pattern:
                where_clauses.append("""
                    EXISTS (
                        SELECT 1 FROM server_names snam 
                        WHERE snam.ip = s.ip AND snam.port = s.port 
                        AND REGEXP_MATCHES(snam.name, ?)
                    )
                """)
                query_params.append(pattern)

            where_str = " AND ".join(where_clauses)

            # --- FETCH 1: Total Snapshots per Bucket (Global denominator) ---
            # This is NOT filtered by maps/servers because it represents the global availability of the system
            daily_total_snapshots_df = con.execute(
                f"""
                SELECT 
                    time_bucket(INTERVAL '{interval}', timestamp) as date,
                    COUNT(DISTINCT guid) as total_snapshots
                FROM snaps 
                WHERE timestamp >= ? AND timestamp < ?
                GROUP BY 1
                ORDER BY 1
                """,
                [window_start, window_end]
            ).df()
            daily_total_snapshots_df['date'] = pd.to_datetime(daily_total_snapshots_df['date'])

            # --- FETCH 2: Aggregated Player Sum per Map per Bucket (Filtered) ---
            if use_rollups:
                df_agg = con.execute(
                    f"""
                    SELECT 
                        r.bucket as date,
                        m.name as map,
                        SUM(r.players) as players
                    FROM sample_rollups_2h r
                    JOIN servers s ON r.server_id = s.id
                    JOIN maps m ON r.map_id = m.id
                    WHERE {where_str}
                    GROUP BY 1, 2
                    """,
                    query_params
                ).df()
            else:
                df_agg = con.execute(
                    f"""
                    SELECT 
                        time_bucket(INTERVAL '{interval}', sn.timestamp) as date,
                        m.name as map,
                        SUM(sa.players) as players
                    FROM samples_all sa
                    JOIN snaps sn ON sa.snapshot_id = sn.id
                    JOIN servers s ON sa.server_id = s.id
                    JOIN maps m ON sa.map_id = m.id
                    WHERE {where_str}
                    GROUP BY 1, 2
                    """,
                    query_params
                ).df()
            df_agg['date'] = pd.to_datetime(df_agg['date'])

            # --- FETCH 3: Aggregated Player Sum per Server per Bucket (Filtered) ---
            # Used for the "Top Servers" chart for the current view
            if use_rollups:
                df_server_agg = con.execute(
                    f"""
                    SELECT 
                        r.bucket as date,
                        s.ip, s.port,
                        SUM(r.players) as players
                    FROM sample_rollups_2h r
                    JOIN servers s ON r.server_id = s.id
                    JOIN maps m ON r.map_id = m.id
                    WHERE {where_str}
                    GROUP BY 1, 2, 3
                    """,
                    query_params
                ).df()
            else:
                df_server_agg = con.execute(
                    f"""
                    SELECT 
                        time_bucket(INTERVAL '{interval}', sn.timestamp) as date,
                        s.ip, s.port,
                        SUM(sa.players) as players
                    FROM samples_all sa
                    JOIN snaps sn ON sa.snapshot_id = sn.id
                    JOIN servers s ON sa.server_id = s.id
                    JOIN maps m ON sa.map_id = m.id
                    WHERE {where_str}
                    GROUP BY 1, 2, 3
                    """,
                    query_params
                ).df()
            df_server_agg['date'] = pd.to_datetime(df_server_agg['date'])

            # --- FETCH 4: Global server stats (Unfiltered window) ---
            # Used for the "Global Server Ranking" table
            if use_rollups:
                df_global_server_agg = con.execute(
                    """
                    SELECT 
                        r.bucket as date,
                        s.ip, s.port,
                        SUM(r.players) as players
                    FROM sample_rollups_2h r
                    JOIN servers s ON r.server_id = s.id
                    WHERE r.bucket >= ? AND r.bucket < ?
                    GROUP BY 1, 2, 3
                    """,
                    [window_start, window_end]
                ).df()
            else:
                df_global_server_agg = con.execute(
                    f"""
                    SELECT 
                        time_bucket(INTERVAL '{interval}', sn.timestamp) as date,
                        s.ip, s.port,
                        SUM(sa.players) as players
                    FROM samples_all sa
                    JOIN snaps sn ON sa.snapshot_id = sn.id
                    JOIN servers s ON sa.server_id = s.id
                    WHERE sn.timestamp >= ? AND sn.timestamp < ?
                    GROUP BY 1, 2, 3
                    """,
                    [window_start, window_end]
                ).df()
            df_global_server_agg['date'] = pd.to_datetime(df_global_server_agg['date'])

            # Helper to fetch server names for labeling
            server_names_map = {}
            try:
                rows = con.execute("SELECT ip, port, name FROM server_names").fetchall()
                for r_ip, r_port, r_name in rows:
                    server_names_map[(r_ip, r_port)] = r_name
            except Exception as e:
                logging.debug(f"Failed to load server names: {e}")

    except Exception as e:
        logging.error(f"[Chart] Failed to load data from DuckDB: {e}")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    if df_agg.empty:
        logging.warning("No data available for the selected parameters.")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    logging.info(f"[Chart] SQL Aggregation complete in {time.time() - _step_time:.2f}s")
    _step_time = time.time()

    # --- PROCESS 1: Map Datasets ---
    full_time_index = pd.date_range(start=pd.Timestamp(start_date).floor('2h'), end=pd.Timestamp(end_date).ceil('2h'), freq='2h')
    full_time_index = full_time_index[(full_time_index >= pd.Timestamp(start_date)) & (full_time_index < pd.Timestamp(end_date))]
    
    # Merge with total snapshots to get avg_players
    merged_df = pd.merge(df_agg, daily_total_snapshots_df, on='date', how='left')
    merged_df['total_snapshots'] = merged_df['total_snapshots'].fillna(1).replace(0, 1)
    merged_df['avg_players'] = (merged_df['players'] / merged_df['total_snapshots']).round(percision)
    
    # Percentage calculation
    daily_total_avg_players = merged_df.groupby('date')['avg_players'].transform('sum')
    merged_df['player_percentage'] = (merged_df['avg_players'] / daily_total_avg_players.replace(0, 1) * 100).fillna(0)

    top_maps = merged_df.groupby('map')['avg_players'].sum().nlargest(maps_to_show).index
    datasets = []
    for map_name in top_maps:
        map_data = merged_df[merged_df['map'] == map_name]
        datasets.append({
            'label': map_name,
            'data': list(map_data.set_index('date')['player_percentage'].reindex(full_time_index, fill_value=0)),
            'backgroundColor': get_color(len(datasets), len(top_maps), color_intensity),
            'borderColor': get_color(len(datasets), len(top_maps), color_intensity).replace('rgb', 'rgba').replace(')', ', 1)'),
            'borderWidth': 1
        })

    appended_map_names = []
    pattern = _regex_filter_pattern(append_maps_containing)
    if pattern:
        try:
            matched = merged_df[merged_df['map'].str.contains(pattern, na=False, regex=True)]['map'].unique().tolist()
            appended_map_names = [m for m in matched if m not in set(top_maps)]
            if appended_map_names:
                avg_map = merged_df.groupby('map')['avg_players'].sum()
                appended_map_names.sort(key=lambda m: float(avg_map.get(m, 0)), reverse=True)
        except re.error:
            pass

    for map_name in appended_map_names:
        map_data = merged_df[merged_df['map'] == map_name]
        datasets.append({
            'label': map_name,
            'data': list(map_data.set_index('date')['player_percentage'].reindex(full_time_index, fill_value=0)),
            'backgroundColor': get_color(len(datasets), max(1, len(top_maps) + len(appended_map_names)), color_intensity),
            'borderColor': get_color(len(datasets), max(1, len(top_maps) + len(appended_map_names)), color_intensity).replace('rgb', 'rgba').replace(')', ', 1)'),
            'borderWidth': 1
        })

    other_exclude = set(top_maps).union(set(appended_map_names))
    other_maps_df = merged_df[~merged_df['map'].isin(other_exclude)]
    if not other_maps_df.empty:
        other_data = other_maps_df.groupby('date')['player_percentage'].sum().reindex(full_time_index, fill_value=0)
        datasets.append({
            'label': 'Other',
            'data': list(other_data),
            'backgroundColor': 'rgba(128, 128, 128, 0.5)',
            'borderColor': 'rgba(128, 128, 128, 1)',
            'borderWidth': 1
        })

    # --- PROCESS 2: Daily Totals (Overall popularity line) ---
    daily_total_players_sum = df_agg.groupby('date')['players'].sum()
    daily_totals_indexed = daily_total_snapshots_df.set_index('date')['total_snapshots']
    daily_totals_df = (daily_total_players_sum.div(daily_totals_indexed, fill_value=0)).fillna(0)
    daily_totals = daily_totals_df.reindex(full_time_index, fill_value=0).round(percision).tolist()
    snapshot_counts = daily_totals_indexed.reindex(full_time_index, fill_value=0).tolist()

    # --- PROCESS 3: Server Contributions (View-specific) ---
    try:
        srv = df_server_agg.merge(daily_totals_indexed.rename('snapshots'), left_on='date', right_index=True, how='left')
        srv['snapshots'] = srv['snapshots'].fillna(1).replace(0, 1)
        srv['avg_contrib'] = (srv['players'] / srv['snapshots']).fillna(0)
        srv['server'] = srv['ip'] + ':' + srv['port'].astype(str)

        pivot = srv.pivot_table(index='date', columns='server', values='avg_contrib', aggfunc='sum').reindex(full_time_index, fill_value=0)
        
        num_valid_buckets = (daily_totals_indexed.reindex(full_time_index, fill_value=0) > 0).sum()
        num_valid_buckets = max(1, num_valid_buckets)
        averages = (pivot.sum(axis=0) / num_valid_buckets).sort_values(ascending=False)

        top_n = min(int(top_servers or 10), pivot.shape[1])
        top_servers_list = list(averages.head(top_n).index) if top_n > 0 else []
        server_ranking = [{ 'id': s, 'label': s, 'pop': round(float(averages[s]), 2) } for s in top_servers_list]
        
        if pivot.shape[1] > top_n:
            other_val = float(averages.iloc[top_n:].sum())
            if other_val > 0:
                server_ranking.append({ 'id': 'Other', 'label': 'Other', 'pop': round(other_val, 2) })

        total_players_server_datasets = []
        for idx, server in enumerate(top_servers_list):
            series = pivot[server].fillna(0)
            total_players_server_datasets.append({
                'label': server,
                'data': list(series.round(percision).astype(float).values),
                'backgroundColor': get_color(idx, max(1, len(top_servers_list)), color_intensity).replace('rgb', 'rgba').replace(')', ', 0.5)'),
                'borderColor': get_color(idx, max(1, len(top_servers_list)), color_intensity),
                'fill': True,
                'stack': 'servers',
            })

        if pivot.shape[1] > len(top_servers_list):
            other_series = pivot.drop(columns=top_servers_list, errors='ignore').sum(axis=1).fillna(0)
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
        server_ranking = []

    # --- PROCESS 4: Global Server Ranking (Window-specific) ---
    try:
        gsrv = df_global_server_agg.merge(daily_totals_indexed.rename('snapshots'), left_on='date', right_index=True, how='left')
        gsrv['snapshots'] = gsrv['snapshots'].fillna(1).replace(0, 1)
        gsrv['avg_contrib'] = (gsrv['players'] / gsrv['snapshots']).fillna(0)
        gsrv['server'] = gsrv['ip'] + ':' + gsrv['port'].astype(str)
        
        # We can aggregate directly since we don't need a time series for global ranking
        g_averages = (gsrv.groupby('server')['avg_contrib'].sum() / num_valid_buckets).sort_values(ascending=False)
        top_n_w = min(10, len(g_averages))
        global_server_ranking = [{ 'label': srv, 'pop': round(float(g_averages[srv]), 2) } for srv in list(g_averages.head(top_n_w).index)]
        if len(g_averages) > top_n_w:
            other_val_w = float(g_averages.iloc[top_n_w:].sum())
            if other_val_w > 0:
                global_server_ranking.append({ 'label': 'Other', 'pop': round(other_val_w, 2) })
    except Exception as e:
        logging.debug(f"Failed to compute global server ranking: {e}")
        global_server_ranking = []

    # --- PROCESS 5: Map Ranking ---
    map_total_contrib = merged_df.groupby('map')['avg_players'].sum()
    total_contrib_sum = map_total_contrib.sum()
    ranking = []
    if total_contrib_sum > 0:
        top_maps_contrib = map_total_contrib[map_total_contrib.index.isin(top_maps)].sort_values(ascending=False)
        ranking = (top_maps_contrib / total_contrib_sum * 100).round(2).reset_index(name='pop').rename(columns={'map': 'label'}).to_dict('records')
        
        if appended_map_names:
            app_maps_contrib = map_total_contrib[map_total_contrib.index.isin(appended_map_names)].sort_values(ascending=False)
            ranking += (app_maps_contrib / total_contrib_sum * 100).round(2).reset_index(name='pop').rename(columns={'map': 'label'}).to_dict('records')

        if not other_maps_df.empty:
            other_maps_contrib_sum = map_total_contrib[~map_total_contrib.index.isin(other_exclude)].sum()
            if other_maps_contrib_sum > 0:
                ranking.append({'label': 'Other', 'pop': round((other_maps_contrib_sum / total_contrib_sum) * 100, 2)})

    def _sanitize_dataset_list(ds_list):
        out = []
        for ds in ds_list:
            s = dict(ds)
            if isinstance(s.get('data'), list):
                s['data'] = [0 if (isinstance(v, float) and (np.isnan(v))) else (float(v) if isinstance(v, (np.floating, np.integer)) else v) for v in s['data']]
            out.append(s)
        return out

    result = {
        'labels': [d.isoformat() for d in full_time_index],
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

    # Label Labeling (Server Names)
    def get_server_display_name(s_ip, s_port):
        try:
            return server_names_map.get((s_ip, int(s_port)), f"{s_ip}:{s_port}")
        except:
            return f"{s_ip}:{s_port}"

    for key in ['serverRanking', 'totalPlayersServerDatasets', 'globalServerRanking']:
        if key in result:
            for item in result[key]:
                label = item.get('label')
                if label and ':' in label and label != 'Other':
                    try:
                        s_ip, s_port = label.split(':', 1)
                        item['label'] = get_server_display_name(s_ip, s_port)
                    except:
                        pass

    logging.info(f"[Chart] Generation complete in {time.time() - _start_time:.2f}s (results={len(datasets)})")
    g_chart_data_cache[cache_key] = {'timestamp': time.time(), 'data': result}
    return result

# ─── New Helper ───────────────────────────────────────────────────────────────
def get_recent_ips(days=7):
    """
    Fetch distinct (ip, port) pairs that have appeared in the DB 
    within the last N days.
    """
    try:
        with duckdb.connect(DB_FILE) as con:
            cutoff = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            rows = con.execute(
                """
                SELECT DISTINCT s.ip, s.port 
                FROM samples_all sa
                JOIN snaps sn ON sa.snapshot_id = sn.id
                JOIN servers s ON sa.server_id = s.id
                WHERE sn.timestamp >= ?
                """,
                [cutoff]
            ).fetchall()
            return [(r[0], int(r[1])) for r in rows]
    except Exception as e:
        logging.error(f"Failed to fetch recent IPs from DB: {e}")
        return []
