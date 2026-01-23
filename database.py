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

# ─── data cache ───────────────────────────────────────────────────────────────
g_chart_data_cache = {}

# ─── served data cache (decoupled from DB) ────────────────────────────────────
g_served_data = {
    'freshness': None,           
    'date_range': {'min_date': None, 'max_date': None},
    'last_updated': 0,           
    'default_chart_data': None,  
}
g_served_lock = threading.RLock() 

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
}

def init_db():
    try:
        with duckdb.connect(DB_FILE) as con:
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
            
    except Exception as e:
        logging.error(f"Failed to initialize DuckDB: {e}")
    
    # Always sync replica on startup to ensure schema changes propagate
    if os.path.exists(DB_FILE):
        update_replica_db()

def update_replica_db():
    try:
        if os.path.exists(DB_FILE):
            shutil.copy2(DB_FILE, DB_REPLICA_FILE)
            logging.info("Replica DB updated.")
    except Exception as e:
        logging.warning(f"Failed to update replica DB: {e}")

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
        with duckdb.connect(DB_FILE) as con:
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
                    server_updates.append((ip, port, name, now))
        except Exception:
            continue

    if not server_updates:
        return

    try:
        with duckdb.connect(DB_FILE) as con:
            con.executemany(
                """
                INSERT OR REPLACE INTO server_names (ip, port, name, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                server_updates
            )
    except Exception as e:
        logging.error(f"Failed to update server names in DuckDB: {e}")

def record_snapshot(snapshot_id, snapshot_dt_str):
    try:
        with duckdb.connect(DB_FILE) as con:
            try:
                ts = datetime.strptime(snapshot_dt_str, ReaderTimeFormat)
            except ValueError:
                ts = datetime.fromisoformat(snapshot_dt_str)
            
            con.execute(
                "INSERT OR IGNORE INTO snaps (guid, timestamp) VALUES (?, ?)",
                [snapshot_id, ts]
            )
    except Exception as e:
        logging.error(f"Failed to record snapshot: {e}")

def write_samples(rows):
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
            snapshot_id = row[6] if len(row) > 6 else None
            country_code = get_country(ip)
            prepared_rows.append((ip, port, map_name, players, ts, country_code, snapshot_id))
        except Exception as e:
            logging.debug(f"Skipping row due to parse error: {row} ({e})")

    if not prepared_rows:
        return

    try:
        with duckdb.connect(DB_FILE) as con:
            # Upsert maps
            maps = list(set(r[2] for r in prepared_rows))
            con.execute("INSERT OR IGNORE INTO maps (name) SELECT * FROM (VALUES " + ",".join(["(?)"] * len(maps)) + ")", maps)
            
            # Upsert servers
            # We want unique ip,port. 
            # Note: duckdb executemany might be slow if we do complex upserts.
            # Best is to just insert ignore.
            servers = {} # (ip, port) -> country
            for r in prepared_rows:
                servers[(r[0], r[1])] = r[5]
            
            # Prepare server tuples
            srv_tuples = [(ip, port, code) for (ip, port), code in servers.items()]
            con.executemany("INSERT OR IGNORE INTO servers (ip, port, country_code) VALUES (?, ?, ?)", srv_tuples)
             
            # Now we need IDs to insert into samples_v2
            # For bulk performance, we can load these into temp tables and join
            
            con.execute("CREATE TEMPORARY TABLE IF NOT EXISTS temp_raw_samples (ip TEXT, port INTEGER, map TEXT, players INTEGER, guid TEXT)")
            con.execute("DELETE FROM temp_raw_samples")
            
            raw_tuples = [(r[0], r[1], r[2], r[3], r[6]) for r in prepared_rows]
            con.executemany("INSERT INTO temp_raw_samples VALUES (?,?,?,?,?)", raw_tuples)
            
            con.execute("""
                INSERT INTO samples_v2 (snapshot_id, server_id, map_id, players)
                SELECT 
                    sn.id, s.id, m.id, t.players
                FROM temp_raw_samples t
                JOIN snaps sn ON t.guid = sn.guid
                JOIN servers s ON t.ip = s.ip AND t.port = s.port
                JOIN maps m ON t.map = m.name
            """)
            con.execute("DROP TABLE temp_raw_samples")

    except Exception as e:
        logging.error(f"Failed to write to DuckDB: {e}")

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
        with duckdb.connect(DB_REPLICA_FILE, read_only=True) as con:
            row = con.execute("SELECT max(timestamp) FROM snaps").fetchone()
            latest = row[0] if row else None
            if latest:
                if isinstance(latest, str):
                    try:
                        latest_dt = datetime.strptime(latest, ReaderTimeFormat)
                    except ValueError:
                        latest_dt = datetime.fromisoformat(latest)
                else:
                    latest_dt = latest
                freshness = latest_dt.strftime(ReaderTimeFormat)
            
            row = con.execute("SELECT min(timestamp), max(timestamp) FROM snaps").fetchone()
            if row and row[0] is not None and row[1] is not None:
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
        )
        with g_served_lock:
            g_served_data['default_chart_data'] = chart_data
        logging.info("[Cache] Default chart data pre-computed successfully")
    except Exception as e:
        logging.error(f"[Cache] Failed to pre-compute chart data: {e}")

def refresh_served_cache():
    _update_served_cache_from_db()
    _precompute_default_chart_data()

def get_chart_data(start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent, top_servers=10, append_maps_containing=None, server_filter=None):
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
        cache_key
    )
    return future.result()

def _get_chart_data_worker(start_date_str, days_to_show, only_maps_containing, maps_to_show, percision, color_intensity, bias_exponent, top_servers, append_maps_containing, server_filter, cache_key):
    logging.info("Generating new chart data (Worker)...")
    _start_time = time.time()

    try:
        logging.info("[Chart] Connecting to database (Replica)...")
        with duckdb.connect(DB_REPLICA_FILE, read_only=True) as con:
            row = con.execute("SELECT max(timestamp) FROM snaps").fetchone()
            max_date_in_data = row[0] if row else None
            if not max_date_in_data:
                return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

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

            logging.info(f"[Chart] Querying samples_v2 from {start_date.date()} to {end_date.date()}...")
            df_window = con.execute(
                """
                SELECT s.ip, s.port, m.name as map, sa.players, sn.timestamp, s.country_code, sn.guid as snapshot_id
                FROM samples_v2 sa
                JOIN snaps sn ON sa.snapshot_id = sn.id
                JOIN servers s ON sa.server_id = s.id
                JOIN maps m ON sa.map_id = m.id
                WHERE sn.timestamp >= ? AND sn.timestamp < ?
                """,
                [pd.Timestamp(start_date).to_pydatetime(), pd.Timestamp(end_date).to_pydatetime()]
            ).df()
            logging.info(f"[Chart] Query complete, fetched {len(df_window)} rows in {time.time() - _start_time:.2f}s")
            
            # Helper to fetch server names
            server_names_map = {}
            try:
                rows = con.execute("SELECT ip, port, name FROM server_names").fetchall()
                for r_ip, r_port, r_name in rows:
                    server_names_map[(r_ip, r_port)] = r_name
            except Exception as e:
                logging.debug(f"Failed to load server names: {e}")

    except Exception as e:
        logging.error(f"Failed to load data from DuckDB: {e}")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    if df_window.empty:
        logging.warning("No data available for the selected parameters.")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    logging.info(f"[Chart] Cleaning and normalizing data...")
    df = df_window.copy()
    
    # DEBUG LOGGING
    logging.info(f"DEBUG: df dtypes: {df.dtypes}")
    if not df.empty:
        logging.info(f"DEBUG: First row timestamp: {df.iloc[0]['timestamp']} (type: {type(df.iloc[0]['timestamp'])})")
    
    df['players'] = pd.to_numeric(df['players'], errors='coerce').fillna(0).astype(int)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # DEBUG LOGGING POST-CONVERSION
    if not df.empty:
         logging.info(f"DEBUG: Post-conversion timestamp: {df.iloc[0]['timestamp']}")
         null_ts = df['timestamp'].isna().sum()
         logging.info(f"DEBUG: Timestamps becoming NaT: {null_ts} / {len(df)}")
         
    df.dropna(subset=['timestamp'], inplace=True)

    if server_filter and isinstance(server_filter, str) and server_filter.upper() != 'ALL':
        try:
            ip_str, port_str = server_filter.split(':', 1)
            ip_str = ip_str.strip()
            port_val = int(port_str.strip())
            if ip_str and port_val >= 0:
                df = df[(df['ip'] == ip_str) & (df['port'] == port_val)]
        except Exception:
            pass

    if only_maps_containing:
        try:
            safe_tokens = [re.escape(s)[:50] for s in only_maps_containing if isinstance(s, str) and s]
            if safe_tokens:
                pattern = '|'.join(safe_tokens)
                df = df[df['map'].str.contains(pattern, na=False, regex=True)]
        except re.error:
            pass

    if df.empty:
        logging.warning("No data available for the selected parameters.")
        return {'labels': [], 'datasets': [], 'dailyTotals': [], 'snapshotCounts': [], 'ranking': [], 'shownMapsCount': 0, 'averageDailyPlayerCount': 0}

    logging.info(f"[Chart] Aggregating data ({len(df)} rows after filtering)...")
    # bucket to 2 hours
    df['date'] = df['timestamp'].dt.floor('2h')
    
    # 1. Calculate total snapshots per bucket (Global denominator) using the snapshots table
    try:
        with duckdb.connect(DB_REPLICA_FILE, read_only=True) as con_snap:
            # Optimize: Aggregate in DB directly using time_bucket
            daily_total_snapshots = con_snap.execute(
                """
                SELECT 
                    time_bucket(INTERVAL '2 hours', timestamp) as date,
                    COUNT(DISTINCT guid) as total_snapshots
                FROM snaps 
                WHERE timestamp >= ? AND timestamp < ?
                GROUP BY 1
                ORDER BY 1
                """,
                [pd.Timestamp(start_date).to_pydatetime(), pd.Timestamp(end_date).to_pydatetime()]
            ).df()
            # Ensure date is datetime for merging
            daily_total_snapshots['date'] = pd.to_datetime(daily_total_snapshots['date'])
    except Exception as e:
        logging.error(f"[Chart] Failed to query snapshots table: {e}. Falling back to samples count.")
        daily_total_snapshots = df.groupby('date')['snapshot_id'].nunique().reset_index()
        daily_total_snapshots.rename(columns={'snapshot_id': 'total_snapshots'}, inplace=True)

    # 2. Calculate total players per map per bucket
    daily_player_sum = df.groupby(['date', 'map'])['players'].sum().reset_index()

    # 3. Merge to get avg_players = sum(players) / total_snapshots
    merged_df = pd.merge(daily_player_sum, daily_total_snapshots, on='date', how='left')
    # Fill NaN total_snapshots with 1 to avoid division by zero (shouldn't happen if logic is correct)
    merged_df['total_snapshots'] = merged_df['total_snapshots'].fillna(1)
    
    # "Average Players" is now the total player-seconds(ish) divided by the cached time intervals (snapshots)
    merged_df['avg_players'] = (merged_df['players'] / merged_df['total_snapshots']).round(percision)
    
    # Calculate percentage share for the bucket
    daily_total_avg_players = merged_df.groupby('date')['avg_players'].transform('sum')
    merged_df['player_percentage'] = (merged_df['avg_players'] / daily_total_avg_players.replace(0, 1) * 100).fillna(0)

    logging.info(f"[Chart] Preparing chart datasets...")
    
    # Construct a comprehensive time index for filling gaps
    full_time_index = pd.date_range(start=pd.Timestamp(start_date).floor('2h'), end=pd.Timestamp(end_date).ceil('2h'), freq='2h')
    # filtered to actual requested range
    full_time_index = full_time_index[(full_time_index >= pd.Timestamp(start_date)) & (full_time_index < pd.Timestamp(end_date))]
    
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
    if append_maps_containing:
        try:
            safe_tokens = [re.escape(s)[:50] for s in append_maps_containing if isinstance(s, str) and s]
            if safe_tokens:
                pattern = '|'.join(safe_tokens)
                matched = merged_df[merged_df['map'].str.contains(pattern, na=False, regex=True)]['map'].unique().tolist()
                appended_map_names = [m for m in matched if m not in set(top_maps)]
                if appended_map_names:
                    avg_map = merged_df.groupby('map')['avg_players'].sum()
                    appended_map_names.sort(key=lambda m: float(avg_map.get(m, 0)), reverse=True)
        except re.error:
            appended_map_names = []

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

    total_daily_players = df.groupby('date')['players'].sum()
    total_daily_snapshots = df.groupby('date')['snapshot_id'].nunique()
    daily_totals_df = (total_daily_players / total_daily_snapshots).fillna(0)
    daily_totals = daily_totals_df.reindex(full_time_index, fill_value=0).round(percision).tolist()
    
    # Use the globally calculated daily_total_snapshots (from snapshots table)
    daily_totals_indexed = daily_total_snapshots.set_index('date')['total_snapshots']
    snapshot_counts = daily_totals_indexed.reindex(full_time_index, fill_value=0).tolist()

    logging.info(f"[Chart] Calculating per-server contributions...")
    try:
        srv_sum = df.groupby(['date', 'ip', 'port'])['players'].sum().reset_index()
        srv = srv_sum.merge(
            daily_totals_indexed.rename('snapshots'), left_on='date', right_index=True, how='left'
        )
        srv['snapshots'] = srv['snapshots'].fillna(1)
        srv['avg_contrib'] = (srv['players'] / srv['snapshots']).fillna(0)
        srv['server'] = srv['ip'] + ':' + srv['port'].astype(str)

        pivot = srv.pivot_table(index='date', columns='server', values='avg_contrib', aggfunc='sum')

        if pivot.shape[1] > 0:
            totals = pivot.sum(axis=0)
            # Count only buckets that have actual snapshots (ignore gaps in data collection)
            # A bucket has snapshots if there's any data at all in the pivot for that date
            buckets_with_snapshots = daily_totals_indexed.reindex(full_time_index, fill_value=0)
            num_valid_buckets = (buckets_with_snapshots > 0).sum()
            num_valid_buckets = max(1, num_valid_buckets)  # Avoid division by zero
            averages = (totals / num_valid_buckets).sort_values(ascending=False)
        else:
            averages = pd.Series([], dtype=float)

        pivot = pivot.reindex(full_time_index, fill_value=0)

        top_n = min(int(top_servers or 10), pivot.shape[1])
        if top_n > 0:
            top_servers_list = list(averages.head(top_n).index)
            server_ranking = [{ 'id': srv, 'label': srv, 'pop': round(float(averages[srv]), 2) } for srv in top_servers_list]
            if pivot.shape[1] > top_n:
                other_val = float(averages.iloc[top_n:].sum())
                if not np.isnan(other_val) and other_val > 0:
                    server_ranking.append({ 'id': 'Other', 'label': 'Other', 'pop': round(other_val, 2) })
        else:
            top_servers_list = []
            server_ranking = []

        total_players_server_datasets = []
        for idx, server in enumerate(top_servers_list):
            series = pivot[server] if server in pivot.columns else pd.Series([0]*len(pivot), index=pivot.index)
            series = series.fillna(0)
            total_players_server_datasets.append({
                'label': server,
                'data': list(series.round(percision).astype(float).values),
                'backgroundColor': get_color(idx, max(1, len(top_servers_list)), color_intensity).replace('rgb', 'rgba').replace(')', ', 0.5)'),
                'borderColor': get_color(idx, max(1, len(top_servers_list)), color_intensity),
                'fill': True,
                'stack': 'servers',
            })

        if pivot.shape[1] > len(top_servers_list):
            other_series = pivot.drop(columns=top_servers_list, errors='ignore').sum(axis=1)
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

    map_total_contrib = merged_df.groupby('map')['avg_players'].sum()
    total_contrib_sum = map_total_contrib.sum()
    ranking = []

    if total_contrib_sum > 0:
        top_maps_contrib = map_total_contrib[map_total_contrib.index.isin(top_maps)].sort_values(ascending=False)
        ranking_df = (top_maps_contrib / total_contrib_sum * 100).round(2).reset_index(name='pop')
        ranking_df.rename(columns={'map': 'label'}, inplace=True)
        ranking = ranking_df.to_dict('records')

        if appended_map_names:
            app_maps_contrib = map_total_contrib[map_total_contrib.index.isin(appended_map_names)].sort_values(ascending=False)
            app_rank_df = (app_maps_contrib / total_contrib_sum * 100).round(2).reset_index(name='pop')
            app_rank_df.rename(columns={'map': 'label'}, inplace=True)
            ranking += app_rank_df.to_dict('records')

        if not other_maps_df.empty:
            other_maps_contrib_sum = map_total_contrib[~map_total_contrib.index.isin(set(top_maps).union(set(appended_map_names)))].sum()
            if other_maps_contrib_sum > 0:
                other_pop = round((other_maps_contrib_sum / total_contrib_sum) * 100, 2)
                ranking.append({'label': 'Other', 'pop': other_pop})

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
        totals_w = pivot_w.sum(axis=0) if pivot_w.shape[1] > 0 else pd.Series([], dtype=float)
        # Count only days that have actual snapshots (ignore gaps in data collection)
        num_valid_days_w = (daily_snapshots_w > 0).sum() if len(daily_snapshots_w) > 0 else 1
        num_valid_days_w = max(1, num_valid_days_w)  # Avoid division by zero
        averages_w = (totals_w / num_valid_days_w).sort_values(ascending=False)
        top_n_w = min(10, len(averages_w))
        global_server_ranking = [{ 'label': srv, 'pop': round(float(averages_w[srv]), 2) } for srv in list(averages_w.head(top_n_w).index)]
        if len(averages_w) > top_n_w:
            other_val_w = float(averages_w.iloc[top_n_w:].sum())
            if not np.isnan(other_val_w) and other_val_w > 0:
                global_server_ranking.append({ 'label': 'Other', 'pop': round(other_val_w, 2) })
    except Exception as e:
        logging.debug(f"Failed to compute global server ranking: {e}")
        global_server_ranking = []

    def _sanitize_dataset_list(ds_list):
        out = []
        for ds in ds_list:
            s = dict(ds)
            if isinstance(s.get('data'), list):
                s['data'] = [0 if (isinstance(v, float) and (np.isnan(v))) else (float(v) if isinstance(v, (np.floating, np.integer)) else v) for v in s['data']]
            out.append(s)
        return out

    if 'server_ranking' not in locals():
        server_ranking = []

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

    # Helper to get display name
    def get_server_display_name(s_ip, s_port):
        try:
            s_port = int(s_port)
            name = server_names_map.get((s_ip, s_port))
            if name:
                return name
        except:
            pass
        return f"{s_ip}:{s_port}"

    if 'serverRanking' in result:
        for item in result['serverRanking']:
            if ':' in item['label'] and item['label'] != 'Other':
                try:
                    s_ip, s_port = item['label'].split(':', 1)
                    item['label'] = get_server_display_name(s_ip, s_port)
                except:
                    pass

    if 'totalPlayersServerDatasets' in result:
        for ds in result['totalPlayersServerDatasets']:
            if ':' in ds['label'] and ds['label'] != 'Other':
                 try:
                    s_ip, s_port = ds['label'].split(':', 1)
                    ds['label'] = get_server_display_name(s_ip, s_port)
                 except:
                    pass

    if 'globalServerRanking' in result:
        for item in result['globalServerRanking']:
             if ':' in item['label'] and item['label'] != 'Other':
                try:
                    s_ip, s_port = item['label'].split(':', 1)
                    item['label'] = get_server_display_name(s_ip, s_port)
                except:
                    pass

    logging.info(f"[Chart] Generation complete in {time.time() - _start_time:.2f}s (datasets={len(datasets)}, ranking={len(ranking)})")
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
                FROM samples_v2 sa
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
