import time
import logging
import socket
import re
import duckdb
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import a2s
from database import (
    load_cooldowns_from_db,
    save_cooldowns_to_db,
    write_samples,
    save_server_names_to_db,
    update_replica_db,
    refresh_served_cache,
    record_snapshot,
    DB_FILE,
    ReaderTimeFormat
)
from steam_api import get_server_list
from utils import is_valid_public_ip, sanitize_server_name

# ─── Constants ────────────────────────────────────────────────────────────────
SCAN_THREADS = 100
MAX_SINGLE_IP_TIMEOUT = 5.0
BASE_SKIP_DURATION = 60
MAX_SKIP_DURATION = 600
SERVER_TIMEOUT = 2.0 # Default start timeout

# Load cooldowns on module load (or when starting the scanner)
server_cooldowns = load_cooldowns_from_db()

def IpReader(ip):
    """Query single game server, return CSV row or None."""
    ip_str, port = ip
    server_key = (ip_str, port)
    now = time.time()
    
    # Get or initialize cooldown info for this server
    cooldown = server_cooldowns.get(server_key, {
        'timeout': SERVER_TIMEOUT,
        'failures': 0,
        'skip_until': 0
    })
    
    # Skip if in cooldown period
    if now < cooldown['skip_until']:
        return None
    
    timeout = cooldown['timeout']

    try:
        info = a2s.info(ip, timeout=timeout)
        map_name = re.sub(r'[\r\n\x00-\x1F\x7F-\x9F]', '', info.map_name).strip()
        timestamp = datetime.now().strftime(ReaderTimeFormat)
        
        # Log successful connection
        # Log successful connection
        logging.debug(f"OK {ip_str}:{port} | {info.player_count} players | {map_name}")
        
        # Success! Reduce timeout and reset failure count
        server_cooldowns[server_key] = {
            'timeout': max(0.1, timeout * 0.9),
            'failures': 0,
            'skip_until': 0
        }
        return [ip_str, str(port), map_name, str(info.player_count), timestamp, sanitize_server_name(str(info.server_name))]

    except (socket.timeout, ConnectionResetError):
        failures = cooldown['failures'] + 1
        new_timeout = min(MAX_SINGLE_IP_TIMEOUT, timeout * 2)  # Double timeout on failure
        
        # Exponential backoff
        skip_duration = min(MAX_SKIP_DURATION, BASE_SKIP_DURATION * (2 ** (failures - 1)))
        skip_until = now + skip_duration
        logging.debug(f"Timeout for {ip_str}:{port} (failure #{failures}, skip for {skip_duration}s)")
        
        server_cooldowns[server_key] = {
            'timeout': new_timeout,
            'failures': failures,
            'skip_until': skip_until
        }
        return None
        
    except Exception as e:
        failures = cooldown['failures'] + 1
        new_timeout = min(MAX_SINGLE_IP_TIMEOUT, timeout * 2)
        
        skip_duration = min(MAX_SKIP_DURATION, BASE_SKIP_DURATION * (2 ** (failures - 1)))
        skip_until = now + skip_duration
        
        logging.debug(f"Error for {ip_str}:{port}: {str(e)}")
        
        server_cooldowns[server_key] = {
            'timeout': new_timeout,
            'failures': failures,
            'skip_until': skip_until
        }
        return None

def IpReaderMulti(lst, snapshot_id, snapshot_dt_str):
    """Process a list of IPs and append a snapshot_id using multiple threads."""
    out = []
    skipped = 0
    
    valid_servers = [(ip, port) for ip, port in lst if is_valid_public_ip(ip)]
    filtered_count = len(lst) - len(valid_servers)
    if filtered_count > 0:
        logging.info(f"Filtered out {filtered_count} invalid/link-local addresses")
    
    with ThreadPoolExecutor(max_workers=SCAN_THREADS) as executor:
        # We don't pass the timestamp to IpReader to avoid changing its signature too much,
        # instead we just append it here since IpReader returns [ip, port, map, players, <old_timestamp>, name]
        futures = {executor.submit(IpReader, ip): ip for ip in valid_servers}
        
        for future in futures:
            try:
                row = future.result()
                if row:
                    # row structure from IpReader: [ip, port, map, players, timestamp, server_name]
                    # We OVERWRITE the timestamp with our unified snapshot timestamp
                    row[4] = snapshot_dt_str
                    
                    # Append snapshot_id
                    out.append(row + [snapshot_id])
                else:
                    ip_tuple = futures[future]
                    if server_cooldowns.get(ip_tuple, {}).get('skip_until', 0) > time.time():
                        skipped += 1
            except Exception as e:
                logging.error(f"Thread error: {e}")

    if skipped > 0:
        logging.info(f"Skipped {skipped} servers in cooldown")
    
    return out

def scan_loop():
    """The main loop for continuously scanning servers."""
    logging.info("--- Starting scan_loop ---")
    
    logging.info("Initializing served cache from existing data...")
    refresh_served_cache()
    
    while True:
        now_dt = datetime.now()
        snapshot_id = now_dt.strftime('%Y%m%d%H%M%S')
        # Use the same timestamp for all records in this snapshot
        snapshot_dt_str = now_dt.strftime(ReaderTimeFormat) 
        
        logging.info(f"Starting new scan cycle with snapshot_id: {snapshot_id}")
        
        server_list = get_server_list()
        if not server_list:
            logging.warning("Server list is empty. Skipping this scan cycle.")
            time.sleep(60)
            continue

        # Also scan IPs seen in the DB recently
        try:
            with duckdb.connect(DB_FILE) as con:
                cutoff = (datetime.now() - pd.Timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
                rows = con.execute(
                    "SELECT DISTINCT ip, port FROM samples WHERE timestamp >= ?",
                    [cutoff]
                ).fetchall()
                
                added_count = 0
                existing_set = set(server_list)
                for r_ip, r_port in rows:
                    if (r_ip, r_port) not in existing_set:
                        try:
                            server_list.append((r_ip, int(r_port)))
                            existing_set.add((r_ip, int(r_port)))
                            added_count += 1
                        except:
                            pass
                
                if added_count > 0:
                    logging.info(f"Added {added_count} recent servers from DB to the scan list.")
        except Exception as e:
            logging.warning(f"Failed to fetch recent IPs from DB: {e}")

        results = IpReaderMulti(server_list, snapshot_id, snapshot_dt_str)
        write_samples(results)
        record_snapshot(snapshot_id, snapshot_dt_str) # Mark this snapshot as completed (even if empty)
        save_server_names_to_db(results)
        
        save_cooldowns_to_db(server_cooldowns)
        
        update_replica_db()
        
        refresh_served_cache()
        
        logging.info(f"Scan cycle complete. Wrote {len(results)} rows. Waiting for next cycle.")
        time.sleep(300)
