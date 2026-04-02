import time
import pandas as pd
import duckdb
import os
import logging

DB_FILE = 'c:/Users/uraan/Documents/SourceMapStats/sourcemapstats.duckdb'

def profile_current_approach():
    print("--- Profiling Current Approach ---")
    start_time = time.time()
    
    # Simulate params for 7 days
    days_to_show = 7
    with duckdb.connect(DB_FILE, read_only=True) as con:
        # Get max date
        row = con.execute("SELECT max(timestamp) FROM snaps").fetchone()
        max_date = row[0]
        end_date = pd.Timestamp(max_date)
        start_date = end_date - pd.Timedelta(days=days_to_show)
        
        print(f"Date range: {start_date} to {end_date}")
        
        step_start = time.time()
        # Original query
        df_window = con.execute(
            """
            SELECT s.ip, s.port, m.name as map, sa.players, sn.timestamp, s.country_code, sn.guid as snapshot_id
            FROM samples_v2 sa
            JOIN snaps sn ON sa.snapshot_id = sn.id
            JOIN servers s ON sa.server_id = s.id
            JOIN maps m ON sa.map_id = m.id
            WHERE sn.timestamp >= ? AND sn.timestamp < ?
            """,
            [start_date.to_pydatetime(), end_date.to_pydatetime()]
        ).df()
        print(f"Data fetch took: {time.time() - step_start:.4f}s ({len(df_window)} rows)")
        
        step_start = time.time()
        # Normalization
        df = df_window.copy()
        df['players'] = pd.to_numeric(df['players'], errors='coerce').fillna(0).astype(int)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
        print(f"Normalization took: {time.time() - step_start:.4f}s")
        
        step_start = time.time()
        # Aggregation (2h)
        df['date'] = df['timestamp'].dt.floor('2h')
        daily_player_sum = df.groupby(['date', 'map'])['players'].sum().reset_index()
        print(f"Aggregation in Pandas took: {time.time() - step_start:.4f}s")

def profile_optimized_sql():
    print("\n--- Profiling Optimized SQL Approach ---")
    start_time = time.time()
    
    days_to_show = 7
    with duckdb.connect(DB_FILE, read_only=True) as con:
        row = con.execute("SELECT max(timestamp) FROM snaps").fetchone()
        max_date = row[0]
        end_date = pd.Timestamp(max_date)
        start_date = end_date - pd.Timedelta(days=days_to_show)
        
        step_start = time.time()
        # Optimized query with time_bucket and aggregation
        # We need sum(players) per (map, 2h_bucket)
        # We also need total_snapshots per 2h_bucket for the entire window
        
        # 1. Total players per map per 2h bucket
        df_agg = con.execute(
            """
            SELECT 
                time_bucket(INTERVAL '2 hours', sn.timestamp) as bucket,
                m.name as map,
                SUM(sa.players) as total_players
            FROM samples_v2 sa
            JOIN snaps sn ON sa.snapshot_id = sn.id
            JOIN maps m ON sa.map_id = m.id
            WHERE sn.timestamp >= ? AND sn.timestamp < ?
            GROUP BY 1, 2
            """,
            [start_date.to_pydatetime(), end_date.to_pydatetime()]
        ).df()
        print(f"Aggregated Data fetch took: {time.time() - step_start:.4f}s ({len(df_agg)} rows)")
        
        step_start = time.time()
        # 2. Total snapshots per 2h bucket
        df_snaps = con.execute(
            """
            SELECT 
                time_bucket(INTERVAL '2 hours', timestamp) as bucket,
                COUNT(DISTINCT guid) as total_snapshots
            FROM snaps 
            WHERE timestamp >= ? AND timestamp < ?
            GROUP BY 1
            """,
            [start_date.to_pydatetime(), end_date.to_pydatetime()]
        ).df()
        print(f"Snapshots fetch took: {time.time() - step_start:.4f}s ({len(df_snaps)} rows)")

if __name__ == "__main__":
    profile_current_approach()
    profile_optimized_sql()
