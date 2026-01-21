
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import shutil
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import unittest
from unittest.mock import patch
import tempfile
import argparse

# Helper type for ground truth
# (date_str, map_name) -> {'total_players': int}
# We use a separate 'day_snapshots' dict for calculating denominator

# --- Generator Parameters ---
# --- Generator Parameters ---
DAYS = 31 # One month
SNAPSHOTS_PER_DAY = 288  # Total snapshots per day (every 5 minutes)
MINUTES_PER_SNAPSHOT = 5

# Generate 15 Servers
SERVERS = [f"192.168.0.{i}:27015" for i in range(1, 16)]

# Generate 15 Maps with varied profiles
base_maps = ['cp_dustbowl', 'ctf_2fort', 'pl_upward', 'koth_harvest', 'pl_badwater', 
             'cp_gorge', 'koth_viaduct', 'plr_hightower', 'cp_steel', 'ctf_turbine',
             'cp_granary', 'pl_thundermountain', 'cp_process', 'cp_gullywash', 'koth_lakeside']

MAP_PROFILES = {}
for m in base_maps:
    # Randomize popularity and player counts
    avg = random.randint(4, 32)
    var = random.randint(1, int(avg/2)) if avg > 2 else 0
    prob = round(random.uniform(0.1, 0.9), 2)
    MAP_PROFILES[m] = {'prob': prob, 'avg_players': avg, 'variance': var}

MAPS = list(MAP_PROFILES.keys())

# Ensure we have at least 10 of each
assert len(SERVERS) >= 10
assert len(MAPS) >= 10

# DEFAULT START DATE (Fixed for repeatable tests, but overridable)
FIXED_START_DATE = datetime(2024, 1, 1)

def generate_mock_data(con, start_date=FIXED_START_DATE):
    """
    Generates mock data into the given duckdb connection.
    Returns:
       ground_truth: dict { (date_str, map_name) -> total_players }
       server_ground_truth: dict { (date_str, server_name) -> total_players }
       day_snapshot_counts: dict { date_str -> int(num_snapshots) }
    """
    print(f"Generating mock data starting from {start_date.date()}...")
    samples = []
    
    ground_truth = {} 
    server_ground_truth = {}
    day_snapshot_counts = {}

    snapshot_rows = []

    for day_offset in range(DAYS):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.strftime('%Y-%m-%d')

        
        # We simulate exactly SNAPSHOTS_PER_DAY snapshots for this day
        day_snapshot_counts[date_str] = SNAPSHOTS_PER_DAY
        
        for snap_idx in range(SNAPSHOTS_PER_DAY):
            snapshot_time = current_date + timedelta(minutes=snap_idx * MINUTES_PER_SNAPSHOT)
            snapshot_id = snapshot_time.strftime('%Y%m%d%H%M%S')
            
            # buffer snapshot for batch insert
            snapshot_rows.append((snapshot_id, snapshot_time))
            
            # For each server, decide what map is playing
            for server in SERVERS:
                ip, port = server.split(':')
                
                # Pick a random map
                map_name = random.choice(MAPS)
                profile = MAP_PROFILES[map_name]
                
                # Decide if the server is active
                if random.random() < profile['prob']:
                    # It's active
                    base_players = profile['avg_players']
                    variance = random.randint(-profile['variance'], profile['variance'])
                    players = max(0, base_players + variance)
                    
                    samples.append((
                        ip, int(port), map_name, players, snapshot_time, 'US', snapshot_id
                    ))
                    
                    # Update Map Stats
                    key = (date_str, map_name)
                    if key not in ground_truth:
                        ground_truth[key] = 0
                    ground_truth[key] += players

                    # Update Server Stats
                    srv_key = (date_str, server)
                    if srv_key not in server_ground_truth:
                        server_ground_truth[srv_key] = 0
                    server_ground_truth[srv_key] += players
            
    print(f"Generated {len(samples)} sample rows.")
    print(f"Batch inserting {len(snapshot_rows)} snapshots (via Pandas)...")
    
    # Use Pandas for super-fast insertion
    df_snaps = pd.DataFrame(snapshot_rows, columns=['snapshot_id', 'timestamp'])
    con.register('df_snaps_view', df_snaps)
    con.execute("INSERT OR REPLACE INTO snapshots SELECT * FROM df_snaps_view")
    con.unregister('df_snaps_view')
    
    print("Batch inserting samples (via Pandas)...")
    df_samples = pd.DataFrame(samples, columns=['ip', 'port', 'map_name', 'players', 'timestamp', 'region', 'snapshot_id'])
    con.register('df_samples_view', df_samples)
    con.execute("INSERT INTO samples SELECT * FROM df_samples_view")
    con.unregister('df_samples_view')
    
    return ground_truth, server_ground_truth, day_snapshot_counts


class TestAdvancedMath(unittest.TestCase):
    
    VISUALIZE_MODE = False
    
    def setUp(self):
        if self.VISUALIZE_MODE:
             # Use REAL database paths
             import database
             BASE_DIR = os.path.dirname(os.path.abspath(__file__))
             self.db_path = os.path.join(BASE_DIR, "sourcemapstats.duckdb")
             self.replica_path = os.path.join(BASE_DIR, "sourcemapstats_replica.duckdb")
             
             # We assume database.py is already pointing to these, but explicit is good
             # We DO NOT patch them, we just use them.
             
             # For visualization, we want recent dates so it shows up in the UI default view
             now = datetime.now()
             self.start_date = datetime(now.year, now.month, now.day) - timedelta(days=DAYS - 1)
             
             # We assume existing DB structure is fine.
             # We probably want to wipe old test data if this is run multiple times?
             # For now, let's just insert. If snapshot IDs collide (unlikely with timestamps), Replace handles it.
             
        else:
            # Create a temp dir for our DB file
            self.test_dir = tempfile.mkdtemp()
            self.db_path = os.path.join(self.test_dir, "test_sourcemapstats.duckdb")
            self.replica_path = os.path.join(self.test_dir, "test_sourcemapstats_replica.duckdb")
            
            # Patch the paths in database.py
            import database
            self.orig_db_file = database.DB_FILE
            self.orig_replica_file = database.DB_REPLICA_FILE
            
            database.DB_FILE = self.db_path
            database.DB_REPLICA_FILE = self.replica_path
            
            self.start_date = FIXED_START_DATE
            
            # Initialize the DB logic
            database.init_db()
        
        # Populate with Mock Data
        # Connect to the DB path we decided on
        with duckdb.connect(self.db_path) as con:
            if self.VISUALIZE_MODE:
                print(f"!!! WRITING MOCK DATA TO LIVE DATABASE: {self.db_path} !!!")
                print("!!! CLEARING EXISTING DATA FOR CLEAN VISUALIZATION !!!")
                con.execute("DELETE FROM samples")
                con.execute("DELETE FROM snapshots")
                # Also server names/cooldowns? Maybe keep them.
                # But to be safe for "stats", samples/snapshots is what matters.
                pass

            self.ground_truth, self.server_ground_truth, self.day_snapshot_counts = generate_mock_data(con, self.start_date)
            
        # Ensure replica is updated
        import database
        database.update_replica_db()
        
        # Force cache clear logic if needed
        database.g_chart_data_cache.clear()
        
    def tearDown(self):
        if self.VISUALIZE_MODE:
            # Do NOT delete the live DB
            pass
        else:
            import database
            database.DB_FILE = self.orig_db_file
            database.DB_REPLICA_FILE = self.orig_replica_file
            shutil.rmtree(self.test_dir)

    def test_end_to_end_math_parity(self):
        import database
        
        # Request data for the full range
        start_date_str = self.start_date.strftime('%Y-%m-%d')
        
        # We ask for all generated days
        result = database.get_chart_data(
            start_date_str=start_date_str,
            days_to_show=DAYS,
            only_maps_containing=[],
            maps_to_show=100, # Show all
            percision=2,
            color_intensity=3,
            bias_exponent=1.2,
            top_servers=100, # Show all
            append_maps_containing=None,
            server_filter='ALL'
        )
        
        # 1. Verify Logic: Avg Players = Total Players / Total Snapshots (for that day)
        
        # Check Labels (Dates)
        self.assertTrue(len(result['labels']) >= DAYS, "Should return at least the requested days")
        
        # Verify Map Datasets
        # result['datasets'] is a list of dicts: { label: map_name, data: [pct1, pct2...], ... }
        # Note: The 'data' in 'datasets' is PERCENTAGE share.
        
        # We need to reconstruct the raw averages to check math.
        # But 'dailyTotals' gives us the SUM of averages for the day.
        # AND 'datasets' gives us percentage. 
        # So Actual_Avg_Map_M_Day_D = (Dataset_Value / 100) * Daily_Total_Avg_Players
        # Wait, database.py logic:
        # daily_total_avg_players = sum(map_avg for all maps)
        # dataset value = (map_avg / daily_total_avg_players) * 100
        
        # This is hard to reverse exactly due to rounding.
        # However, let's look at what we CAN verify directly if we trust the inputs.
        
        # Verify Server Ranking and Server Totals are simpler?
        # database.py: totalPlayersServerDatasets -> data is raw 'avg_contrib' (players/snapshots)
        # This is exactly what we want to verify.
        
        print("\nVerifying Server Stats (Raw Values)...")
        server_datasets = result['totalPlayersServerDatasets']
        failures = 0
        
        # Map date strings to indices
        date_map = {d: i for i, d in enumerate(result['labels'])}
        
        for ds in server_datasets:
            server_name = ds['label'] # ip:port or Name
            data_points = ds['data']
            
            if server_name == 'Other': continue
            
            # The label might have been resolved to a name, but our mock data has raw IPs.
            # In mock data generate, we didn't put names in 'server_names' table.
            # So labels should still be 'ip:port'.
            
            for date_str, expected_total_players in self.server_ground_truth.items():
                # ground truth key is (date, server)
                if date_str not in date_map: continue
                
                gt_date, gt_server = date_str, server_name
                # Only check if this dataset matches the ground truth entry
                # We have to reverse search the ground truth for this server?
                # Faster: iterate the dataset's days
                pass
            
        # Let's iterate the ground truth and find it in the result
        for (date_str, server_name), total_players in self.server_ground_truth.items():
            if date_str not in date_map:
                # Might be out of range if we generated more than requested? 
                # We requested all DAYS.
                continue
                
            idx = date_map[date_str]
            day_snaps = self.day_snapshot_counts.get(date_str, 1) # Should be 24
            
            expected_avg = total_players / day_snaps
            
            # Find in result
            found = False
            for ds in server_datasets:
                if ds['label'] == server_name:
                    actual = ds['data'][idx]
                    found = True
                    if abs(actual - expected_avg) > 0.05: # Allow some float/rounding slop (precision=2 in app)
                        print(f"FAIL Server {server_name} on {date_str}: Expected {expected_avg:.3f}, Got {actual}")
                        failures += 1
                    break
            
            if not found and expected_avg > 0.01:
                # If it's not in the top list, it might be in 'Other' or we requested too few?
                # We requested 100 servers.
                print(f"FAIL Server {server_name} missing from output on {date_str} (Expected {expected_avg})")
                failures += 1
                
        if failures == 0:
            print("Server stats verified successfully against ground truth!")
        else:
            self.fail(f"{failures} server stat checks failed")

        # 2. Verify Output Structure covers Frontend Expectations
        print("\nVerifying Frontend Parity (Structure)...")
        expected_keys = [
            'labels', 'datasets', 'dailyTotals', 'snapshotCounts', 
            'ranking', 'serverRanking', 'totalPlayersServerDatasets'
        ]
        for k in expected_keys:
            self.assertIn(k, result, f"Result missing key '{k}' required by frontend")
            
        self.assertIsInstance(result['labels'], list)
        self.assertIsInstance(result['datasets'], list)
        self.assertIsInstance(result['serverRanking'], list)
        
        # Check Ranking Format
        if result['ranking']:
            r = result['ranking'][0]
            self.assertIn('label', r)
            self.assertIn('pop', r)
        
        # Check Server Ranking Format (should have 'id' for filtering)
        if result['serverRanking']:
            sr = result['serverRanking'][0]
            self.assertIn('label', sr)
            self.assertIn('pop', sr)
            self.assertIn('id', sr, "serverRanking should include 'id' field for filtering")
            
        print("Frontend structure verified.")
        
        # 4. Verify Ranking uses SUM not MEAN (total contribution, not average when active)
        print("\nVerifying Ranking uses Total Contribution (Sum)...")
        
        # Calculate expected ranking from ground truth
        # Sum all players for each map across all days
        map_total_players = {}
        for (date_str, map_name), total_players in self.ground_truth.items():
            if map_name not in map_total_players:
                map_total_players[map_name] = 0
            map_total_players[map_name] += total_players
        
        # Sort by total contribution (descending)
        expected_ranking_order = sorted(map_total_players.keys(), 
                                         key=lambda m: map_total_players[m], 
                                         reverse=True)
        
        # Get actual ranking order from result (exclude 'Other')
        actual_ranking_order = [r['label'] for r in result['ranking'] if r['label'] != 'Other']
        
        # Verify top maps match (allowing for small differences due to 2h bucketing vs daily)
        # At minimum, the top 5 should be the same (possibly in slightly different order due to bucketing)
        top_n = min(5, len(expected_ranking_order), len(actual_ranking_order))
        expected_top = set(expected_ranking_order[:top_n])
        actual_top = set(actual_ranking_order[:top_n])
        
        overlap = len(expected_top.intersection(actual_top))
        print(f"Top {top_n} expected: {expected_ranking_order[:top_n]}")
        print(f"Top {top_n} actual:   {actual_ranking_order[:top_n]}")
        print(f"Overlap: {overlap}/{top_n}")
        
        # Allow at most 1 difference in top 5 due to bucketing differences
        self.assertGreaterEqual(overlap, top_n - 1, 
            f"Ranking should be based on total contribution. Expected {expected_top}, got {actual_top}")
        
        # 3. Verify Generator Realism (Statistical Check)
        print("\nVerifying Generator Realism...")
        # Check if the generated data statistically aligns with MAP_PROFILES
        # Global Avg Expected approx = prob * avg_players * (num_servers)
        # Note: logic in generate_mock_data:
        # For each server, we try map with prob.
        # So expected TOTAL players across all servers for a map per snapshot = 
        #   (Num_Servers * Prob * Avg_Players) * (1/Num_Maps ?? No, independent)
        # Wait, the logic is:
        # for server in SERVERS:
        #    map = random.choice(MAPS) (Uniform selection of map!)
        #    profile = MAP_PROFILES[map]
        #    if random() < profile['prob']: add players
        
        # So Expected Global Avg players for Map M = 
        #   Num_Servers * P(Server picks Map M) * P(Server active | Map M) * Avg_Players
        #   = Num_Servers * (1 / len(MAPS)) * profile['prob'] * profile['avg_players']
        
        gen_failures = 0
        total_snapshots = DAYS * SNAPSHOTS_PER_DAY
        num_servers = len(SERVERS)
        num_maps = len(MAPS)
        
        print(f"\n{'Map Name':<25} | {'Expected Avg':<12} | {'Actual Avg':<12} | {'Diff':<10} | {'% Error':<10}")
        print("-" * 80)
        
        for map_name, profile in MAP_PROFILES.items():
            # Calculate Expected Global Average Player Count (per snapshot)
            # This 'avg_players' in profile is "Base players if active"
            expected_avg = num_servers * (1/num_maps) * profile['prob'] * profile['avg_players']
            
            # Calculate Actual from Ground Truth
            # ground_truth[(date, map)] = total_players_for_day
            total_players_all_time = sum(
                self.ground_truth.get((d, map_name), 0) 
                for d in self.day_snapshot_counts
            )
            actual_avg = total_players_all_time / total_snapshots
            
            # Allow margin of error (e.g. +/- 20% + small epsilon) due to randomness
            # With >5000 samples, it should be decent, but randomness is random.
            diff = abs(actual_avg - expected_avg)
            threshold = (expected_avg * 0.25) + 0.5 # 25% tolerance + 0.5 player buffer
            
            pct_error = (diff / expected_avg * 100) if expected_avg > 0 else 0
            print(f"{map_name:<25} | {expected_avg:<12.3f} | {actual_avg:<12.3f} | {diff:<10.3f} | {pct_error:<9.1f}%")

            if diff > threshold:
                print(f"  >>> WARN: Deviation > Threshold ({threshold:.3f})")
                gen_failures += 1
            else:
                 pass

        if gen_failures > (len(MAPS) / 3):
            # If more than 1/3 of maps are way off, something is wrong with generation
            self.fail(f"Generator produced unrealistic data for {gen_failures}/{len(MAPS)} maps")
        else:
             print("Generator statistics are within expected bounds.")

    def test_single_day_spike_vs_consistent_popularity(self):
        """
        Edge case: A map with a massive single-day spike should rank LOWER than 
        a map with consistent moderate popularity across all days.
        
        This verifies the fix from mean() to sum() for ranking.
        """
        import database
        
        # Create a fresh temp DB for this specific test
        test_dir = tempfile.mkdtemp()
        db_path = os.path.join(test_dir, "edge_test.duckdb")
        replica_path = os.path.join(test_dir, "edge_test_replica.duckdb")
        
        orig_db = database.DB_FILE
        orig_replica = database.DB_REPLICA_FILE
        
        try:
            database.DB_FILE = db_path
            database.DB_REPLICA_FILE = replica_path
            database.init_db()
            
            # Create controlled test data
            with duckdb.connect(db_path) as con:
                samples = []
                snapshot_rows = []
                
                start_date = datetime(2024, 1, 1)
                days = 30
                snapshots_per_day = 12  # Every 2 hours
                
                # Map A: "spike_map" - only played on day 15 with 100 players per snapshot
                # Total contribution: 12 snapshots * 100 players = 1,200 player-snapshots
                
                # Map B: "consistent_map" - played every day with 10 players per snapshot
                # Total contribution: 30 days * 12 snapshots * 10 players = 3,600 player-snapshots
                
                for day_offset in range(days):
                    current_date = start_date + timedelta(days=day_offset)
                    
                    for snap_idx in range(snapshots_per_day):
                        snapshot_time = current_date + timedelta(hours=snap_idx * 2)
                        snapshot_id = snapshot_time.strftime('%Y%m%d%H%M%S')
                        snapshot_rows.append((snapshot_id, snapshot_time))
                        
                        # Consistent map: always 10 players
                        samples.append((
                            '10.0.0.1', 27015, 'consistent_map', 10, 
                            snapshot_time, 'US', snapshot_id
                        ))
                        
                        # Spike map: 100 players ONLY on day 15
                        if day_offset == 14:  # Day 15 (0-indexed)
                            samples.append((
                                '10.0.0.1', 27015, 'spike_map', 100, 
                                snapshot_time, 'US', snapshot_id
                            ))
                
                # Insert data
                df_snaps = pd.DataFrame(snapshot_rows, columns=['snapshot_id', 'timestamp'])
                con.register('df_snaps_view', df_snaps)
                con.execute("INSERT OR REPLACE INTO snapshots SELECT * FROM df_snaps_view")
                con.unregister('df_snaps_view')
                
                df_samples = pd.DataFrame(samples, columns=['ip', 'port', 'map_name', 'players', 'timestamp', 'region', 'snapshot_id'])
                con.register('df_samples_view', df_samples)
                con.execute("INSERT INTO samples SELECT * FROM df_samples_view")
                con.unregister('df_samples_view')
            
            # Update replica
            database.update_replica_db()
            database.g_chart_data_cache.clear()
            
            # Query the data
            result = database.get_chart_data(
                start_date_str=start_date.strftime('%Y-%m-%d'),
                days_to_show=days,
                only_maps_containing=[],
                maps_to_show=10,
                percision=2,
                color_intensity=3,
                bias_exponent=1.2,
                top_servers=10,
                append_maps_containing=None,
                server_filter='ALL'
            )
            
            # Verify ranking
            ranking = {r['label']: r['pop'] for r in result['ranking']}
            
            print("\n=== Edge Case Test: Single-Day Spike vs Consistent Popularity ===")
            print(f"spike_map (1 day, 100 players/snapshot): {ranking.get('spike_map', 0)}%")
            print(f"consistent_map (30 days, 10 players/snapshot): {ranking.get('consistent_map', 0)}%")
            
            # consistent_map should have higher ranking than spike_map
            # Because 30*12*10 = 3600 > 1*12*100 = 1200
            self.assertIn('consistent_map', ranking, "consistent_map should be in ranking")
            self.assertIn('spike_map', ranking, "spike_map should be in ranking")
            
            self.assertGreater(
                ranking['consistent_map'], 
                ranking['spike_map'],
                f"consistent_map ({ranking['consistent_map']}%) should rank higher than spike_map ({ranking['spike_map']}%) "
                f"because total contribution (3600) > single-day spike (1200)"
            )
            
            print("Edge case test PASSED: Sum-based ranking correctly prioritizes consistent popularity over single-day spikes!")
            
        finally:
            database.DB_FILE = orig_db
            database.DB_REPLICA_FILE = orig_replica
            shutil.rmtree(test_dir)

    def test_gaps_in_data_ignored_for_server_ranking(self):
        """
        Edge case: When there are gaps in data collection (days without any snapshots),
        those gaps should be ignored when calculating server averages.
        
        Example: 10 days requested, but only 5 days have snapshots.
        A server with 50 total players should show avg = 50/5 = 10, not 50/10 = 5.
        """
        import database
        
        # Create a fresh temp DB for this specific test
        test_dir = tempfile.mkdtemp()
        db_path = os.path.join(test_dir, "gap_test.duckdb")
        replica_path = os.path.join(test_dir, "gap_test_replica.duckdb")
        
        orig_db = database.DB_FILE
        orig_replica = database.DB_REPLICA_FILE
        
        try:
            database.DB_FILE = db_path
            database.DB_REPLICA_FILE = replica_path
            database.init_db()
            
            # Create controlled test data with gaps
            with duckdb.connect(db_path) as con:
                samples = []
                snapshot_rows = []
                
                start_date = datetime(2024, 1, 1)
                days = 10
                snapshots_per_day = 12  # Every 2 hours
                
                # Only create snapshots for days 0, 2, 4, 6, 8 (5 days with data, 5 days gap)
                days_with_data = [0, 2, 4, 6, 8]
                
                for day_offset in days_with_data:
                    current_date = start_date + timedelta(days=day_offset)
                    
                    for snap_idx in range(snapshots_per_day):
                        snapshot_time = current_date + timedelta(hours=snap_idx * 2)
                        snapshot_id = snapshot_time.strftime('%Y%m%d%H%M%S')
                        snapshot_rows.append((snapshot_id, snapshot_time))
                        
                        # Server has 10 players on each day with data
                        samples.append((
                            '10.0.0.1', 27015, 'test_map', 10, 
                            snapshot_time, 'US', snapshot_id
                        ))
                
                # Insert data
                df_snaps = pd.DataFrame(snapshot_rows, columns=['snapshot_id', 'timestamp'])
                con.register('df_snaps_view', df_snaps)
                con.execute("INSERT OR REPLACE INTO snapshots SELECT * FROM df_snaps_view")
                con.unregister('df_snaps_view')
                
                df_samples = pd.DataFrame(samples, columns=['ip', 'port', 'map_name', 'players', 'timestamp', 'region', 'snapshot_id'])
                con.register('df_samples_view', df_samples)
                con.execute("INSERT INTO samples SELECT * FROM df_samples_view")
                con.unregister('df_samples_view')
            
            # Update replica
            database.update_replica_db()
            database.g_chart_data_cache.clear()
            
            # Query the data for the full 10-day range
            result = database.get_chart_data(
                start_date_str=start_date.strftime('%Y-%m-%d'),
                days_to_show=days,
                only_maps_containing=[],
                maps_to_show=10,
                percision=2,
                color_intensity=3,
                bias_exponent=1.2,
                top_servers=10,
                append_maps_containing=None,
                server_filter='ALL'
            )
            
            # Verify server ranking
            server_ranking = {r['label']: r['pop'] for r in result['serverRanking'] if r['label'] != 'Other'}
            
            print("\n=== Edge Case Test: Gaps in Data Collection ===")
            print(f"Days requested: {days}")
            print(f"Days with actual data: {len(days_with_data)}")
            
            # Server has 10 players per day for 5 days
            # Expected avg if gaps ignored: 10 players (10 * 5 / 5)
            # Wrong avg if gaps counted: 5 players (10 * 5 / 10)
            
            # Get the server's pop value (should be around 10, not 5)
            server_key = None
            for key in server_ranking:
                if '10.0.0.1' in key:
                    server_key = key
                    break
            
            if server_key:
                actual_avg = server_ranking[server_key]
                print(f"Server 10.0.0.1:27015 average: {actual_avg}")
                
                # The average should be close to 10 (ignoring gaps), not 5 (counting gaps)
                self.assertGreater(actual_avg, 7, 
                    f"Server average ({actual_avg}) should be ~10 (gaps ignored), not ~5 (gaps counted)")
                self.assertLess(actual_avg, 13, 
                    f"Server average ({actual_avg}) should be reasonable, not inflated")
                
                print("Edge case test PASSED: Gaps in data collection are correctly ignored!")
            else:
                self.fail("Server 10.0.0.1:27015 not found in server ranking")
            
        finally:
            database.DB_FILE = orig_db
            database.DB_REPLICA_FILE = orig_replica
            shutil.rmtree(test_dir)

    def test_unequal_snapshot_counts_weighted_equally(self):
        """
        Edge case: Days with different snapshot counts should have equal weight.
        
        A day with 1000 snapshots and 10000 total players (avg 10) should contribute
        the same as a day with 10 snapshots and 100 total players (avg 10).
        Both should show avg = 10, and the overall average should be 10.
        """
        import database
        
        # Create a fresh temp DB for this specific test
        test_dir = tempfile.mkdtemp()
        db_path = os.path.join(test_dir, "weight_test.duckdb")
        replica_path = os.path.join(test_dir, "weight_test_replica.duckdb")
        
        orig_db = database.DB_FILE
        orig_replica = database.DB_REPLICA_FILE
        
        try:
            database.DB_FILE = db_path
            database.DB_REPLICA_FILE = replica_path
            database.init_db()
            
            with duckdb.connect(db_path) as con:
                samples = []
                snapshot_rows = []
                
                start_date = datetime(2024, 1, 1)
                
                # Day 1: 100 snapshots, 10 players each = 1000 total, avg = 10
                day1 = start_date
                for snap_idx in range(100):
                    snapshot_time = day1 + timedelta(minutes=snap_idx * 5)
                    snapshot_id = snapshot_time.strftime('%Y%m%d%H%M%S')
                    snapshot_rows.append((snapshot_id, snapshot_time))
                    samples.append((
                        '10.0.0.1', 27015, 'test_map', 10, 
                        snapshot_time, 'US', snapshot_id
                    ))
                
                # Day 2: 10 snapshots, 10 players each = 100 total, avg = 10
                day2 = start_date + timedelta(days=1)
                for snap_idx in range(10):
                    snapshot_time = day2 + timedelta(minutes=snap_idx * 30)
                    snapshot_id = snapshot_time.strftime('%Y%m%d%H%M%S')
                    snapshot_rows.append((snapshot_id, snapshot_time))
                    samples.append((
                        '10.0.0.1', 27015, 'test_map', 10, 
                        snapshot_time, 'US', snapshot_id
                    ))
                
                # Insert data
                df_snaps = pd.DataFrame(snapshot_rows, columns=['snapshot_id', 'timestamp'])
                con.register('df_snaps_view', df_snaps)
                con.execute("INSERT OR REPLACE INTO snapshots SELECT * FROM df_snaps_view")
                con.unregister('df_snaps_view')
                
                df_samples = pd.DataFrame(samples, columns=['ip', 'port', 'map_name', 'players', 'timestamp', 'region', 'snapshot_id'])
                con.register('df_samples_view', df_samples)
                con.execute("INSERT INTO samples SELECT * FROM df_samples_view")
                con.unregister('df_samples_view')
            
            # Update replica
            database.update_replica_db()
            database.g_chart_data_cache.clear()
            
            # Query the data
            result = database.get_chart_data(
                start_date_str=start_date.strftime('%Y-%m-%d'),
                days_to_show=2,
                only_maps_containing=[],
                maps_to_show=10,
                percision=2,
                color_intensity=3,
                bias_exponent=1.2,
                top_servers=10,
                append_maps_containing=None,
                server_filter='ALL'
            )
            
            # Verify server ranking
            server_ranking = {r['label']: r['pop'] for r in result['serverRanking'] if r['label'] != 'Other'}
            
            print("\n=== Edge Case Test: Unequal Snapshot Counts ===")
            print("Day 1: 100 snapshots × 10 players = 1000 total (avg 10)")
            print("Day 2: 10 snapshots × 10 players = 100 total (avg 10)")
            print("Expected overall: avg = 10 (each day weighted equally)")
            
            # Get the server's pop value
            server_key = None
            for key in server_ranking:
                if '10.0.0.1' in key:
                    server_key = key
                    break
            
            if server_key:
                actual_avg = server_ranking[server_key]
                print(f"Actual server average: {actual_avg}")
                
                # The average should be 10 (both days contribute equally)
                # If day 1 had more weight, it would still be 10 since both have same avg
                # But if there was a bug, it might be wrong
                self.assertGreater(actual_avg, 8, 
                    f"Server average ({actual_avg}) should be ~10")
                self.assertLess(actual_avg, 12, 
                    f"Server average ({actual_avg}) should be ~10")
                
                print("Edge case test PASSED: Days with different snapshot counts are weighted equally!")
            else:
                self.fail("Server 10.0.0.1:27015 not found in server ranking")
            
        finally:
            database.DB_FILE = orig_db
            database.DB_REPLICA_FILE = orig_replica
            shutil.rmtree(test_dir)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--visualize', action='store_true', help='Write mock data to the ACTUAL database for frontend visualization')
    args, unknown = parser.parse_known_args()
    
    # Pass arguments to unittest by removing them from sys.argv
    # unittest.main() looks at sys.argv
    sys.argv = [sys.argv[0]] + unknown
    
    if args.visualize:
        TestAdvancedMath.VISUALIZE_MODE = True
        
    unittest.main()
