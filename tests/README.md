# SourceMapStats Tests

## Overview

This directory contains tests for the SourceMapStats data processing and visualization logic.

## Test Files

### `advanced_math_test.py`

Comprehensive tests verifying the mathematical correctness of player statistics and ranking calculations.

## Test Cases

### 1. End-to-End Math Parity (`test_end_to_end_math_parity`)

Verifies that:
- Server contribution calculations match ground truth data
- Frontend-expected data structures are present (`labels`, `datasets`, `serverRanking`, etc.)
- Map rankings are ordered by **total contribution** (sum), not average when active
- Generator produces statistically realistic data

### 2. Single-Day Spike vs Consistent Popularity (`test_single_day_spike_vs_consistent_popularity`)

**Edge case**: Ensures maps played consistently rank higher than those with single-day spikes.

| Scenario | Players | Result |
|----------|---------|--------|
| `consistent_map`: 10 players × 30 days | 3,600 total | **Ranks higher** |
| `spike_map`: 100 players × 1 day | 1,200 total | Ranks lower |

### 3. Gaps in Data Collection (`test_gaps_in_data_ignored_for_server_ranking`)

**Edge case**: Days without any snapshots (data collection gaps) should be ignored.

| Scenario | Calculation |
|----------|-------------|
| 10 days requested, 5 days have data | avg = total / 5 ✓ |
| Wrong (counting gaps) | avg = total / 10 ✗ |

### 4. Unequal Snapshot Counts (`test_unequal_snapshot_counts_weighted_equally`)

**Edge case**: Each time bucket contributes equally regardless of snapshot count.

| Day | Snapshots | Players/Snapshot | Weight |
|-----|-----------|------------------|--------|
| Day 1 | 100 | 10 | **Equal** |
| Day 2 | 10 | 10 | **Equal** |

Both days have avg = 10, overall = 10 (not skewed by Day 1's higher snapshot count).

## Running Tests

```bash
# Run all tests
python tests/advanced_math_test.py

# Run with verbose output
python tests/advanced_math_test.py -v

# Write test data to live database for frontend visualization
python tests/advanced_math_test.py --visualize
```

## Key Implementation Details

- **Ranking uses SUM not MEAN**: Maps/servers ranked by total contribution, not average when active
- **Bucket normalization**: Each 2-hour bucket's contribution = `players / snapshots`
- **Gap handling**: Only buckets with actual snapshots count in denominator
- **Date constraints**: Frontend restricts date selection to valid snapshot range
