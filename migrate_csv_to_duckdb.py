#!/usr/bin/env python3
"""
Standalone migration script to import legacy CSV data into DuckDB.

Default assumptions:
- CSV has no header row
- Column order: ip, port, map, players, timestamp, country_code, snapshot_id
- Timestamp format: %Y-%m-%d-%H:%M:%S (e.g., 2025-04-05-09:39:40)

Usage examples:
  python migrate_csv_to_duckdb.py                             # uses output.csv -> sourcemapstats.duckdb
  python migrate_csv_to_duckdb.py --csv example_output.csv    # import a specific CSV file
  python migrate_csv_to_duckdb.py --db mydata.duckdb          # import into a specific database file
  python migrate_csv_to_duckdb.py --truncate                  # clear table before importing
  python migrate_csv_to_duckdb.py --delete-csv-after          # delete CSV after successful import

This script is idempotent only if your CSV contains unique rows. No de-duplication is applied.
"""
import argparse
import os
import sys
import duckdb

DEFAULT_CSV = "output.csv"
DEFAULT_DB = "sourcemapstats.duckdb"
TABLE_DDL = (
    """
    CREATE TABLE IF NOT EXISTS samples (
        ip TEXT,
        port INTEGER,
        map TEXT,
        players INTEGER,
        timestamp TIMESTAMP,
        country_code TEXT,
        snapshot_id TEXT
    )
    """
)

INSERT_SQL = (
    """
    INSERT INTO samples (ip, port, map, players, timestamp, country_code, snapshot_id)
    SELECT
        ip,
        try_cast(port AS INTEGER) as port,
        map,
        try_cast(players AS INTEGER) as players,
        strptime(timestamp_str, '%Y-%m-%d-%H:%M:%S') as timestamp,
        country_code,
        snapshot_id
    FROM read_csv(
        ?,
        columns={'ip':'VARCHAR','port':'VARCHAR','map':'VARCHAR','players':'VARCHAR','timestamp_str':'VARCHAR','country_code':'VARCHAR','snapshot_id':'VARCHAR'},
        delim=',',
        header=false,
        quote='"',
        escape='"',
        sample_size=-1,
        nullstr=['N/A']
    )
    """
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Migrate CSV data into DuckDB database")
    p.add_argument("--csv", dest="csv_path", default=DEFAULT_CSV, help=f"Path to CSV file (default: {DEFAULT_CSV})")
    p.add_argument("--db", dest="db_path", default=DEFAULT_DB, help=f"Path to DuckDB database file (default: {DEFAULT_DB})")
    p.add_argument("--truncate", action="store_true", help="Truncate the 'samples' table before import")
    p.add_argument("--delete-csv-after", action="store_true", help="Delete the CSV after successful import")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    csv_path = os.path.abspath(args.csv_path)
    db_path = os.path.abspath(args.db_path)

    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        return 1

    print(f"CSV: {csv_path}")
    print(f"DB : {db_path}")

    try:
        with duckdb.connect(db_path) as con:
            con.execute(TABLE_DDL)

            if args.truncate:
                print("Truncating table 'samples'...")
                con.execute("DELETE FROM samples")

            before = con.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
            print(f"Rows in 'samples' before import: {before}")

            print("Importing CSV into DuckDB (this may take a moment)...")
            con.execute(INSERT_SQL, [csv_path])

            after = con.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
            inserted = after - before
            print(f"Rows inserted: {inserted}")
            print(f"Rows in 'samples' after import: {after}")

    except Exception as e:
        print(f"Migration failed: {e}")
        return 2

    if args.delete_csv_after:
        try:
            os.remove(csv_path)
            print(f"Deleted CSV: {csv_path}")
        except Exception as e:
            print(f"Imported, but failed to delete CSV: {e}")
            return 3

    print("Migration completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
