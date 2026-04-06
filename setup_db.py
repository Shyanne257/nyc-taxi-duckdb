"""
setup_db.py
-----------
One-time script to initialize the DuckDB database from raw Parquet/CSV files.
Run this ONCE before launching the Streamlit app:
    python setup_db.py

This script:
  1. Creates taxi_trips table from Jan + Feb 2026 Parquet files
  2. Creates taxi_zones dimension table from the CSV lookup file
  3. Prints row counts and schema to confirm success
"""

import duckdb
import os
import sys

# ── Path configuration ────────────────────────────────────────────────────────
# Edit these paths if your data files are stored elsewhere.
PARQUET_JAN = "data/yellow_tripdata_2026-01.parquet"
PARQUET_FEB = "data/yellow_tripdata_2026-02.parquet"
ZONE_CSV    = "data/taxi_zone_lookup.csv"
DB_PATH     = "taxi.duckdb"

def check_files():
    missing = [f for f in [PARQUET_JAN, PARQUET_FEB, ZONE_CSV] if not os.path.exists(f)]
    if missing:
        print("ERROR: The following data files were not found:")
        for f in missing:
            print(f"  {f}")
        print("\nPlease place your data files in the 'data/' folder.")
        sys.exit(1)

def setup():
    check_files()

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing {DB_PATH}")

    con = duckdb.connect(DB_PATH)
    print(f"Creating database: {DB_PATH}\n")

    # ── Table 1: taxi_trips (merged Jan + Feb) ────────────────────────────────
    print("Loading taxi_trips from Parquet files...")
    con.execute(f"""
        CREATE TABLE taxi_trips AS
        SELECT
            VendorID,
            tpep_pickup_datetime                        AS pickup_datetime,
            tpep_dropoff_datetime                       AS dropoff_datetime,
            passenger_count,
            trip_distance,
            PULocationID                                AS pickup_location_id,
            DOLocationID                                AS dropoff_location_id,
            payment_type,
            fare_amount,
            tip_amount,
            tolls_amount,
            total_amount,
            congestion_surcharge,
            Airport_fee                                 AS airport_fee,
            -- Derived columns for richer analytics
            EXTRACT(hour  FROM tpep_pickup_datetime)    AS pickup_hour,
            EXTRACT(dow   FROM tpep_pickup_datetime)    AS pickup_dow,   -- 0=Sun
            EXTRACT(month FROM tpep_pickup_datetime)    AS pickup_month,
            DATE_TRUNC('day', tpep_pickup_datetime)     AS pickup_date,
            DATEDIFF('minute', tpep_pickup_datetime,
                               tpep_dropoff_datetime)   AS trip_duration_min,
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                ELSE 'Unknown'
            END                                         AS payment_label
        FROM read_parquet(['{PARQUET_JAN}', '{PARQUET_FEB}'])
        WHERE
            fare_amount      > 0
            AND trip_distance > 0
            AND total_amount  > 0
            AND tpep_pickup_datetime >= '2026-01-01'
            AND tpep_pickup_datetime <  '2026-03-01'
            AND trip_duration_min BETWEEN 1 AND 180
    """)

    n_trips = con.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
    print(f"  taxi_trips loaded: {n_trips:,} rows")

    # ── Table 2: taxi_zones (dimension / lookup) ──────────────────────────────
    print("Loading taxi_zones from CSV...")
    con.execute(f"""
        CREATE TABLE taxi_zones AS
        SELECT
            LocationID   AS location_id,
            Borough      AS borough,
            Zone         AS zone,
            service_zone
        FROM read_csv_auto('{ZONE_CSV}')
    """)

    n_zones = con.execute("SELECT COUNT(*) FROM taxi_zones").fetchone()[0]
    print(f"  taxi_zones loaded: {n_zones:,} rows")

    # ── Schema summary ────────────────────────────────────────────────────────
    print("\n── taxi_trips schema ──")
    print(con.execute("DESCRIBE taxi_trips").df().to_string(index=False))

    print("\n── taxi_zones schema ──")
    print(con.execute("DESCRIBE taxi_zones").df().to_string(index=False))

    con.close()
    print(f"\nDone. Database saved to: {DB_PATH}")

if __name__ == "__main__":
    setup()
