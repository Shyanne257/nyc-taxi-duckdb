# NYC Taxi · DuckDB Internals Explorer
### DSCI 551 Course Project — Chenyu Zuo (USC ID: 2933-8178-16)

---

## Project Overview

This application is an analytical dashboard built on **DuckDB**, demonstrating how DuckDB's
**vectorized execution engine** processes analytical queries over ~7 million NYC Yellow Taxi
trips (January–February 2026).

For every query, the dashboard shows:
1. The **query result** and an interactive chart
2. A detailed **DB Internals** tab mapping application behavior → DuckDB internal execution
3. The **EXPLAIN** logical plan and optionally **EXPLAIN ANALYZE** with operator timing

---

## Setup Instructions

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Place data files

Create a `data/` folder and copy your downloaded files into it:

```
nyc_taxi_explorer/
├── data/
│   ├── yellow_tripdata_2026-01.parquet
│   ├── yellow_tripdata_2026-02.parquet
│   └── taxi_zone_lookup.csv
├── app.py
├── setup_db.py
├── queries.py
├── requirements.txt
└── README.md
```

### 3. Initialize the database (run ONCE)

```bash
python setup_db.py
```

This creates `taxi.duckdb` (~400 MB) from the Parquet files. You will see:
```
Creating database: taxi.duckdb
Loading taxi_trips from Parquet files...
  taxi_trips loaded: 7,124,755 rows
Loading taxi_zones from CSV...
  taxi_zones loaded: 265 rows
Done. Database saved to: taxi.duckdb
```

### 4. Launch the dashboard

```bash
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## Application Features

### Queries Implemented

| Category | Query | Internal Focus |
|----------|-------|----------------|
| **Aggregation** | Total Revenue by Borough | Vectorized HashAggregate + column projection |
| **Aggregation** | Trip Volume by Hour of Day | Pre-computed derived columns + single-pass scan |
| **Filter + Projection** | Long Trips from Manhattan (>10 mi) | Predicate pushdown + late materialization |
| **Filter + Projection** | Credit Card vs Cash Tip Breakdown | Inline expression evaluation in vectorized scan |
| **Top-K Ranking** | Top 20 Busiest Pickup Zones | TOP_N operator (bounded priority queue) |
| **Top-K Ranking** | Top 10 Highest-Revenue Hours | Two-column GROUP BY + TOP_N optimization |
| **Join + Aggregation** | Borough Performance Summary | Build-probe hash join + multi-metric aggregation |
| **Join + Aggregation** | Tip Rate by Pickup Zone (Top 25) | Predicate pushdown before join + conditional SUM |

### Dashboard Tabs (per query)
- **Chart** — Interactive Plotly visualization
- **Results Table** — Raw query output
- **DB Internals** — Three-panel: Application Behavior / Internal Execution / Why It Matters
- **SQL & EXPLAIN** — Full SQL + optional EXPLAIN / EXPLAIN ANALYZE output

---

## Database Schema

### `taxi_trips` (7,124,755 rows)
| Column | Type | Description |
|--------|------|-------------|
| VendorID | INTEGER | Taxi vendor (1 or 2) |
| pickup_datetime | TIMESTAMP | Trip start time |
| dropoff_datetime | TIMESTAMP | Trip end time |
| passenger_count | BIGINT | Number of passengers |
| trip_distance | DOUBLE | Distance in miles |
| pickup_location_id | INTEGER | Pickup zone ID (FK → taxi_zones) |
| dropoff_location_id | INTEGER | Dropoff zone ID (FK → taxi_zones) |
| payment_type | BIGINT | Raw payment code |
| fare_amount | DOUBLE | Metered fare ($) |
| tip_amount | DOUBLE | Tip ($) |
| total_amount | DOUBLE | Total charged ($) |
| pickup_hour | INTEGER | Derived: hour 0–23 |
| pickup_dow | INTEGER | Derived: day of week 0–6 |
| pickup_month | INTEGER | Derived: 1 or 2 |
| pickup_date | DATE | Derived: date only |
| trip_duration_min | INTEGER | Derived: duration in minutes |
| payment_label | VARCHAR | Derived: 'Credit Card', 'Cash', etc. |

### `taxi_zones` (265 rows)
| Column | Type | Description |
|--------|------|-------------|
| location_id | INTEGER | Zone ID (PK) |
| borough | VARCHAR | Borough name |
| zone | VARCHAR | Zone name |
| service_zone | VARCHAR | 'Yellow Zone', 'Boro Zone', 'EWR' |

---

## DuckDB Internal Concepts Demonstrated

### 1. Vectorized Execution
DuckDB processes data in **chunks of 2,048 rows** (vectors) rather than one row at a time.
This enables SIMD CPU instructions and keeps data in L1/L2 cache.

### 2. Column Projection
DuckDB reads only the columns needed by the query from Parquet storage.
For a 20-column table where only 3 are needed, ~85% of I/O is eliminated.

### 3. Predicate Pushdown
Filters are applied during the Parquet scan, before any joins or aggregations.
Row groups that don't match the predicate statistics are skipped entirely.

### 4. Build-Probe Hash Join
For joins between a large fact table and a small dimension table,
DuckDB loads the small table into memory as the build side
and streams the large table through as the probe side.

### 5. TOP_N Optimization
`ORDER BY x LIMIT k` queries use a bounded priority queue of size k
instead of a full sort — O(n log k) instead of O(n log n).

---

## Data Source

- **NYC TLC Yellow Taxi Trip Records**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Taxi Zone Lookup Table**: same page, under "Taxi Zone Maps and Lookup Tables"
- License: NYC Open Data — public domain

---

## References

1. Raasveldt, M. and Mühleisen, H. "DuckDB: an Embeddable Analytical Database." SIGMOD 2019.
2. DuckDB Documentation: EXPLAIN ANALYZE / Profiling. https://duckdb.org/docs/stable/guides/meta/explain_analyze.html
3. DuckDB Documentation: Internals Overview. https://duckdb.org/docs/stable/internals/overview.html
4. NYC TLC Trip Record Data. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
