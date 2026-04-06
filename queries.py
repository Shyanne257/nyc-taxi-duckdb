"""
queries.py
----------
Defines all analytical queries used by the dashboard.

Each entry in QUERIES is a dict with:
  - id          : unique identifier
  - title       : display name shown in the UI
  - category    : grouping label
  - sql         : the actual SQL executed against DuckDB
  - description : what the application does (user-facing)
  - internals   : what DuckDB does internally
  - why_matters : why that internal behavior is significant
  - chart_type  : 'bar' | 'line' | 'scatter' | 'table'
  - x_col / y_col : column names for charting (if applicable)
"""

QUERIES = [

    # ── 1. Aggregation ─────────────────────────────────────────────────────────
    {
        "id": "agg_borough_revenue",
        "title": "Total Revenue by Borough",
        "category": "Aggregation",
        "sql": """
SELECT
    z.borough,
    COUNT(*)                        AS trip_count,
    ROUND(SUM(t.total_amount), 2)   AS total_revenue,
    ROUND(AVG(t.total_amount), 2)   AS avg_fare
FROM taxi_trips t
JOIN taxi_zones z ON t.pickup_location_id = z.location_id
GROUP BY z.borough
ORDER BY total_revenue DESC
""",
        "description": (
            "Groups all 7 million trip records by pickup borough and computes "
            "total revenue, trip count, and average fare. This is the core "
            "aggregation operation: a full-table GROUP BY with SUM and COUNT "
            "applied across every row in the dataset."
        ),
        "internals": (
            "DuckDB executes this query using its **vectorized execution engine**. "
            "Rather than processing one row at a time (tuple-at-a-time model), "
            "DuckDB processes data in *chunks* of up to 2,048 values. "
            "Only the columns `total_amount` and `pickup_location_id` are scanned "
            "from storage — all other columns are skipped entirely. "
            "The HashAggregate operator accumulates partial sums within each chunk, "
            "then merges partial results at the end. The JOIN is resolved as a "
            "hash join against the small 265-row `taxi_zones` table, which is "
            "fully held in memory."
        ),
        "why_matters": (
            "Processing 7M rows in chunks means the CPU can keep 2,048 values "
            "of `total_amount` in L1/L2 cache simultaneously, enabling SIMD "
            "(Single Instruction, Multiple Data) operations. This is why DuckDB "
            "can aggregate millions of rows far faster than a row-oriented system "
            "like MySQL, which would fetch every column of every row even if only "
            "two columns are needed."
        ),
        "chart_type": "bar",
        "x_col": "borough",
        "y_col": "total_revenue",
    },

    {
        "id": "agg_hourly_trips",
        "title": "Trip Volume by Hour of Day",
        "category": "Aggregation",
        "sql": """
SELECT
    pickup_hour,
    COUNT(*)                      AS trip_count,
    ROUND(AVG(trip_duration_min), 1) AS avg_duration_min,
    ROUND(AVG(fare_amount), 2)    AS avg_fare
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY pickup_hour
""",
        "description": (
            "Aggregates all trips by the hour they were picked up (0–23). "
            "Shows how demand, trip duration, and average fare vary throughout "
            "the day — a classic time-series aggregation over a derived column."
        ),
        "internals": (
            "The column `pickup_hour` was pre-computed during table creation via "
            "`EXTRACT(hour FROM pickup_datetime)` and stored as an INTEGER, "
            "so no timestamp parsing is needed at query time. "
            "DuckDB's vectorized HashAggregate scans only `pickup_hour`, "
            "`trip_duration_min`, and `fare_amount` — 3 columns out of 20. "
            "The EXPLAIN ANALYZE plan shows a single **SEQ_SCAN → HASH_GROUP_BY** "
            "pipeline with no intermediate materialization."
        ),
        "why_matters": (
            "Projection pushdown (scanning only 3 of 20 columns) reduces I/O "
            "significantly when reading columnar Parquet storage. In a row-oriented "
            "system, each row would be fetched in full even though 17 columns "
            "are irrelevant. DuckDB's columnar format means irrelevant columns "
            "are never read from disk."
        ),
        "chart_type": "line",
        "x_col": "pickup_hour",
        "y_col": "trip_count",
    },

    # ── 2. Filter + Projection ────────────────────────────────────────────────
    {
        "id": "filter_long_trips",
        "title": "Long Trips from Manhattan (> 10 miles)",
        "category": "Filter + Projection",
        "sql": """
SELECT
    t.pickup_datetime,
    t.trip_distance,
    t.trip_duration_min,
    t.fare_amount,
    t.tip_amount,
    zp.zone   AS pickup_zone,
    zd.zone   AS dropoff_zone
FROM taxi_trips t
JOIN taxi_zones zp ON t.pickup_location_id  = zp.location_id
JOIN taxi_zones zd ON t.dropoff_location_id = zd.location_id
WHERE
    zp.borough    = 'Manhattan'
    AND t.trip_distance > 10
ORDER BY t.trip_distance DESC
LIMIT 500
""",
        "description": (
            "Filters the full dataset down to long-distance trips (> 10 miles) "
            "originating from Manhattan, then projects only 7 relevant columns. "
            "This demonstrates selective filtering combined with strict column "
            "projection — the application only needs a narrow slice of the data."
        ),
        "internals": (
            "DuckDB applies the filter predicate (`trip_distance > 10`) *during* "
            "the vectorized scan — each chunk of 2,048 `trip_distance` values is "
            "evaluated with a SIMD comparison, producing a selection vector that "
            "marks which rows pass. Only rows that pass are forwarded to the JOIN "
            "operators. The projection ensures that only 7 of 20 columns are "
            "materialized in memory for output. The EXPLAIN plan shows: "
            "**PARQUET_SCAN → FILTER → HASH_JOIN × 2 → PROJECTION → TOP_N**."
        ),
        "why_matters": (
            "Late materialization: DuckDB delays constructing full output rows "
            "until after filtering. This means for a predicate that eliminates "
            "99% of rows, almost no memory is allocated for rejected rows. "
            "In contrast, a row-store database would fetch all columns of every "
            "row before applying the filter."
        ),
        "chart_type": "scatter",
        "x_col": "trip_distance",
        "y_col": "fare_amount",
    },

    {
        "id": "filter_payment_projection",
        "title": "Credit Card vs Cash: Fare & Tip Breakdown",
        "category": "Filter + Projection",
        "sql": """
SELECT
    payment_label,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(AVG(tip_amount),  2)      AS avg_tip,
    ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 1) AS avg_tip_pct
FROM taxi_trips
WHERE payment_label IN ('Credit Card', 'Cash')
GROUP BY payment_label
""",
        "description": (
            "Filters trips to only Credit Card and Cash payments, then computes "
            "fare and tip statistics per payment type. Demonstrates both a simple "
            "equality filter and a computed expression (tip percentage) executed "
            "inline during aggregation."
        ),
        "internals": (
            "The filter on `payment_label` is applied as a vectorized predicate "
            "during the SEQ_SCAN phase. The expression "
            "`tip_amount / NULLIF(fare_amount, 0)` is evaluated as a vectorized "
            "arithmetic operation across each chunk — DuckDB computes divisions "
            "for all 2,048 values in a chunk simultaneously before passing results "
            "to the HashAggregate. NULLIF is handled with a selection mask, not "
            "a branch per row."
        ),
        "why_matters": (
            "Inline expression evaluation during scanning (rather than in a "
            "separate pass) reduces memory pressure. DuckDB never materializes "
            "a full intermediate column for `tip_amount / fare_amount` — the "
            "division is fused into the aggregation pipeline."
        ),
        "chart_type": "bar",
        "x_col": "payment_label",
        "y_col": "avg_tip_pct",
    },

    # ── 3. Top-K Ranking ──────────────────────────────────────────────────────
    {
        "id": "topk_pickup_zones",
        "title": "Top 20 Busiest Pickup Zones",
        "category": "Top-K Ranking",
        "sql": """
SELECT
    z.zone,
    z.borough,
    COUNT(*)                      AS trip_count,
    ROUND(AVG(t.fare_amount), 2)  AS avg_fare,
    ROUND(SUM(t.total_amount), 2) AS total_revenue
FROM taxi_trips t
JOIN taxi_zones z ON t.pickup_location_id = z.location_id
GROUP BY z.zone, z.borough
ORDER BY trip_count DESC
LIMIT 20
""",
        "description": (
            "Ranks all 265 NYC taxi zones by total trip count and returns the "
            "top 20 busiest pickup locations. This is a classic Top-K query: "
            "GROUP BY over the full dataset, then sort and truncate."
        ),
        "internals": (
            "DuckDB uses a **TOP_N operator** for this query rather than a full "
            "sort. Instead of sorting all 265 group results, it maintains a "
            "bounded priority queue of size 20 during aggregation. This means "
            "only 20 entries are kept in memory at any time, regardless of how "
            "many groups exist. The physical plan shows: "
            "**SEQ_SCAN → HASH_JOIN → HASH_GROUP_BY → TOP_N(20)**."
        ),
        "why_matters": (
            "The TOP_N optimization avoids a full O(n log n) sort when only the "
            "top k results are needed. For dashboards that always show 'top 10' "
            "or 'top 20', this is a significant performance win that DuckDB "
            "applies automatically based on the query shape."
        ),
        "chart_type": "bar",
        "x_col": "zone",
        "y_col": "trip_count",
    },

    {
        "id": "topk_revenue_hours",
        "title": "Top 10 Highest-Revenue Hours (Jan vs Feb)",
        "category": "Top-K Ranking",
        "sql": """
SELECT
    pickup_month,
    pickup_hour,
    COUNT(*)                       AS trip_count,
    ROUND(SUM(total_amount), 2)    AS total_revenue,
    ROUND(AVG(total_amount), 2)    AS avg_revenue_per_trip
FROM taxi_trips
GROUP BY pickup_month, pickup_hour
ORDER BY total_revenue DESC
LIMIT 10
""",
        "description": (
            "Finds the 10 hour-of-day / month combinations with the highest "
            "total revenue across the dataset — useful for understanding when "
            "taxi earnings peak across the two-month window."
        ),
        "internals": (
            "This is a two-column GROUP BY (`pickup_month`, `pickup_hour`) "
            "producing 48 groups (2 months × 24 hours). DuckDB's HashAggregate "
            "builds a hash table with 48 buckets and accumulates SUM and COUNT "
            "in a single pass over all 7M rows. Only `pickup_month`, `pickup_hour`, "
            "and `total_amount` are scanned (3 of 20 columns). "
            "The TOP_N(10) is applied after aggregation on 48 rows — trivial cost."
        ),
        "why_matters": (
            "A single sequential pass over 7M rows with vectorized hash "
            "aggregation is fundamentally more efficient than the equivalent "
            "in a row-store, which would require reading and discarding 17 "
            "irrelevant columns per row. The columnar scan + vectorized hashing "
            "combination is DuckDB's primary advantage for this workload."
        ),
        "chart_type": "bar",
        "x_col": "pickup_hour",
        "y_col": "total_revenue",
    },

    # ── 4. Join + Aggregation ─────────────────────────────────────────────────
    {
        "id": "join_borough_summary",
        "title": "Borough Performance Summary (Join + Multi-Agg)",
        "category": "Join + Aggregation",
        "sql": """
SELECT
    z.borough,
    z.service_zone,
    COUNT(*)                               AS trip_count,
    ROUND(AVG(t.trip_distance),     2)     AS avg_distance_mi,
    ROUND(AVG(t.trip_duration_min), 1)     AS avg_duration_min,
    ROUND(AVG(t.fare_amount),       2)     AS avg_fare,
    ROUND(AVG(t.tip_amount),        2)     AS avg_tip,
    ROUND(SUM(t.total_amount) / 1e6, 2)   AS total_revenue_millions,
    ROUND(AVG(t.passenger_count),   1)     AS avg_passengers
FROM taxi_trips t
JOIN taxi_zones z ON t.pickup_location_id = z.location_id
GROUP BY z.borough, z.service_zone
ORDER BY trip_count DESC
""",
        "description": (
            "Joins 7M trip records with the 265-row zone dimension table to "
            "enrich trips with borough and service_zone labels, then computes "
            "8 aggregate metrics per borough. This is the most complex query: "
            "a fact-table + dimension-table join followed by multi-metric aggregation."
        ),
        "internals": (
            "DuckDB uses a **build-probe hash join** strategy here. The smaller "
            "`taxi_zones` table (265 rows) is used as the *build side*: it is "
            "fully loaded into a hash table in memory. The larger `taxi_trips` "
            "table (7M rows) is then streamed through as the *probe side*, with "
            "each chunk of 2,048 `pickup_location_id` values hashed against the "
            "in-memory table. This is called a *broadcast hash join* and avoids "
            "any disk-based shuffle. The physical plan shows: "
            "**SEQ_SCAN(taxi_trips) → HASH_JOIN [build: taxi_zones] → HASH_GROUP_BY → PROJECTION**."
        ),
        "why_matters": (
            "The asymmetric join strategy (small build side, large probe side) "
            "is automatically chosen by DuckDB's query optimizer based on table "
            "statistics. The entire 265-row zone table fits in a few KB of memory, "
            "so the join cost is effectively O(n) in the number of trip rows — "
            "no sorting or partitioning is required. This is the optimal execution "
            "strategy for star-schema analytical queries."
        ),
        "chart_type": "bar",
        "x_col": "borough",
        "y_col": "total_revenue_millions",
    },

    {
        "id": "join_zone_tip_rate",
        "title": "Tip Rate by Pickup Zone (Top 25)",
        "category": "Join + Aggregation",
        "sql": """
SELECT
    z.zone,
    z.borough,
    COUNT(*)                            AS trip_count,
    ROUND(AVG(t.tip_amount), 2)         AS avg_tip,
    ROUND(
        100.0 * SUM(CASE WHEN t.tip_amount > 0 THEN 1 ELSE 0 END)
        / COUNT(*), 1
    )                                   AS tip_rate_pct
FROM taxi_trips t
JOIN taxi_zones z ON t.pickup_location_id = z.location_id
WHERE t.payment_label = 'Credit Card'
GROUP BY z.zone, z.borough
HAVING COUNT(*) > 1000
ORDER BY avg_tip DESC
LIMIT 25
""",
        "description": (
            "Identifies the 25 pickup zones where credit-card passengers tip the "
            "most on average. Uses a HAVING clause to exclude low-volume zones, "
            "a conditional SUM for tip rate, and a filter on payment type — "
            "combining join, filter, aggregation, and Top-K in one query."
        ),
        "internals": (
            "This query demonstrates DuckDB's **predicate pushdown**: the filter "
            "`payment_label = 'Credit Card'` is applied during the Parquet scan, "
            "before the join. This means the hash join only receives matching rows, "
            "roughly halving the probe-side volume. The conditional expression "
            "`CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END` is evaluated as a "
            "vectorized selection mask — no branch prediction overhead per row. "
            "The HAVING filter is applied after aggregation on the small group "
            "result set (265 zones), not on the raw 7M rows."
        ),
        "why_matters": (
            "Predicate pushdown is one of DuckDB's most impactful optimizations: "
            "by filtering rows as early as possible in the pipeline (at scan time), "
            "all downstream operators receive fewer rows. This is especially "
            "valuable when reading Parquet files, because entire row groups can "
            "be skipped based on column statistics without reading any data."
        ),
        "chart_type": "bar",
        "x_col": "zone",
        "y_col": "avg_tip",
    },

]

# Lookup helper
QUERY_MAP = {q["id"]: q for q in QUERIES}
CATEGORIES = list(dict.fromkeys(q["category"] for q in QUERIES))
