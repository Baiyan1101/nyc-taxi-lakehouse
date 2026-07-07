# Findings

Record data quality findings, performance experiment results, and business insights here.

## Task 2: Raw Ingestion Notes

- The ingestion job supports Parquet and CSV.
- The first recommended dataset is one month of Yellow Taxi Parquet data.
- The job prints row count, column count, column names, and sample rows as ingestion sanity checks.
- Deeper validity checks are intentionally deferred to Task 3.

## Task 3: Data Quality Notes

The quality report currently measures:

- total rows
- duplicate rows
- null counts for key fields
- invalid trip distances
- negative fares
- pickup timestamps later than dropoff timestamps
- abnormal passenger counts

These metrics are intended to describe raw data problems before cleaning. They should be compared with cleaned-layer row counts in Task 4.

For January 2024 Yellow Taxi data, the first quality report found:

- total rows: 2,964,624
- duplicate rows: 0
- invalid trip distance rows: 60,371
- negative fare rows: 37,448
- pickup after dropoff rows: 56
- abnormal passenger count rows: 171,628
- null passenger count rows: 140,162

## Task 4: Cleaning Notes

The cleaned layer applies deterministic filters based on Task 3 quality checks and adds reusable derived columns:

- `pickup_date`
- `pickup_hour`
- `trip_duration_minutes`
- `total_amount_per_mile`
- `tip_percentage`

The cleaned output is written as Parquet and partitioned by `pickup_date`, which supports common date-filtered analytics queries.

For January 2024 Yellow Taxi data, the first cleaning run produced:

- input rows: 2,964,624
- output rows: 2,722,415
- removed rows: 242,209
- output path: `data/cleaned/yellow_taxi`
- partition column: `pickup_date`

Observed follow-up: the January source file initially produced a few out-of-month partitions such as `pickup_date=2002-12-31`, `pickup_date=2009-01-01`, and `pickup_date=2023-12-31`. The cleaning job now supports an optional expected pickup-date window, for example `--expected-start-date 2024-01-01 --expected-end-date 2024-02-01`, to remove records that do not belong to the source month.

## Task 5: Curated Modeling Notes

The curated layer models cleaned trips as a star schema:

- `fact_trips`: one row per cleaned taxi trip
- `dim_date`: one row per pickup date
- `dim_zone`: TLC taxi zone lookup
- `dim_payment_type`: payment code lookup

This structure prepares the project for Task 6 analytics because daily summaries, hourly demand, zone rankings, and route rankings can be expressed as joins and groupings over stable curated tables.

For January 2024 Yellow Taxi cleaned data, the first curated build produced:

- `fact_trips`: 2,722,398 rows
- `dim_date`: 31 rows
- `dim_zone`: 265 rows
- `dim_payment_type`: 6 rows

The `fact_trips` table is partitioned by `pickup_date`, matching the most common date-filtered analytics pattern.

## Task 6: Analytics Notes

The analytics layer generates reusable aggregate tables from the curated star schema:

- `daily_trip_summary`
- `hourly_demand_summary`
- `top_pickup_zones`
- `top_dropoff_zones`
- `top_routes`
- `top_tip_zones`

These outputs are designed to answer common business questions without repeatedly scanning the row-level fact table.

For January 2024 Yellow Taxi curated data, the first analytics build produced:

- `daily_trip_summary`: 31 rows
- `hourly_demand_summary`: 24 rows
- `top_pickup_zones`: 10 rows
- `top_dropoff_zones`: 10 rows
- `top_routes`: 10 rows
- `top_tip_zones`: 10 rows
