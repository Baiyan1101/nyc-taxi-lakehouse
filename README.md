# NYC Taxi Lakehouse

Scala + Apache Spark data engineering project for building a local lakehouse pipeline on NYC Taxi trip records.

## Tech Stack

- Scala 2.12.18
- Apache Spark 3.5.1
- sbt 1.9.9
- Java 17 recommended
- Parquet on local filesystem

## Project Layout

```text
data/
  raw/
  cleaned/
  curated/
  analytics/
src/
  main/scala/com/example/taxi/
  test/scala/com/example/taxi/
docs/
```

## Run

```bash
source scripts/env.sh
sbt run
```

Run raw trip ingestion for one Yellow Taxi Parquet file:

```bash
mkdir -p data/raw/yellow_taxi/year=2024/month=01
curl -L \
  -o data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

sbt "run ingest-raw --format parquet --input data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet --sample-size 5"
```

You can pass multiple `--input` values to read multiple months in one run.

Run a data quality report:

```bash
sbt "run quality-report --format parquet --input data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet --output data/reports/quality/yellow_taxi/year=2024/month=01"
```

Build the cleaned trips layer:

```bash
sbt "run clean-trips --format parquet --input data/raw/yellow_taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet --output data/cleaned/yellow_taxi --expected-start-date 2024-01-01 --expected-end-date 2024-02-01"
```

Build the curated star schema:

```bash
mkdir -p data/raw/taxi_zone_lookup
curl -L \
  -o data/raw/taxi_zone_lookup/taxi_zone_lookup.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

sbt "run build-curated --cleaned-input data/cleaned/yellow_taxi --zone-lookup data/raw/taxi_zone_lookup/taxi_zone_lookup.csv --output data/curated"
```

Build analytics-ready aggregate tables:

```bash
sbt "run build-analytics --curated-input data/curated --output data/analytics --top-n 10"
```

## Test

```bash
source scripts/env.sh
sbt test
```

## Data Source

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- AWS Open Data mirror: https://registry.opendata.aws/nyc-tlc-trip-records-pds/

Start with one month of Yellow Taxi Parquet data, then expand to more months after the pipeline is stable.

## Task 2: Raw Data Ingestion

The raw ingestion job is intentionally simple: it reads source files with an explicit Yellow Taxi schema, prints row count, column count, column names, and sample rows, then leaves the source data unchanged in `data/raw/`.

Implemented entry point:

```bash
sbt "run ingest-raw --format parquet --input <path> --sample-size 10"
```

Supported formats:

- `parquet`: preferred for current NYC TLC trip data
- `csv`: supported for older exports or manually converted data

Design choices:

- Keep raw files immutable so later data quality and cleaning steps are reproducible.
- Use an explicit schema so production jobs do not depend on expensive or unstable schema inference.
- Accept multiple input paths so the same job can scale from one month to many months.
- Print basic metadata immediately so we can catch wrong files, empty inputs, or schema drift early.

Interview talking points:

- `inferSchema` is convenient for exploration, but it adds an extra scan for CSV and can infer inconsistent types across files or months. In production-style pipelines, a declared schema makes failures more deterministic.
- CSV is row-oriented text, larger on disk, slower to parse, and has weaker type information. Parquet is columnar, compressed, stores schema metadata, and lets Spark read only required columns for analytical queries.
- The raw layer should preserve source data exactly. Cleaning, filtering, and derived columns belong in later layers so the pipeline can be audited and rerun.
- Row count and sample output are not full quality checks. They are ingestion sanity checks. Task 3 will add null counts, duplicate checks, invalid distances, invalid fares, and timestamp consistency checks.

## Task 3: Data Quality Report

The data quality report reads the same raw inputs as Task 2 and calculates rule-based metrics that tell us whether the records are usable for downstream cleaning and modeling.

Implemented entry point:

```bash
sbt "run quality-report --format parquet --input <path> --output data/reports/quality/<dataset>"
```

Current checks:

- Total row count
- Duplicate row count across all ingested columns
- Null counts for key fields
- `trip_distance <= 0`
- `fare_amount < 0`
- Pickup timestamp later than dropoff timestamp
- `passenger_count` null, less than or equal to 0, or greater than 8

Design choices:

- Quality checks run against raw data but do not mutate raw data.
- The report produces metrics, not cleaned records. Filtering belongs in Task 4.
- Duplicate counting uses full-row equality, which is simple and explainable for a first pass.
- The passenger-count threshold is intentionally conservative; it can be tuned when domain rules are documented.

Interview talking points:

- Ingestion checks answer whether Spark can read the data; data quality checks answer whether the records make sense.
- Data quality rules should be explicit, measurable, and repeatable. They become a contract between raw data and downstream tables.
- Some quality rules are technical, such as null checks and duplicate checks. Others are business rules, such as valid distance, valid fare, and valid trip duration.
- Quality reports should usually be generated before cleaning so the team can explain how much data was removed or corrected later.

## Task 4: Cleaned Trips Layer

The cleaning job applies the data quality rules and writes analytics-friendly Parquet data to the cleaned layer.

Implemented entry point:

```bash
sbt "run clean-trips --format parquet --input <raw-path> --output data/cleaned/yellow_taxi --expected-start-date 2024-01-01 --expected-end-date 2024-02-01"
```

Cleaning rules:

- Remove rows where pickup or dropoff timestamp is null.
- Remove rows where `trip_distance <= 0`.
- Remove rows where `fare_amount < 0`.
- Remove rows where `passenger_count` is null, `<= 0`, or `> 8`.
- Compute `trip_duration_minutes`.
- Remove rows where trip duration is `<= 0`.
- Remove rows where trip duration is greater than 6 hours.
- Optionally remove rows outside an expected pickup date window.

Derived columns:

- `pickup_date`
- `pickup_hour`
- `trip_duration_minutes`
- `total_amount_per_mile`
- `tip_percentage`

Output design:

- Format: Parquet
- Path: `data/cleaned/yellow_taxi`
- Partition column: `pickup_date`

Interview talking points:

- Task 3 measures data problems; Task 4 applies deterministic cleaning rules.
- Cleaning should be explainable: every removed row should map back to an explicit rule.
- Writing cleaned data as Parquet improves downstream analytical query performance.
- Partitioning by pickup date helps date-filtered queries skip irrelevant files.
- Month-level source files can contain out-of-month timestamps. A configurable expected date window catches those records without hard-coding one month into the job.
- A production pipeline should record input rows, output rows, and removed rows for auditability.

## Task 5: Curated Star Schema

The curated model turns cleaned trips into a simple star schema for analytics.

Implemented entry point:

```bash
sbt "run build-curated --cleaned-input data/cleaned/yellow_taxi --zone-lookup data/raw/taxi_zone_lookup/taxi_zone_lookup.csv --output data/curated"
```

Output tables:

- `data/curated/fact_trips`
- `data/curated/dim_date`
- `data/curated/dim_zone`
- `data/curated/dim_payment_type`

Design choices:

- `fact_trips` contains one row per cleaned trip and stores measurable business events.
- `dim_date` stores reusable date attributes for grouping by day, month, weekday, and weekend.
- `dim_zone` comes from the official Taxi Zone Lookup CSV and gives location IDs business meaning.
- `dim_payment_type` maps numeric TLC payment codes to readable labels.
- `trip_id` is generated as a deterministic hash from stable trip attributes.

Interview talking points:

- The curated layer is closer to a data warehouse than the raw or cleaned layer.
- Fact tables hold events and metrics; dimension tables hold descriptive context.
- A star schema makes downstream OLAP queries simpler and keeps metric definitions consistent.
- Using dimensions avoids repeating descriptive fields in every fact row and makes joins explicit.

## Task 6: Analytics Aggregates

The analytics layer stores precomputed aggregate tables for common reporting and dashboard queries.

Implemented entry point:

```bash
sbt "run build-analytics --curated-input data/curated --output data/analytics --top-n 10"
```

Output tables:

- `data/analytics/daily_trip_summary`
- `data/analytics/hourly_demand_summary`
- `data/analytics/top_pickup_zones`
- `data/analytics/top_dropoff_zones`
- `data/analytics/top_routes`
- `data/analytics/top_tip_zones`

Metric examples:

- Daily total trips
- Daily total revenue
- Daily average fare
- Daily average trip distance
- Daily average trip duration
- Hourly trip demand
- Top pickup and dropoff zones
- Top routes by revenue
- Pickup zones with highest average tip percentage

Interview talking points:

- The analytics layer is optimized for repeated reads by downstream BI tools or notebooks.
- Aggregating from curated fact and dimension tables keeps metric definitions consistent.
- Precomputing common aggregates reduces repeated scan and shuffle work for dashboards.
- Ranking tables such as top routes usually require joins with dimensions to make IDs human-readable.
