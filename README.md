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
