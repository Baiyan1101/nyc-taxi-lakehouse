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
