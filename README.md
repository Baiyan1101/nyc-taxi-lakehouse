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
sbt run
```

## Test

```bash
sbt test
```

## Data Source

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- AWS Open Data mirror: https://registry.opendata.aws/nyc-tlc-trip-records-pds/

Start with one month of Yellow Taxi Parquet data, then expand to more months after the pipeline is stable.

