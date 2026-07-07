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
