# Architecture

The project follows a layered lakehouse layout:

- Raw: original files, unchanged
- Cleaned: validated and standardized trip records
- Curated: fact and dimension tables
- Analytics: aggregated tables for analysis

Quality reports are generated from the raw layer before cleaning. They are diagnostic artifacts: they explain what kinds of records exist in raw data, but they do not modify the raw files.

The cleaned layer is the first transformed layer. It removes records that violate explicit quality rules, adds reusable derived columns, and writes partitioned Parquet for downstream curated and analytics jobs.

The curated layer uses a simple star schema:

```text
             dim_date
                |
dim_zone -- fact_trips -- dim_payment_type
```

`fact_trips` stores trip events and measurable values. Dimensions store descriptive attributes used for grouping, filtering, and joining.

The analytics layer stores precomputed OLAP aggregates derived from the curated model. These tables are designed for repeated consumption by SQL queries, notebooks, or dashboards rather than row-level processing.
