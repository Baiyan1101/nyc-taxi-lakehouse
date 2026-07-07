# Architecture

The project follows a layered lakehouse layout:

- Raw: original files, unchanged
- Cleaned: validated and standardized trip records
- Curated: fact and dimension tables
- Analytics: aggregated tables for analysis

Quality reports are generated from the raw layer before cleaning. They are diagnostic artifacts: they explain what kinds of records exist in raw data, but they do not modify the raw files.

The cleaned layer is the first transformed layer. It removes records that violate explicit quality rules, adds reusable derived columns, and writes partitioned Parquet for downstream curated and analytics jobs.
