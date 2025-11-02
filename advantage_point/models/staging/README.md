# Staging Layer

The **staging layer** represents the first transformation point after raw data ingestion.  
It exists to create a clean, well-structured, and reliable interface between raw source data and downstream transformations.

This layer’s goal is to apply light, non-destructive transformations that make data easier to query and model while preserving the grain of the source tables.


## Purpose

According to dbt’s best-practice guidance, staging models should:

- Select directly from declared `sources`.
- Rename, clean, and cast columns to consistent data types.
- Preserve the same record grain as the source.
- Avoid joins, aggregations, or business rules.
- Serve as the single, stable reference point for downstream models.