# ADR-004: dbt-on-DuckDB for Dimensional Modeling and Gold Marts

## Status
**Accepted**

## Context
In a local Lakehouse architecture, PySpark handles large-scale heavy ingestion, bronze schema enforcement, and silver deduplication. However, using PySpark for the Gold layer (staging views, sessionization, window functions, dimensional SCD2 modeling, and marts) presents several downsides:
1. High Spark JVM startup and execution latency for analytical queries and testing.
2. Inability to leverage the mature software engineering ergonomics of the `dbt` ecosystem (version control, Jinja macros, modular DAG testing, automated documentation, exposures).
3. Running a cloud warehouse (Snowflake / BigQuery) locally incurs cost and internet connectivity dependencies.

## Decision
We adopted **`dbt-duckdb`** as our transformation and modeling engine for the Gold layer:
- DuckDB reads Silver Delta Lake tables directly from MinIO using the native `delta` and `httpfs` extensions.
- dbt compiles and executes standard SQL models (staging views, intermediate sessionizers, dimensional fact/dim tables, and aggregated gold marts) directly against DuckDB.
- All 84 dbt tests (74 generic + 10 singular) run locally in seconds.

## Alternatives Considered
- **dbt-spark / Spark SQL:** High JVM overhead; running tests and compilation is slow in local Docker environments.
- **dbt-postgres:** Requires writing data out of object storage into a separate relational database instance, violating Lakehouse architecture principles.
- **Pure PySpark Scripts for Gold:** Loses dbt's automated dependency graph, lineage, testing framework, and documentation tooling.

## Tradeoffs & Consequences
- **Positive:**
  - Sub-second model compilation and test execution.
  - Native Delta Lake querying on MinIO without exporting data to intermediate files.
  - Industry-standard dbt modeling workflow (staging ➔ intermediate ➔ marts) with full test coverage.
- **Negative:**
  - DuckDB is an in-process, single-node engine; scaling to multi-terabyte production data would require swapping the dbt adapter to `dbt-databricks`, `dbt-snowflake`, or `dbt-trino` (though the SQL code itself remains largely identical).
