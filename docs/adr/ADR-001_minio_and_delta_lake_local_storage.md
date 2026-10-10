# ADR-001: MinIO S3 Object Storage & Delta Lake for Local Lakehouse Storage

## Status
**Accepted**

## Context
When architecting a local data engineering portfolio project, storage design often falls into two flawed extremes:
1. **Local Filesystem Parquet:** Simple to set up, but fails to emulate cloud S3 object storage semantics (e.g. S3A authentication, eventual consistency, object prefix partitioning, zero-copy renames).
2. **Paid Cloud S3 + Databricks/Snowflake:** Accurate cloud semantics, but introduces ongoing cloud costs, credential management overhead, and external network latency for local development.

Furthermore, raw Parquet on object storage lacks ACID transaction guarantees, schema enforcement, time-travel capabilities, and atomic `MERGE` operations, which are essential for resilient data pipelines.

## Decision
We chose **MinIO** as our local S3-compatible object storage layer and **Delta Lake** as our transactional table format for Bronze and Silver layers.

- **MinIO Configuration:** Runs as a Docker container with standard S3A credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), exposing port `9000` (API) and `9001` (Web Console), configured with distinct buckets: `raw`, `bronze`, `silver`, `gold`, `reference`, and `logs`.
- **Delta Lake Integration:** Apache Spark interacts with MinIO via the `hadoop-aws` and `delta-spark` connectors, reading and writing ACID-compliant Delta tables stored under `s3a://<bucket>/<table_name>/`.

## Alternatives Considered
- **Raw Parquet on Local Disk:** Discarded because it cannot demonstrate transactional `MERGE` idempotency, time-travel rollback, or S3 connector debugging.
- **Apache Iceberg:** Viable open-table alternative, but Delta Lake provides native, zero-friction integration with PySpark MERGE operations and DuckDB `delta` extensions in our local execution environment.
- **Local PostgreSQL / SQLite:** Traditional RDBMS storage does not reflect modern Lakehouse Medallion architecture or columnar object storage patterns.

## Tradeoffs & Consequences
- **Positive:**
  - Full parity with AWS S3 API semantics at zero cloud cost.
  - Native ACID transactions, schema enforcement, and time-travel querying (`DESCRIBE HISTORY`, `VERSION AS OF`).
  - Idempotent `MERGE` operations enable incremental reprocessing that inserts 0 rows for an already-merged batch.
- **Negative:**
  - Requires S3A connector JARs (`hadoop-aws`, `aws-java-sdk-bundle`) in the Spark classpath, increasing container build size.
  - Multi-part upload and S3 eventual consistency patterns must be handled cleanly via configuration.
