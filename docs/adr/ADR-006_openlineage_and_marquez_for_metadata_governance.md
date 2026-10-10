# ADR-006: OpenLineage Standard & Marquez for Metadata Governance and Operational Lineage

## Status
**Accepted**

## Context
A major operational blind spot in multi-engine Lakehouse environments (Spark for Bronze/Silver + dbt/DuckDB for Gold + Airflow for Orchestration) is fragmented metadata. Without unified lineage tracking:
- There is no single pane of glass showing data flow from raw files to final BI marts.
- Schema drift and column-level mutations across engine boundaries go untracked.
- Debugging pipeline incidents requires manually correlating Airflow task logs, Delta table version logs, and dbt manifest files.

## Decision
We adopted the **Linux Foundation OpenLineage Standard** with **Marquez** as the centralized metadata and lineage repository:
- **Marquez Deployment:** Runs as a lightweight service in `docker-compose.yml` (`marquezproject/marquez`), exposing standard OpenLineage REST APIs and a visual Web UI.
- **Airflow lineage:** Airflow emits job-run lineage through the configured OpenLineage transport (`AIRFLOW__OPENLINEAGE__TRANSPORT`) at the task level.
- **Dataset & schema facets:** `scripts/emit_marquez_lineage.py` emits dataset inputs, outputs, and schema facets for the Spark and dbt datasets. A Spark listener and a dbt model-level integration are not implemented; the emitter's output has not been verified against a live Marquez instance.
- **Incident Blast Radius Analysis:** When contract violations occur, Marquez visualizes the exact downstream models and consumers impacted by the quarantine.

## Alternatives Considered
- **Custom Metadata Logging to SQLite / Postgres:** Low engineering signal; reinvents lineage serialization formats without adhering to open standards.
- **DataHub / Apache Atlas:** Highly capable, but has significantly heavier infra overhead (Elasticsearch, Kafka, Neo4j) compared to Marquez for local execution.
- **Static dbt Docs (`dbt docs generate`):** Only captures dbt SQL lineage, completely blind to Spark jobs, MinIO files, and upstream ingestion tasks.

## Tradeoffs & Consequences
- **Positive:**
  - Standardized, vendor-neutral OpenLineage JSON spec.
  - Airflow job-run lineage, plus a script that emits dataset schemas and input/output dependencies for the Spark and dbt datasets (emitter output not yet verified against a live Marquez).
  - Clear visual interface for incident root-cause and blast-radius triage.
- **Negative:**
  - Requires adding the Marquez container to Docker Compose and running `scripts/emit_marquez_lineage.py` (or Airflow's transport) to populate facets.
