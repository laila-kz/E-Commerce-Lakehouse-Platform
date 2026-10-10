# 🏛️ E-Commerce Lakehouse Platform

[![CI/CD Pipeline](https://github.com/laila-kz/E-Commerce-Lakehouse-Platform/actions/workflows/ci.yml/badge.svg)](https://github.com/laila-kz/E-Commerce-Lakehouse-Platform/actions/workflows/ci.yml)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logo=apachespark&logoColor=white)](https://delta.io/)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Scala 2.12](https://img.shields.io/badge/Scala-2.12.18-DC322F?style=flat&logo=scala&logoColor=white)](https://www.scala-lang.org/)
[![Apache Spark Catalyst](https://img.shields.io/badge/Spark_Catalyst-Optimizer_Rule-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)

A production-patterned local **Data Lakehouse Platform** built for high-volume e-commerce clickstream events. It combines **Apache Spark (PySpark + Custom Scala Catalyst Optimizer Rules) + Delta Lake** on **MinIO S3** for scalable transactional ingestion, automated **Soda Core quality gates**, a **YAML Data Contract engine**, **dbt + DuckDB** for dimensional modeling (SCD Type 2, sessionization, and business marts), orchestrated under **Apache Airflow** with **OpenLineage metadata governance**.

---

## 🎯 Platform Mission & Core Architecture

Traditional data platforms frequently suffer from three critical production problems:
1. **Silent Data Drops & Watermark Regressions:** Incremental pipelines silently discard late-arriving events or corrupt state during restarts.
2. **Schema Breakages & Uncontrolled Drift:** Upstream producers push breaking changes that crash downstream dashboards without quarantine controls.
3. **Black-Box Metadata:** Inability to trace lineage, dataset versions, and blast radius across heterogeneous compute engines (Spark ➔ DuckDB).

This project solves these challenges with a **reconciliation-first, crash-resilient local lakehouse**:

```mermaid
graph TD
    subgraph Ingestion & Storage ["1. Storage & Ingestion"]
        A[Kaggle API / Source CSVs] -->|contract_cli.py| C{Contract Gate}
        C -->|Valid| B[(MinIO: s3a://raw)]
        C -->|Breaking Schema| Q[("data/quarantine/ (local)")]
        B -->|schema enforcement| D[bronze_transform.py]
    end

    subgraph Medallion Compute ["2. Medallion Processing (PySpark & Delta Lake)"]
        D -->|Struct Schema Enforcement| E[(MinIO: s3a://bronze Delta)]
        E -->|Soda Quality Gate| F{Bronze Gate}
        F -->|Pass| G[silver_transform.py]
        F -->|Fail| H[Halt & Slack Alert]
        G -->|SHA-256 Dedup + MERGE| I[(MinIO: s3a://silver Delta)]
        I -->|Soda Quality Gate| J{Silver Gate}
        J -->|Pass| K[dbt_run_staging]
        J -->|Fail| H
    end

    subgraph Analytics & Marts ["3. Dimensional Modeling (dbt + DuckDB)"]
        K -->|Views| L[stg_events / stg_products]
        L -->|30-min Idle Windowing| M[int_sessions / int_events_enriched]
        M -->|SCD Type 2 (product) + Star Schema| N[dim_customer / dim_product / dim_date]
        N --> O[fact_events / fact_purchases]
        O -->|Incremental delete+insert| P[mart_daily_summary / mart_retention / mart_category]
        P -->|84 dbt Tests Passing| R[dbt Exposures & BI Dashboards]
    end

    subgraph Governance ["4. Enterprise Governance (OpenLineage + Marquez)"]
        AF[Airflow DAG ecommerce_lakehouse] -.->|OpenLineage transport: job-run lineage| OL[Marquez Metadata Server]
        EM[scripts/emit_marquez_lineage.py] -.->|dataset & schema facets| OL
    end
```

> _Note: the Contract Gate is a standalone CLI today (`make test-contracts`), not an Airflow DAG task. Airflow emits job-run lineage; dataset/schema facets come from `scripts/emit_marquez_lineage.py` (no Spark listener or dbt integration)._

---

## 💼 Business Questions Answered

The Gold layer dimensional model and analytical marts answer core e-commerce operational questions:

1. **Executive Daily Performance (`mart_daily_summary`):**
   - What is the daily revenue, active user count, and total event volume broken down by top-level category (`category_l1`)?
   - Materialized incrementally via `delete+insert` on affected partition keys.
2. **Customer Cohort Retention (`mart_customer_retention`):**
   - How do monthly acquisition cohorts retain over time?
   - Enforces domain invariants: Month-0 retention must equal 100%, and retained user count must be monotonically non-increasing.
3. **Category Revenue Growth (`mart_category_performance`):**
   - What is the month-over-month revenue growth rate per category, handling cold-start zero-division safely?
4. **User Sessionization (`int_sessions`):**
   - How do user interactions group into distinct browsing sessions based on a 30-minute inactivity threshold?

---

## 🏗️ Repository Structure

```text
.
├── .github/workflows/          # CI/CD: Automated linting (flake8), dbt compilation, syntax verification
├── airflow/                    # Airflow DAGs, plugins (Slack failure alerts), and demo samples
│   ├── dags/
│   │   └── lakehouse_pipeline.py  # Master 10-task automated orchestration DAG
│   └── plugins/
│       └── slack_alert.py         # Dual-channel failure alerting (Slack Webhook + Email)
├── checks/                     # Quality gate runners and cross-layer reconciliation suites
│   └── run_quality_gate.py     # Unified 3-layer quality gate runner (Raw + Bronze + Silver)
├── contracts/                  # Shift-left YAML data contract engine
│   ├── contract_cli.py         # Contract validation CLI, diff analyzer, and quarantine router
│   └── schemas/                # Versioned contract definitions (ecommerce_events_v1.yml)
├── data/                       # Local test samples, sample CSVs, and dead-letter quarantine directories
├── dbt/                        # dbt project configured with dbt-duckdb
│   ├── models/
│   │   ├── staging/            # 1:1 view models on Silver Delta tables (stg_events, stg_products)
│   │   ├── intermediate/       # Sessionization window models (int_sessions, int_events_enriched)
│   │   └── marts/              # Fact tables, SCD2 dimensions, and Gold business marts
│   └── tests/                  # 84 dbt tests (74 generic + 10 singular)
├── docs/                       # Architectural documentation, ADRs, data dictionary, runbooks
│   └── adr/                    # Architecture Decision Records (ADR-001 through ADR-006)
├── extensions/                 # Custom JVM / Spark Catalyst Extensions (Scala 2.12)
│   └── catalyst_optimizer/     # DynamicEcommercePartitionPruner AST optimization rule & sbt build
├── ingestion/                  # Ingestion scripts (kaggle_ingest.py with retry backoff & MinIO markers)
├── soda/                       # Soda Core quality checks for Raw, Bronze, and Silver layers
├── spark_jobs/                 # PySpark transformations and performance benchmarking suite
│   ├── bronze_transform.py     # Schema enforcement and Bronze Delta write
│   ├── silver_transform.py     # Incremental MERGE, SHA-256 dedup, and post-commit watermarking
│   └── spark_benchmark.py     # PySpark optimization benchmark (Z-Ordering & Broadcast joins)
├── docker-compose.yml          # Full multi-container stack: MinIO, Spark, Airflow, Marquez, Metabase
└── Makefile                    # Cross-platform task runner
```

---

## ⚡ Quick Start & Local Setup

### Prerequisites
- **Docker & Docker Compose** (Allocated with ≥ 6GB RAM)
- **Python 3.10+**
- Kaggle API Key (`kaggle.json` or credentials in `.env`)

### 1. Environment Initialization

#### 🪟 Windows (PowerShell)
```powershell
# Copy environment template
Copy-Item .env.example .env

# Start Docker infrastructure (MinIO, Spark, Airflow, Postgres, Metabase)
docker compose up -d

# Create local Python virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt (or dbt-duckdb soda-core-spark)

# Initialize MinIO Buckets
python scripts/create_buckets.py
```

#### 🐧 Linux / macOS (Bash)
```bash
# Copy environment template
cp .env.example .env

# Start Docker infrastructure
docker compose up -d

# Create local Python virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Initialize MinIO Buckets
python3 scripts/create_buckets.py
```

---

## 🚀 Running the Full Pipeline

### Option A: Complete Orchestration via Airflow
Open the Airflow UI at [http://localhost:8080](http://localhost:8080) (Default credentials: `airflow` / `airflow`) and trigger the `ecommerce_lakehouse` DAG.
> **Note:** The default pipeline executes against the curated `demo_sample` dataset to enable fast local end-to-end execution. Full historical backfills can be triggered via Airflow CLI.

### Option B: Step-by-Step CLI Execution (via `Makefile`)

```bash
# 1. Run Data Contract Pre-Ingestion Gate
make test-contracts

# 2. Ingest Raw Clickstream Data to MinIO
python ingestion/kaggle_ingest.py

# 3. Execute PySpark Bronze Transform (Schema Enforcement)
make run-bronze

# 4. Execute PySpark Silver Transform (Incremental MERGE & Dedup)
make run-silver

# 5. Execute dbt Dimensional Transformations & Full Test Suite (84 Tests)
make run-dbt
make test-dbt

# 6. Run PySpark Delta Optimization & Benchmark Suite
make run-benchmarks
```

---

## 🔄 Continuous Integration

A single workflow, [`.github/workflows/ci.yml`](.github/workflows/ci.yml), runs on every push to `main` and `audit-remediation` and on every pull request. Its jobs are:

| Job | What it verifies |
|---|---|
| `lint-code` | `flake8 .` against the repository (config in `.flake8`). |
| `python-syntax` | `python -m compileall` over `ingestion`, `spark_jobs`, `checks`, `contracts`, `scripts`, `airflow`, `soda` and `tests`. |
| `dbt-check` | `dbt parse` + `dbt compile` for the DuckDB Gold models. |
| `unit-tests` | Contract-gate, reconciliation and ingest-marker tests (`tests/test_contract_cli.py`, `tests/test_reconciliation.py`, `tests/test_ingest_marker.py`). No Spark required. |
| `spark-tests` | Spark 3.5.0 + Delta 3.1.0 dedup/MERGE idempotency and crash-recovery tests (`tests/test_silver_dedup.py`, `tests/test_failure_recovery_silver.py`). |
| `scala-tests` | `sbt test` for the Catalyst partition-pruner rule (`extensions/catalyst_optimizer`). |
| `publish-lineage-graph` | Main branch only: regenerates `docs/graph_data.json` and deploys the 3D lineage graph to GitHub Pages. |

**Not covered by CI** (verified locally until a Step 10 runtime pass):

- A full `docker-compose` end-to-end run (Airflow → MinIO → Spark → dbt). The DAG is checked structurally and its tasks are unit-tested, but the orchestrated pipeline is not executed in CI.
- The 84 dbt tests (`make test-dbt`), which need populated DuckDB/MinIO tables; `dbt-check` only parses and compiles models.
- The OpenLineage emitter output against a live Marquez, and the (not-yet-implemented) Spark listener and dbt integration.

---

## 🛡️ Reliability, Contracts & Crash-Recovery Matrix

### 1. Shift-Left YAML Data Contracts (`contract_cli.py`)
Schema changes are evaluated prior to ingestion:
- **`COMPATIBLE`** (e.g. adding a nullable column that carries a default value): Batch and contract version pass.
- **`WARNING`** (e.g. new optional column without a default): Batch passes; existing rows will be NULL.
- **`BREAKING`** (e.g. column dropped, column renamed, data type change, required column added, nullability tightened): Batch is quarantined locally to `data/quarantine/` with a diagnostic JSON, and a Slack alert is attempted (no-op with a logged warning when `SLACK_WEBHOOK_URL` is unset).

The contract gate is a standalone CLI today: `contract_cli.py` validates local CSV files and isn't called by the Airflow DAG (invoke it via `make test-contracts`). It does not yet post a schema diff to Slack (only breaking-validation alerts) and it is not wired into the pipeline.

### 2. Failure-Injection Recovery Matrix
Three of the four boundaries are now backed by automated tests (run in CI via `.github/workflows/ci.yml`); the dbt row remains designed behavior without a test. The ingest and Silver crash hooks are default-off, so normal runs are unaffected.

| Boundary | Instrumented? | Expected Behavior | Recovery Action | Status |
|---|---|---|---|---|
| Raw ingest | Yes — `FAIL_AFTER_INGEST_UPLOAD` (default off, `ingestion/kaggle_ingest.py`) | Uploads complete but the `_SUCCESS` marker is not written | Re-run ingestion | Test-backed by `tests/test_ingest_marker.py`: the marker is written last, a pre-marker crash leaves no marker, and `--force` overwrites it |
| Bronze write / gate | Gate exit code tested; Bronze write crash not injected | A non-zero quality-gate exit halts Silver via DAG ordering | Re-run Soda gate | Test-backed by `tests/test_failure_recovery_silver.py`: the gate exit code is non-zero when any suite fails, and the DAG graph places `bronze_quality_gate` upstream of `silver_transform` |
| Silver post-MERGE, pre-watermark | Yes — `SIMULATE_CRASH_AFTER_MERGE` / `FAIL_AFTER_SILVER_MERGE` / `FAIL_BEFORE_WATERMARK_UPDATE` (three aliases of one injection point, `spark_jobs/silver_transform.py`) | Watermark stays at the prior timestamp; a re-run re-MERGEs the same batch | `make run-silver` | Test-backed by `tests/test_failure_recovery_silver.py`: crash → watermark unchanged → re-run inserts 0 rows → watermark advances. Re-run idempotency also covered by `tests/test_silver_dedup.py` |
| dbt build | No | Target tables remain consistent (atomic swap) | Re-run dbt | Designed behavior, not verified (requires a dbt + DuckDB runtime) |

_The ingest-marker and Silver MERGE-to-watermark boundaries are covered by automated tests (`tests/test_ingest_marker.py`, `tests/test_failure_recovery_silver.py`); the Bronze-gate halt is covered by the gate exit-code and DAG-ordering tests, while a Bronze write crash and dbt mid-build failure remain designed but untested._

### 3. Airflow Orchestration (`airflow/dags/lakehouse_pipeline.py`)
Retries are tuned per task group rather than uniform: `ingest_raw` retries 4× with 1m exponential backoff (cap 8m); `bronze_transform`/`silver_transform` retry 2× at 3m (cap 15m); the Soda quality gates retry 1× at 1m with no backoff; the four dbt build tasks retry 2× at 2m (cap 10m); `dbt_test_gate` retries 0×. Each task keeps its own `execution_timeout`. If the `slack_alert` plugin cannot be imported, the DAG logs a warning and the failure callback becomes a no-op, so a missing plugin is visible in the scheduler log instead of failing silently. Spark tasks are launched with `--jars /opt/spark/jars-extra/*.jar` and `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.lakehouse.optimizer.EcommerceSparkExtensions`, passing the custom Catalyst partition-pruner rule to Airflow-launched Spark jobs (configured, not verified at runtime).

---

## 🔍 Metadata & Lineage Governance (OpenLineage + Marquez)

The platform implements the **Linux Foundation OpenLineage Standard**:
- **Airflow job-run lineage:** Airflow emits job-run lineage through the configured OpenLineage transport (task lifecycle states).
- **Dataset & schema facets:** Emitted by `scripts/emit_marquez_lineage.py` for the Spark and dbt datasets. A Spark listener and a dbt integration are not implemented, and the emitter's output has not been verified against a live Marquez.
- **Marquez UI:** Available at [http://localhost:3001](http://localhost:3001) for visual lineage inspection, dataset facet tracking, and incident blast-radius analysis.

---

## ⚖️ Architectural Decisions & Tradeoffs

Detailed Architecture Decision Records are maintained in [`docs/adr/`](docs/adr/README.md):

- **[ADR-001: MinIO S3 & Delta Lake for Local Storage](docs/adr/ADR-001_minio_and_delta_lake_local_storage.md)** — Simulates cloud S3 API semantics and ACID transaction guarantees at zero infrastructure cost.
- **[ADR-002: Hybrid Event-Time Watermarking & Lateness Buffer](docs/adr/ADR-002_hybrid_event_time_watermarking_and_lateness_buffer.md)** — Combines event-time partitioning with allowed lateness buffers and post-commit state advancement.
- **[ADR-003: Deterministic SHA-256 Composite Keys](docs/adr/ADR-003_sha256_synthetic_event_keys.md)** — Enables deterministic deduplication across multi-engine boundaries.
- **[ADR-004: dbt-on-DuckDB for Gold Layer](docs/adr/ADR-004_dbt_on_duckdb_for_gold_layer.md)** — Achieves sub-second analytical model compilation and 84 data tests directly against MinIO Delta tables.
- **[ADR-005: Late Data & Partition Rebuild Strategy](docs/adr/ADR-005_late_data_and_partition_rebuild_strategy.md)** — Dynamic affected-partition detection for late events.
- **[ADR-006: OpenLineage Standard & Marquez for Metadata Governance](docs/adr/ADR-006_openlineage_and_marquez_for_metadata_governance.md)** — Vendor-neutral metadata tracking across Spark, Airflow, and dbt.

---

## 📚 Technical Documentation

- 📖 **[ADR Index](docs/adr/README.md)** — Full technical decision records.
- 🚀 **[Execution & Runbook Guide](docs/EXECUTION_AND_RUNBOOK_GUIDE.md)** — Complete step-by-step master guide for running the pipeline locally in Fast Demo Mode.
- 📚 **[Data Catalog & Data Dictionary](docs/data_dictionary.md)** — Unified schema reference across Raw, Bronze, Silver, and Gold.
- ⚡ **[Spark Performance Benchmarks](docs/performance_benchmarks.md)** — Benchmark script and a reference profile (fallback values when Spark is unavailable); a live measurement has not been recorded.
- 📋 **[Demo Script & Runbook](docs/DEMO_AND_RUNBOOK.md)** — Cold-start execution instructions and video walkthrough guide.

---

## 🏆 Resume Summary Bullets

- *Architected a production-patterned e-commerce Data Lakehouse utilizing PySpark, Delta Lake, MinIO, and dbt on DuckDB under Apache Airflow orchestration.*
- *Implemented incremental ingestion with SHA-256 deterministic deduplication and post-commit watermarking; re-running an already-merged batch inserts 0 rows, and duplicate keys inside a batch are collapsed deterministically before the write (`tests/test_silver_dedup.py`, run in CI).*
- *Engineered a YAML Data Contract engine with breaking-schema classification, automated dead-letter quarantine routing, and breaking-batch Slack alerting (no-op when the webhook is unset).*
- *Integrated the OpenLineage standard with Marquez for Airflow job-level lineage, with `scripts/emit_marquez_lineage.py` emitting dataset and schema facets for the Spark and dbt datasets.*
- *Authored a suite of 84 dbt tests covering SCD Type 2 dimensions, 30-minute idle sessionization, and cohort retention invariants.*