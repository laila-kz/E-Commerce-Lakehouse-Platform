# 🏛️ Complete Guide: Running the E-Commerce Lakehouse (Fast Demo Mode)

**Role Perspective:** Senior Data Platform Engineer  
**Dataset Strategy:** **Fast Demo Mode** (uses the included `data/demo_sample/` ~6MB, 50,000 rows). You do **NOT** need to download the 15GB+ full Kaggle dataset. This lets the entire pipeline run end-to-end locally in **under 45 seconds** without locking CPU or memory.

---

## 🧭 Service Port & UI Reference

| Service | URL | Credentials | Purpose |
|---|---|---|---|
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` | S3 Object Storage Browser |
| **Marquez (OpenLineage UI)** | [http://localhost:3001](http://localhost:3001) | *None* | Operational Lineage & Governance |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` | Pipeline Orchestration |
| **Spark Master UI** | [http://localhost:8081](http://localhost:8081) | *None* | Spark Cluster & Job Status |
| **Metabase BI** | [http://localhost:3000](http://localhost:3000) | Setup on first launch | Business Intelligence Dashboards |

---

## 🛠️ Step 0: Environment Setup & Infrastructure Initialization

### 1. Configure `.env`
Ensure `.env` exists in the repository root (copy from `.env.example`):

#### 🪟 Windows (PowerShell):
```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```
#### 🐧 Linux / macOS (Bash):
```bash
cp -n .env.example .env
```

Your `.env` should look like:
```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000
KAGGLE_USERNAME=demo_user
KAGGLE_KEY=demo_key
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/DUMMY/WEBHOOK/KEY
```

---

### 2. Start the Docker Infrastructure
Spin up MinIO, Spark, Airflow, Postgres, and Marquez:

```bash
docker compose up -d
```

Verify that all containers are healthy:
```bash
docker compose ps
```

---

### 3. Initialize MinIO S3 Buckets
Create the 6 required storage buckets (`raw`, `bronze`, `silver`, `gold`, `reference`, `logs`):

```bash
python scripts/create_buckets.py
```
*(Open [http://localhost:9001](http://localhost:9001) to verify buckets in MinIO).*

---

## 🚀 Step-by-Step Pipeline Execution (From Contracts to Lineage)

---

### Step 1: Shift-Left Data Contracts & Schema Evolution Gate

Before ingesting raw data, validate datasets against versioned YAML contracts and test schema evolution compatibility:

#### 1A. Test Contract Evolution Diff (v1 ➔ v2 Compatibility)
```bash
python contracts/contract_cli.py diff \
  --old-contract contracts/schemas/ecommerce_events_v1.yml \
  --new-contract contracts/schemas/ecommerce_events_v2.yml
```
> **Output:** Classifies schema additions (`device_type`, `currency`) as `[COMPATIBLE]`.

#### 1B. Validate a Clean Dataset
```bash
python contracts/contract_cli.py validate \
  --input-file data/test_valid.csv \
  --contract-file contracts/schemas/ecommerce_events_v1.yml
```
> **Output:** `[CONTRACT PASS]` — logs execution to `contract_registry` table in DuckDB.

#### 1C. Validate a Breaking Schema Dataset (Quarantine Test)
```bash
python contracts/contract_cli.py validate \
  --input-file data/test_breaking.csv \
  --contract-file contracts/schemas/ecommerce_events_v1.yml
```
> **Output:** Intercepts breaking changes and routes bad records to `data/quarantine/`.

---

### Step 2: Ingest Demo Clickstream Data to MinIO (Raw S3)

Ingest the 50,000-row demo dataset into `s3a://raw/ecommerce_events/`:

```bash
python ingestion/kaggle_ingest.py --data-dir ./data/demo_sample --force
```
> **Verification:** Check [http://localhost:9001](http://localhost:9001) (`raw` bucket ➔ `ecommerce_events/ingested_date=...`).

---

### Step 3: PySpark Bronze Transform (Schema Enforcement)

Execute the Bronze transform inside the Spark container to apply explicit Struct schemas and write Delta Lake files to MinIO:

#### 🪟 Windows (PowerShell):
```powershell
docker compose exec spark /opt/spark/bin/spark-submit `
  --driver-memory 2g `
  --jars /opt/spark/jars-extra/*.jar `
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
  /workspace/spark_jobs/bronze_transform.py
```

#### 🐧 Linux / macOS (Bash) or `make`:
```bash
make run-bronze
```
> **Output:** Delta table created at `s3a://bronze/ecommerce_events/`.

---

### Step 4: PySpark Silver Transform (Incremental MERGE & Dedup)

Execute the Silver transform with SHA-256 synthetic deduplication keys, lateness lookback buffer, and post-commit watermarking:

#### 🪟 Windows (PowerShell):
```powershell
docker compose exec spark /opt/spark/bin/spark-submit `
  --driver-memory 2g `
  --jars /opt/spark/jars-extra/*.jar `
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
  /workspace/spark_jobs/silver_transform.py
```

#### 🐧 Linux / macOS (Bash) or `make`:
```bash
make run-silver
```
> **Output:** Incremental MERGE completed into `s3a://silver/ecommerce_events/` and watermark state updated in `s3a://silver/ecommerce_events/_watermarks`.

---

### Step 5: dbt Dimensional Modeling & Invariant Test Suite (DuckDB)

Run dbt models and test assertions against DuckDB:

```bash
# 5A. Execute Transformations (Staging -> Intermediate -> SCD2 Dims & Facts -> Gold Marts)
cd dbt
dbt run --profiles-dir .

# 5B. Run Full 83-Test Quality Suite
dbt test --profiles-dir .
cd ..
```
> **Output:** `83 of 83 tests PASSED` (Sessionization bounds, cohort retention invariants, SCD2 ranges).
> 
> *Note on Infrastructure Requirements:*
> - When running `dbt test` against local materialized DuckDB tables (`dev.duckdb`), 55 dimension and mart invariant tests run directly with zero latency.
> - When running staging tests (`stg_events`, `assert_fact_events_preserve_row_count`) that invoke `delta_scan('s3://...')`, ensure the MinIO container is running (`docker compose up -d minio`) or execute dbt within the containerized environment.

---

### Step 6: Cross-Layer Reconciliation Suite (Zero-Drift Assertion)

Run the multi-layer mathematical reconciliation check:

```bash
python checks/run_reconciliation.py
```
> **Output:**
> ```text
> =====================================================================================
>   CROSS-LAYER RECONCILIATION AUDIT REPORT
> =====================================================================================
> CHECK NAME                          SOURCE          TARGET          DRIFT    STATUS
> -------------------------------------------------------------------------------------
> silver_to_fact_events_row_parity    Silver          Gold (Fact)     0        [PASS]
> purchases_to_mart_revenue_parity    Gold (Fact)     Gold (Mart)     0.0      [PASS]
> silver_to_fact_purchases_parity     Silver          Gold (Fact)     0        [PASS]
> =====================================================================================
> ```
> Results saved to `audit/reconciliation_*.json` and DuckDB `reconciliation_results` table.

---

### Step 7: Platform Observability Telemetry

Record batch runtime metrics, throughput, and watermark lag:

```bash
python checks/record_metrics.py
```
> Results logged to `audit/metrics_*.json` and DuckDB `pipeline_metrics` table.

---

### Step 8: Idempotent Replay & Backfill Engine

Test historical date-range backfills with watermark isolation:

#### 8A. Dry-Run Simulation:
```bash
python scripts/replay_events.py --start-date 2019-10-01 --end-date 2019-10-05 --dry-run
```

#### 8B. Live Backfill Execution:
```bash
python scripts/replay_events.py --start-date 2019-10-01 --end-date 2019-10-05 --mode backfill
```
> **Result:** Reprocesses historical partitions and triggers dbt partition recomputes while leaving forward production watermarks completely untouched.

---

### Step 9: OpenLineage & Marquez Governance Web UI

1. **Populate Lineage Metadata (if not run via Airflow):**
   ```bash
   python scripts/emit_marquez_lineage.py
   ```
2. Open your browser to **[http://localhost:3001](http://localhost:3001)** (Marquez Web UI).
3. In the top navigation / namespace selector, choose **`ecommerce_lakehouse`**.
4. Explore:
   - **Lineage Graph:** Click on any job (e.g. `dbt_gold_marts` or `pyspark_silver_merge_transform`) to view the interactive visual trace from `s3a://raw` ➔ `s3a://bronze` ➔ `s3a://silver` ➔ `stg_events` ➔ `gold_marts`.
   - **Dataset & Job Facets:** View schema fields, data types, and run states.

---

### Step 10: Apache Airflow Automated Orchestration

To run the entire pipeline end-to-end on schedule:
1. Open [http://localhost:8080](http://localhost:8080) (`airflow` / `airflow`).
2. Toggle the `ecommerce_lakehouse` DAG on.
3. Click **Trigger DAG** to watch the automated 10-task flow run from Ingestion ➔ Quality Gates ➔ PySpark ➔ dbt ➔ Tests.

---

## ⚡ 1-Command "Fast Demo" Runner (For Live Demos)

If you are recording a video demo or presenting to an interviewer, run everything in **one command**:

```bash
make run-demo
```
*(Runs sample generation, ingestion, Bronze, Silver, dbt models, and 83 data tests in ~35 seconds).*

---

## 🧪 Crash-Safety & Failure-Injection Test (Interview Showcase)

To demonstrate how the platform recovers from a crash without losing or duplicating data:

```bash
# 1. Simulate a worker crash right after Silver MERGE (before watermark advances)
docker compose exec spark bash -c "SIMULATE_CRASH_AFTER_MERGE=true /opt/spark/bin/spark-submit --driver-memory 2g --jars /opt/spark/jars-extra/*.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/silver_transform.py"
# -> Job crashes intentionally with RuntimeError!

# 2. Trigger automatic recovery retry
make run-silver
# -> Job re-reads prior watermark, applies SHA-256 deduplication, and succeeds with ZERO duplicate rows!

# 3. Verify reconciliation still reports 0 drift
python checks/run_reconciliation.py
```

---

## 🧹 Teardown & Reset

When you want to reset or clean up local containers:

```bash
# Stop containers and preserve state
docker compose stop

# Full reset (cleans Docker volumes for a clean cold-start rebuild)
make clean
```
