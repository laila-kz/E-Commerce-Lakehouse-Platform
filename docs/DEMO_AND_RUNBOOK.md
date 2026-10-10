# 🎬 E-Commerce Data Lakehouse: Complete Live Demo Runbook

This guide provides an exact, step-by-step master script for recording a portfolio video demo from a complete cold start. It showcases **Shift-Left Data Contracts**, **Medallion Processing**, **Cross-Layer Reconciliation**, **Idempotent Replays**, and **Enterprise OpenLineage Metadata Governance with Marquez**.

---

## ⚡ Quick Execution Summary

```bash
# 1. Start Fresh Environment (MinIO, Spark, Airflow, Marquez, Postgres)
docker compose up -d

# 2. Step 1: Shift-Left Data Contracts Validation & Evolution Diff
python contracts/contract_cli.py diff --old-contract contracts/schemas/ecommerce_events_v1.yml --new-contract contracts/schemas/ecommerce_events_v2.yml
make test-contracts

# 3. Step 2: Generate Demo Dataset & Ingest to MinIO
python scripts/create_demo_sample.py
python ingestion/kaggle_ingest.py --data-dir ./data/demo_sample --force

# 4. Step 3: PySpark Bronze & Incremental Silver MERGE
make run-bronze
make run-silver

# 5. Step 4: dbt Models & Full Invariant Test Suite (84 Tests)
make run-dbt
make test-dbt

# 6. Step 5: Cross-Layer Reconciliation Suite (Zero-Drift Assertion)
python checks/run_reconciliation.py

# 7. Step 6: Idempotent Date-Range Replay & Backfill Engine
python scripts/replay_events.py --start-date 2019-10-01 --end-date 2019-10-05 --mode backfill

# 8. Step 7: OpenLineage & Marquez Governance UI
# Open http://localhost:3001 in your browser
```

---

## 🎥 Master Live Video Demo Walkthrough

### 🛡️ Step 1: Shift-Left Data Contracts & Schema Quarantine
**Talking Point:** *"Before any data enters our lakehouse, we enforce strict shift-left Data Contracts using a custom CLI engine (`contract_cli.py`) to prevent upstream schema drift from breaking downstream models."*

```bash
# 1A. Test backward-compatibility between v1 and v2 contracts -> [COMPATIBLE]
python contracts/contract_cli.py diff \
  --old-contract contracts/schemas/ecommerce_events_v1.yml \
  --new-contract contracts/schemas/ecommerce_events_v2.yml

# 1B. Validate a clean dataset -> [PASS]
python contracts/contract_cli.py validate \
  --input-file data/test_valid.csv \
  --contract-file contracts/schemas/ecommerce_events_v1.yml

# 1C. Validate a breaking schema dataset -> [VIOLATION DETECTED & QUARANTINED]
python contracts/contract_cli.py validate \
  --input-file data/test_breaking.csv \
  --contract-file contracts/schemas/ecommerce_events_v1.yml
```
**Screen Display:** Show the terminal output flagging breaking changes and routing bad records to `data/quarantine/`.

---

### 📥 Step 2: Raw Ingestion to MinIO S3
**Talking Point:** *"We ingest clickstream events to MinIO S3 (`s3a://raw/ecommerce_events/`) using idempotent markers and retry backoffs."*

```bash
python scripts/create_demo_sample.py
python ingestion/kaggle_ingest.py --data-dir ./data/demo_sample --force
```
**Screen Display:** MinIO Web Console (`http://localhost:9001`) showing raw files.

---

### 🥉 Step 3: PySpark Bronze & Incremental Silver MERGE
**Talking Point:** *"PySpark parses raw events into a schema-enforced Bronze Delta Lake table. Then, our Silver layer performs incremental MERGE using SHA-256 event keys and an allowed lateness lookback buffer to prevent late data drops."*

```bash
make run-bronze
make run-silver
```

---

### 📊 Step 4: dbt Dimensional Modeling & Invariant Test Suite
**Talking Point:** *"dbt on DuckDB builds our analytical star schema (SCD Type 2 product dimensions, 30-minute idle sessionization, and Gold business marts) verified by 84 rigorous tests."*

```bash
make run-dbt
make test-dbt
```
**Screen Display:** Terminal showing `84 of 84 tests PASSED`.

---

### ⚖️ Step 5: Cross-Layer Reconciliation Suite
**Talking Point:** *"To check for data loss across multi-engine boundaries, our reconciliation runner compares row parity and revenue conservation between Silver Delta and DuckDB Marts."*

```bash
python checks/run_reconciliation.py
```
**Screen Display:** Terminal showing 0 drift on the three Gold-layer checks (staging events vs fact_events, purchase counts, purchase revenue vs mart_daily_summary); a check whose source is unavailable is recorded as SKIP and the process can still exit 0.

---

### 🔄 Step 6: Idempotent Replay & Backfill Engine
**Talking Point:** *"We built an isolated replay engine that reprocesses historical date ranges while keeping forward production watermarks completely isolated."*

```bash
python scripts/replay_events.py --start-date 2019-10-01 --end-date 2019-10-05 --mode backfill
```
**Screen Display:** Generated audit JSON under `audit/replay_*.json`.

---

### 🌐 Step 7: OpenLineage & Marquez Control Plane
**Talking Point:** *"Airflow emits job-run lineage under the OpenLineage standard via its transport, and dataset/schema facets for the Spark and dbt datasets are emitted to Marquez by `scripts/emit_marquez_lineage.py`."*

**Action:**
1. Open browser to **[http://localhost:3001](http://localhost:3001)** (Marquez Web UI).
2. Inspect the complete lineage graph: `s3a://raw` ➔ `s3a://bronze` ➔ `s3a://silver` ➔ `dbt staging` ➔ `dbt marts`.
3. Highlight dataset facets: schema versions, input/output row counts, and job run durations.
