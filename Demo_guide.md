Here is an end-to-end, structured runbook and video recording script to demonstrate the entire **E-Commerce Medallion Lakehouse Platform** from start to finish.

---

# 🎬 End-to-End Demo Video Guide & Runbook

## ⏱️ Video Timeline Overview (~4–5 Minutes)

| Scene | Duration | Component Covered | Action / Commands |
|---|---|---|---|
| **1. Intro & Architecture** | 0:45 | Architecture & Lineage Portal | Show GitHub Pages UI, medallion layers |
| **2. Infrastructure Spin-up** | 0:45 | Docker, MinIO, Spark, DuckDB | `docker compose up -d` & MinIO Console |
| **3. Ingestion & Bronze/Silver** | 1:15 | PySpark, Delta MERGE, Soda Gates | Ingestion, Bronze Delta, Silver MERGE |
| **4. Gold dbt & Invariant Testing** | 1:00 | DuckDB, SCD 2, 57 dbt tests | `dbt run` & `dbt test` |
| **5. Contracts & Reconciliation** | 0:45 | Schema Diff & $0.00 Drift Audit | `contract_cli.py` & `run_reconciliation.py` |
| **6. Airflow Orchestration & Wrap-up** | 0:30 | Airflow DAGs & TaskGroups | Airflow UI pipeline execution |

---

## 🚀 Step-by-Step Execution Guide

### Scene 1: Introduction & Live Interactive Portal
1. Open your browser to the deployed portal:
   👉 `https://laila-kz.github.io/E-Commerce-Lakehouse-Platform/`
2. **What to demonstrate on screen:**
   - **Lineage Explorer Tab**: Show the clean 7-layer Medallion DAG (`Source` &rarr; `Landing` &rarr; `Bronze` &rarr; `Silver` &rarr; `Gold` &rarr; `Marts` &rarr; `BI`).
   - Click on nodes like `fact_purchases` or `dim_product` to open the slide-out metadata drawer showing schemas and dbt SQL logic.
   - Toggle the **Theme Switcher** (Dark/Light mode).
   - Click the **Contracts & Quality** tab to show the backward/forward breaking change analyzer and the 11/11 passed Soda Quality Gates.

---

### Scene 2: Spin Up Infrastructure
Open your terminal in the project root:

```bash
# 1. Start all containerized services (MinIO, Spark, Airflow, DuckDB environment)
docker compose up -d

# 2. Verify containers are healthy
docker compose ps
```

* **Visual to show:** Open `http://localhost:9001` (MinIO Console: `admin` / `password123`) to show the empty storage buckets (`raw`, `bronze`, `silver`, `gold`).

---

### Scene 3: Raw Ingestion & PySpark Medallion Transforms (Bronze & Silver)

```bash
# Step 3.1: Ingest raw CSV feed into MinIO raw/ landing bucket
python ingestion/kaggle_ingest.py --data-dir data/demo_sample --force

# Step 3.2: Run Spark Bronze Transform (Schema enforcement & Quarantine routing)
docker exec spark /opt/spark/bin/spark-submit \
  --driver-memory 2g --executor-memory 2g \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark_jobs/bronze_transform.py

# Step 3.3: Run Bronze Soda Core Quality Gate (Schema validation & null checks)
docker exec spark python3 /workspace/checks/run_quality_gate.py --layer bronze

# Step 3.4: Run Spark Silver Transform (Delta MERGE, watermark lookback & SHA-256 deduplication)
docker exec spark /opt/spark/bin/spark-submit \
  --driver-memory 2g --executor-memory 2g \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark_jobs/silver_transform.py

# Step 3.5: Run Silver Soda Core Quality Gate (Deduplication & Referential integrity)
docker exec spark python3 /workspace/checks/run_quality_gate.py --layer silver
```

---

### Scene 4: Gold Warehouse Modeling with dbt & DuckDB

```bash
# Move into the dbt directory
cd dbt

# Step 4.1: Run all dbt staging, intermediate, dimensional, and mart models
dbt run --profiles-dir .

# Step 4.2: Execute all 57 dbt integrity & business rule tests
dbt test --exclude tag:s3_dependent --profiles-dir .

# Return to root directory
cd ..
```

* **Key talking point:** Highlight how DuckDB queries Delta tables in-process and tests SCD Type 2 non-overlapping validity windows (`dim_product`) and cohort retention calculations.

---

### Scene 5: Data Contract Schema Diff & Monetary Reconciliation

```bash
# Step 5.1: Run the Contract Evolution CLI to analyze a breaking contract change
python contracts/contract_cli.py diff \
  --old-contract contracts/schemas/ecommerce_events_v1.yml \
  --new-contract contracts/schemas/ecommerce_events_v2_breaking.yml

# Step 5.2: Run Cross-Layer Financial Reconciliation (Assert $0.00 drift)
python checks/run_reconciliation.py
```

* **Key talking point:** Explain how the breaking schema analyzer catches dropped columns and tightened types before they reach production, and how the reconciliation audit guarantees zero monetary drift between transactional events and daily reporting marts.

---

### Scene 6: Full Airflow DAG Orchestration & Summary Verification

```bash
# Single command to run the complete platform verification suite
make verify-all
```

* **Visual to show:** Open Airflow at `http://localhost:8080` (credentials: `admin` / `admin`).
* Show the DAG `ecommerce_lakehouse` with grouped `TaskGroup`s (`bronze_stage`, `silver_stage`, `dbt_warehouse_stage`) executing smoothly from green start to green finish.

---

## 🎙️ Video Speaking Script Cheatsheet

> *"Welcome! In this demo, I will walk you through our production-grade E-Commerce Medallion Lakehouse platform built with PySpark, Delta Lake, MinIO, DuckDB, dbt, Soda Core, and Apache Airflow.*
>
> *First, here is our interactive Lineage and Governance Portal. It maps our end-to-end data flow from Kaggle clickstream events through Bronze schema validation, Silver ACID merges with SHA-256 deduplication, into Gold star schemas and executive marts.*
>
> *Next, let's run the pipeline. We ingest raw events into our MinIO S3 landing zone, transform them into Delta Lake tables, and enforce strict Soda Core quality gates that isolate invalid records to a dead-letter quarantine.*
>
> *In our Gold layer, dbt and DuckDB model our dimensions—including SCD Type 2 product history—and aggregate financial marts. Over 57 invariant tests pass with 100% data integrity.*
>
> *Finally, our contract evolution engine flags breaking schema changes before they hit pipelines, and our cross-layer reconciliation audit proves exact $0.00 revenue drift across all layers.*
>
> *Thank you for watching!"*