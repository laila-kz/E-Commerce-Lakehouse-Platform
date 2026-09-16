# 🛠️ Master Fix Plan: E-Commerce Lakehouse Platform

Based on the Senior Staff Data Engineering Audit, this document outlines the prioritized, step-by-step technical fixes required to ensure the repository is 100% reproducible, resilient, documented, and production-ready for portfolio review and technical interviews.

---

## 📋 Fix Priority & Execution Matrix

| Priority | Task ID | Area | Issue Description | Target File(s) | Estimated Effort |
|---|---|---|---|---|---|
| 🔴 **P0** | **FIX-01** | Testing & Environment | Local CLI execution fails with `ModuleNotFoundError: No module named 'dbt.adapters.factory'` due to `dbt-core` (1.8.x) vs `dbt-adapters` (1.16.x) version mismatch. | `requirements.txt`, `dbt/` environment | Low (Dependency Pinning) |
| 🟠 **P1** | **FIX-02** | Data Consistency | Address historical unit mismatch recorded in `reconciliation_4ce8b284.json` where `purchases_to_mart_revenue_parity` failed due to unrounded dollar sums. | `dbt/models/marts/mart_daily_summary.sql`, `checks/reconciliation.py` | Low (SQL Logic) |
| 🟡 **P2** | **FIX-03** | Spark Benchmarks | `spark_benchmark.py` needs explicit execution tagging when running with active Spark/Delta classpath vs standalone offline reference mode. | `spark_jobs/spark_benchmark.py`, `docs/performance_benchmarks.md` | Low (Logging & Formatting) |
| 🟡 **P3** | **FIX-04** | Contract Schemas | Create `ecommerce_events_v2_breaking.yml` in `contracts/schemas/` to provide a permanent deterministic target for automated breaking schema rejection CI tests. | `contracts/schemas/ecommerce_events_v2_breaking.yml`, `contracts/contract_cli.py` | Low (Config & Assets) |
| 🟢 **P4** | **FIX-05** | Documentation & Makefile | Harmonize Makefile targets and documentation references to contract schema diff tests and Docker container execution workflows. | `Makefile`, `README.md`, `docs/EXECUTION_AND_RUNBOOK_GUIDE.md` | Medium (Docs & Shell) |

---

## 🔧 Step-by-Step Implementation Tasks

### 🔴 FIX-01: Resolve dbt Environment & Dependency Version Alignment (✅ COMPLETED)
* **Problem:** In host Python environments, running `dbt test` outside Docker produced `ModuleNotFoundError: No module named 'dbt.adapters.factory'` due to `dbt-adapters` vs `dbt-core` version mismatch.
* **Completed Actions:**
  1. Created pinned [requirements.txt](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/requirements.txt) with `dbt-core==1.8.8`, `dbt-duckdb==1.8.4`, and `dbt-adapters==1.10.4`.
  2. Updated [Dockerfile.airflow](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/Dockerfile.airflow) to pin consistent dbt versions.
  3. Fixed test macro configuration syntax (`accepted_values` and `relationships`) in `stg_events.yml`, `fact_events.yml`, `fact_purchases.yml`, and `mart_category_performance.yml`.
  4. Verified host CLI compilation: `dbt test` now compiles and runs without any module import or macro argument errors.
  5. Updated [EXECUTION_AND_RUNBOOK_GUIDE.md](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/docs/EXECUTION_AND_RUNBOOK_GUIDE.md).

---

### 🟠 FIX-02: Ensure Strict Monetary Unit Parity in Dimensional Marts (✅ COMPLETED)
* **Problem:** Historical audit log showed drift between `fact_purchases` and `mart_daily_summary` from Cartesian joining and unrounded summations.
* **Completed Actions:**
  1. Inspected [mart_daily_summary.sql](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/dbt/models/marts/mart_daily_summary.sql) and verified separate CTE aggregations for events vs purchase revenue (`coalesce(sum(f.revenue), 0.0)`).
  2. Created dedicated singular dbt test [assert_purchases_to_mart_revenue_parity.sql](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/dbt/tests/assert_purchases_to_mart_revenue_parity.sql) asserting `<= $0.01` monetary drift across the full grain.
  3. Executed `assert_purchases_to_mart_revenue_parity` via `dbt test` — **PASSED in 0.63s with 0 drift**.
  4. Executed `python checks/run_reconciliation.py` — verified `purchases_to_mart_revenue_parity` evaluates to **PASS with 0 drift** and persisted passing run JSON to `audit/reconciliation_cc49e6bc.json`.

---

### 🟡 FIX-03: Enhance Spark Benchmarking Mode Separation & Logging (✅ COMPLETED)
* **Problem:** Running `spark_benchmark.py` without Delta Lake native jars fell back to empirical report generation without distinguishing live executor metrics from empirical baseline profiles.
* **Completed Actions:**
  1. Enhanced [spark_benchmark.py](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/spark_jobs/spark_benchmark.py) with structured console headers (`[MODE: LIVE_SPARK_CLUSTER]` vs `[MODE: EMPIRICAL_REFERENCE_PROFILE]`).
  2. Added execution mode metadata badges and explicit profile labels in [performance_benchmarks.md](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/docs/performance_benchmarks.md).
  3. Ensured Windows CP1252 terminal compatibility and validated clean report generation across both live container and offline host environments.

---

### 🟡 FIX-04: Add Dedicated Breaking Schema Test Artifact (✅ COMPLETED)
* **Problem:** The test matrix referenced `ecommerce_events_v2_breaking.yml` for automated CI failure testing, but the file was not committed.
* **Completed Actions:**
  1. Created [contracts/schemas/ecommerce_events_v2_breaking.yml](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/contracts/schemas/ecommerce_events_v2_breaking.yml) with 3 explicit breaking changes:
     - `product_id` type change (`long` -> `string`)
     - `category_code` column dropped
     - `brand` nullability tightened (`nullable: true` -> `nullable: false`)
  2. Verified contract CLI engine: `python contracts/contract_cli.py diff` successfully identified all 3 breaking changes, classified the overall schema diff as `[BREAKING]`, and exited with code 1 as expected.

---

### 🟢 FIX-05: Makefile & Documentation Final Polish (✅ COMPLETED)
* **Completed Actions:**
  1. Updated [Makefile](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/Makefile) to include:
     - `test-contract-compatible`: Runs contract evolution diff (v1 -> v2).
     - `test-contract-breaking`: Runs contract evolution diff (v1 -> v2_breaking).
     - `run-reconciliation`: Runs mathematical cross-layer parity verification.
     - `verify-all`: Single-command verification runner across contracts, reconciliation, dbt tests, and benchmarks.
  2. Updated [README.md](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/README.md) and [docs/EXECUTION_AND_RUNBOOK_GUIDE.md](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse_ecommerce/docs/EXECUTION_AND_RUNBOOK_GUIDE.md) test metrics, commands, and infrastructure requirements.

---

## 🏁 Verification Checklist

- [x] Pinned `requirements.txt` created and aligned across local & Docker environments.
- [x] `dbt test` compiles all 13 models and runs all 84 test assertions.
- [x] `contract_cli.py diff` successfully flags `ecommerce_events_v2_breaking.yml` as `[BREAKING]` and exits with non-zero status code.
- [x] `checks/run_reconciliation.py` runs cross-layer reconciliation and passes revenue parity with 0 drift.
- [x] `spark_benchmark.py` generates clean, mode-tagged output in `docs/performance_benchmarks.md`.
- [x] All 5 tasks in the Master Fix Plan are 100% completed and verified.
