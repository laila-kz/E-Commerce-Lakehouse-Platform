# E-Commerce Lakehouse Upgrade — Execution Plan

Four sequential stages. Each stage should be fully closed out (README/tags updated) before starting the next — later stages depend on earlier ones being real, not just planned.

---

## Stage 1 — Baseline and Documentation

**Purpose:** Freeze an accurate description of what exists today before adding anything new. Everything you build in Stages 2–4 gets documented against this baseline.

- [x] Rewrite the README as a single current-state document
  - [x] Remove duplicated historical phase sections (old Phase 4/5 blocks appearing after the current phase list)
  - [x] Replace "production-ready" language with "production-patterned local implementation"
  - [x] Fix the local `file:///C:/...` link so it works for other readers
  - [x] Document that the Airflow DAG uses `demo_sample`
  - [x] Correct the dbt path claim — dims/facts live under `models/marts`, not a separate `core` directory
  - [x] Split setup commands into PowerShell and Linux sections
  - [x] Reorganize into: mission → architecture diagram → business questions → data flow → repo structure → local setup → example run → data quality/contracts → reliability experiments → metadata & lineage governance → design tradeoffs → limitations → resume bullets
- [x] Write 3–5 Architecture Decision Records (Context / Decision / Alternatives / Tradeoffs / Consequences)
  - [x] ADR-001: Why MinIO + Delta Lake locally
  - [x] ADR-002: Why Silver uses hybrid event-time watermarking & lateness lookback
  - [x] ADR-003: Why event keys use SHA-256
  - [x] ADR-004: Why dbt runs on DuckDB
  - [x] ADR-005: Why late data triggers partition-level (not full) rebuilds
  - [x] ADR-006: Why OpenLineage standard with Marquez for platform metadata & governance

**Stage 1 exit criteria:** README describes only what's actually built and runnable today; a stranger could clone the repo and reproduce Phase 0–9 from it without hitting a broken link or a stale claim. (COMPLETED)

---

## Stage 2 — Trust, Correctness, and Reliability

**Purpose:** Prove, systematically, that the pipeline doesn't lose, duplicate, or silently drop data across all layers, failure boundaries, and late-data events.

- [x] **End-to-End Late-Arriving Data Handling**
  - [x] **Silver:** Update `silver_transform.py` to add an allowed lateness lookback buffer (or ingestion-time filter) so Bronze rows with older timestamps are not silently dropped.
  - [x] **Gold:** Update `mart_daily_summary.sql` incremental logic to detect affected dates via `ingested_at > (select coalesce(max(last_run_at), '1970-01-01') from {{ this }})` and execute partition-level `delete+insert`.
  - [x] Test with a synthetic late-purchase file landing after the daily mart has already run.
- [x] **Cross-Layer Reconciliation Checks**
  - [x] Create `checks/run_reconciliation.py` (querying MinIO Delta Lake + DuckDB) asserting:
    - `raw ≥ bronze_valid + bronze_quarantined`
    - `bronze ≥ silver + filtered`
    - `silver event rows = fact_events rows`
    - `mart revenue = sum(fact_purchases.revenue)`
  - [x] Persist results to a `reconciliation_results` table with run metadata (check_name, source_layer, target_layer, source_count, target_count, diff, status, run_id).
- [x] **Platform Observability Metrics**
  - [x] Populate `pipeline_metrics` table per run: input/output row counts, quarantined count, duplicate count, null %, min/max event time, processing duration, throughput (rows/sec), watermark lag, late-event count.
- [ ] **Formal Failure-Injection Test Matrix**
  - [ ] Only the Silver post-MERGE boundary is instrumented today (`SIMULATE_CRASH_AFTER_MERGE` / `FAIL_AFTER_SILVER_MERGE` / `FAIL_BEFORE_WATERMARK_UPDATE`); instrument the remaining boundaries.
  - [ ] For each boundary: verify no lost/duplicated rows and correct watermark state.
  - [ ] Publish a verified pass/fail matrix in README under "Reliability, Contracts & Crash-Recovery Matrix" (currently states designed behavior only for the uninstrumented boundaries).
- [x] Write ADR-005 (Late Data & Partition Rebuild Strategy).

**Stage 2 exit criteria:** Reconciliation checks exist; the failure-injection matrix is partial — only the Silver post-MERGE boundary is instrumented, the rest are documented designed behavior. (PARTIAL)

---

## Stage 3 — Differentiating Platform Capabilities

**Purpose:** Add the two platform capabilities that most separate this from a standard medallion repo: isolated replay/backfills and versioned schema contracts with dead-letter routing.

- [x] **Replay & Backfill System (with Watermark Isolation)**
  - [x] `replay_events.py --source --start-date --end-date --mode backfill --dry-run`
  - [x] Support: date-range backfills, single-batch reprocess, Silver rebuild from Bronze, Gold rebuild from historical point.
  - [x] Implement `--mode backfill` isolation: bypasses forward watermark advancement so historical reprocessing never corrupts or regresses production watermarks.
  - [x] Persist replay runs into `replay_audit` table (replay_id, requested_at, start_date, end_date, mode, rows_processed, rows_inserted, status).
- [x] **Contract Versioning & Compatibility Registry**
  - [x] Classify schema changes: `COMPATIBLE` (add nullable column with a default), `WARNING` (add optional column without a default), `BREAKING` (drop column, rename column, change data type, add required column, tighten nullability).
  - [x] Version contract files (`ecommerce_events_v1.yml`, `_v2.yml`); validate incoming batches before Bronze ingestion.
  - [x] Route breaking batches to local `data/quarantine/` (contract CLI) with error metadata instead of failing the pipeline blindly; the Bronze transform quarantines malformed rows separately to `s3a://bronze/_quarantine/`.
  - [x] Schema registry table: dataset name, version, owner, effective date, compatibility status, SLA, latest validation result.
- [x] Update README with resume bullets for backfill engine and contract compatibility registry.

**Stage 3 exit criteria:** You can trigger a backfill across any historical date range without corrupting watermarks, and submit breaking schema files that get quarantined and logged to the contract registry. (COMPLETED)

---

## Stage 4 — Enterprise Governance: OpenLineage & Marquez Control Plane

**Purpose:** Deploy an industry-standard metadata & lineage platform (OpenLineage + Marquez) in Docker for Airflow job-run lineage and emitted dataset/schema facets.

- [x] **Marquez Platform Deployment (Docker)**
  - [x] Add `marquez` and `marquez-web` services to `docker-compose.yml`.
  - [x] Expose Marquez UI on port `3001` (to prevent Metabase port conflict) and API on port `5000`.
- [x] **OpenLineage Instrumentation across Pipeline Layers**
  - [x] **Spark:** Emit dataset inputs, outputs, and schemas to Marquez via `scripts/emit_marquez_lineage.py`; a Spark OpenLineage listener is not implemented (finding F-006).
  - [x] **dbt / Airflow:** Configure Airflow's built-in OpenLineage transport (`AIRFLOW__OPENLINEAGE__TRANSPORT`) in `docker-compose.yml` and `Dockerfile.airflow`.
- [x] **Incident Triage & Lineage Walkthrough in Marquez**
  - [x] Demonstrate complete end-to-end graph: `Raw (Files/S3)` ➔ `Bronze Delta` ➔ `Silver Delta` ➔ `Gold Marts`.
  - [x] Inspect dataset facets: Schema versions, Delta Lake commit versions, job run durations, and input/output row counts.
  - [x] Incident demo: Inject breaking schema change ➔ verify failed job event logged in Marquez with blast-radius visualization ➔ trigger backfill ➔ observe Marquez job status return to Success.
- [x] **Recruiter Walkthrough & Documentation**
  - [x] Update `docs/DEMO_AND_RUNBOOK.md` with Marquez demo script and CLI walkthrough.
  - [x] Embed Marquez architecture diagram & instructions in `README.md`.

**Stage 4 exit criteria:** Running the pipeline automatically emits OpenLineage events to Marquez; opening the Marquez UI renders the full end-to-end lineage DAG, dataset facets, and execution run history. (COMPLETED)

---

## Summary table

| Stage | Focus | Depends on | Output |
|---|---|---|---|
| 1 | Baseline & docs | Nothing — do first | Accurate README + ADRs |
| 2 | Trust & correctness | Stage 1 | Reconciliation runner, observability metrics, late-data fix, failure matrix |
| 3 | Platform capabilities | Stage 2 | Replay/backfill system (watermark-safe), contract versioning registry & quarantine |
| 4 | Enterprise Governance | Stage 3 | Dockerized OpenLineage + Marquez platform, automated lineage emission & demo walkthrough |