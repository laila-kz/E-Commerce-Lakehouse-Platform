# Data Lakehouse Architecture Diagram

## Current State (End of Week 3)

### Data Flow

┌─────────────────────────────────────────────────────────────────────────────────┐
│ │
│ E-COMMERCE DATA LAKEHOUSE │
│ │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ Stage 1 │ │ Stage 2 │ │ Stage 3 │ │ Stage 4 │ │
│ │ Raw │ │ Bronze │ │ Silver │ │ Gold │ │
│ │ │ │ │ │ │ │ (Planned) │ │
│ │ - CSV files │ │ - Delta │ │ - Delta │ │ │ │
│ │ - Kaggle │ │ - Schema │ │ - Incremen- │ │ - Aggrega- │ │
│ │ imported │ │ enforced │ │ tal MERGE │ │ tions │ │
│ │ │ │ - Lineage │ │ - Dedup │ │ - Business │ │
│ │ │ │ - Quaran- │ │ - Category │ │ ready │ │
│ │ │ │ tine │ │ parsing │ │ │ │
│ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ │
│ │ │ │ │ │
│ ▼ ▼ ▼ ▼ │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ Ingestion │ │ Soda Core │ │ Soda Core │ │
│ │ Script │ │ Quality │ │ Quality │ │
│ │ (Python) │ │ Gate │ │ Gate │ │
│ │ │ │ │ │ │ │
│ │ ✅ Phase 1 │ │ ✅ Phase 2 │ │ ✅ Phase 3 │ │
│ └──────────────┘ └──────────────┘ └──────────────┘ │
│ │
│ Storage: │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ MinIO (S3-compatible) │ │
│ │ raw/ │ bronze/ │ silver/ │ gold/ │ reference/ │ logs/ │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│ │
│ Compute: │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ Apache Spark │ │
│ │ - Bronze Transform (schema enforcement, lineage, quarantine) │ │
│ │ - Silver Transform (MERGE, deduplication, watermarking) │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│ │
│ Orchestration (Planned): │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ Apache Airflow │ │
│ │ (Week 4 - Full DAG orchestration) │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│ │
│ Reliability: │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ Crash-Safety Proven │ │
│ │ - MERGE + Watermark boundary tested │ │
│ │ - Post-commit watermark advancement │ │
│ │ - Zero duplication on recovery │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│ │
└─────────────────────────────────────────────────────────────────────────────────┘


### Evolution Over Time

| Phase | Date | Added | Status |
|-------|------|-------|--------|
| Phase 0 | Week 1 | Docker Stack + MinIO + Spark | ✅ |
| Phase 1 | Week 1 | Ingestion Script | ✅ |
| Phase 2 | Week 2 | Bronze (Delta + Lineage + Soda) | ✅ |
| Phase 3 | Week 3 | Silver (MERGE + Watermark + Soda) | ✅ |
| Phase 4 | Week 4 | Gold + Airflow | 🔜 |