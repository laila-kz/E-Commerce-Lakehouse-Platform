# ADR-002: Hybrid Event-Time Watermarking & Lateness Lookback for Incremental Silver Processing

## Status
**Accepted**

## Context
In e-commerce clickstream processing, event streams contain late-arriving records (e.g. mobile app caching, intermittent network disconnections, retry queues). A naive batch pipeline either:
1. Performs an expensive full table scan on every run.
2. Uses strict event-time filtering (`event_time > max(event_time)`), which permanently and silently drops any record that arrives with a timestamp older than the current high-watermark.
3. Updates the watermark state before the target table write commits, causing lost data if the job crashes mid-transform.

## Decision
We implemented a **Hybrid Event-Time Watermarking Strategy** with **Post-Commit State Advancement** and an **Allowed Lateness Lookback Buffer**:

1. **State Isolation:** The watermark is persisted in a dedicated Delta state table (`s3a://silver/ecommerce_events/_watermarks`) storing `last_processed_event_time`, `updated_at`, and `last_batch_id`.
2. **Post-Commit Advancement:** The watermark is updated *only after* the Delta `MERGE` operation completes and is verified via Delta's `DESCRIBE HISTORY` transaction log.
3. **Lateness Lookback Buffer:** Ingestion filters Bronze using an allowed lateness window (`event_time > watermark - INTERVAL '3 DAYS'`), allowing late-arriving events to be merged safely into historical partitions.
4. **Backfill Isolation:** When running in backfill mode (`--mode backfill`), the production high-watermark is untouched to prevent historical reprocessing from regressing the production watermark.

## Alternatives Considered
- **Strict Event-Time Watermark (No Buffer):** Simple, but causes permanent silent data loss for any out-of-order event.
- **Pure Ingestion-Time (Batch ID / File Timestamp):** Avoids late-data issues, but loses the ability to partition Silver by business event date efficiently.
- **Airflow Execution Date Partitioning:** Coupes pipeline logic to Airflow scheduler internals rather than data-driven state.

## Tradeoffs & Consequences
- **Positive:**
  - Complete crash-resilience: If a crash occurs after `MERGE` but before watermark update, re-running the job reprocesses the batch idempotently with zero duplicates.
  - Out-of-order and late-arriving records are preserved and merged into the correct historical partitions.
- **Negative:**
  - Reading with a lookback buffer requires Delta's `MERGE` operation to check a slightly wider partition range, incurring minor compute overhead.
