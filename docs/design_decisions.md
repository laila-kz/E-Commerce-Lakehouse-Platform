# Design Decisions - Data Lakehouse Project

## ✅ Spark-MinIO Connectivity - WORKING CONFIGURATION

**Date Tested:** 2026-07-19

### Spark Configuration for S3A (MinIO)

```python
spark = SparkSession.builder \
    .appName("MinIO Connectivity Test") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()



## Dataset: E-Commerce Behavior Data from Multi-Category Store

**Source:** Kaggle - https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

### Files Available:
- [List the files you see on the Data tab]
- Example: `2019-Oct.csv` (~500 MB)
- Example: `2019-Nov.csv` (~450 MB)

### Dataset Description:
- [What kind of data is it?]
- Example: Customer behavior data from an e-commerce store
- Contains: user sessions, product views, purchases, etc.

### Key Columns:
- [List main columns from the Data tab]
- Example: `event_time`, `user_id`, `product_id`, `category_code`, `price`, etc.



## Ingestion Service Design Specification

### Purpose
Ingest data from Kaggle API and upload to MinIO `raw` bucket with idempotency, retries, and structured logging.

---

### 1. Sequence of Steps

The script performs these steps in order:

1. **Authenticate**
   - Read `KAGGLE_USERNAME` and `KAGGLE_KEY` from environment variables
   - Initialize Kaggle API client

2. **Check if Already Ingested (Idempotency Check)**
   - Check if file exists in MinIO at: `s3a://raw/ecommerce_events/`
   - **Check condition:** Does a `_SUCCESS` file exist at `s3a://raw/ecommerce_events/_SUCCESS`?
   - If YES → Skip download, log `{"event": "skip_already_ingested"}`
   - If NO → Proceed to download

3. **Download Data**
   - Use Kaggle API to download dataset
   - Dataset: `mkechinov/ecommerce-behavior-data-from-multi-category-store`
   - Save temporarily to `/tmp/ecommerce_data/`

4. **Validate Downloaded Data**
   - Check that files were downloaded
   - Check that files are not empty (> 0 bytes)

5. **Extract/Process**
   - [Add extraction details if needed]

6. **Upload to MinIO**
   - Upload files to `s3a://raw/ecommerce_events/`
   - Create a `_SUCCESS` marker file after upload

7. **Structured Logging**
   - Emit JSON logs at each step

---

### 2. Idempotency Check

**Exact check:** Does an object exist at `s3a://raw/ecommerce_events/_SUCCESS`?

| Condition | Action |
|-----------|--------|
| `_SUCCESS` exists | Skip download, log "already ingested" |
| `_SUCCESS` does NOT exist | Proceed with download and processing |

**Why this works:** The `_SUCCESS` file is only created after a successful upload. If it exists, we know the data is already in MinIO.

---

### 3. Retry Policy

**Backoff Strategy:** Exponential backoff with jitter

| Attempt | Delay |
|---------|-------|
| 1st retry | Wait 1 second |
| 2nd retry | Wait 2 seconds |
| 3rd retry | Wait 4 seconds |
| 4th retry | Wait 8 seconds |
| 5th retry | Wait 16 seconds |

**Max attempts:** 5 total tries (initial + 4 retries)

**When to retry:**
- Network errors
- Timeout errors
- 5xx server errors

**When NOT to retry:**
- Authentication errors (401)
- Permission errors (403)
- Invalid request errors (400)

---

### 4. Structured Logging Format

All logs must be JSON format with these fields:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `event` | string | What happened | `"download_start"`, `"download_success"` |
| `status` | string | `"success"` or `"failure"` | `"success"` |
| `duration_ms` | integer | Time taken in milliseconds | `1234` |
| `bytes` | integer | Size of data processed | `48213911` |
| `error` | string | Error message (if failure) | `"Connection timeout"` |

**Example log output:**
```json
{"event": "download_start", "status": "running", "timestamp": "2026-07-19T22:00:00Z", "dataset": "ecommerce-behavior"}
{"event": "download_success", "status": "success", "duration_ms": 2345, "bytes": 48213911, "file_count": 3}
{"event": "upload_start", "status": "running", "timestamp": "2026-07-19T22:00:03Z"}
{"event": "upload_success", "status": "success", "duration_ms": 1234, "bytes": 48213911}
{"event": "ingestion_complete", "status": "success", "total_duration_ms": 3579}


## Implementation Reconciliation (2026-07-20)

### ✅ What Was Implemented as Planned
- All core functionality from the design doc
- Idempotency via MinIO `_SUCCESS` marker
- Structured JSON logging
- Retry with exponential backoff

### 🔄 Deliberate Deviations
1. **Manual upload for large files**
   - Reason: Network limitations
   - Impact: Script still works for any file size, just took time

2. **No automatic cleanup of local files**
   - Reason: Keep for debugging/backup
   - Impact: May need to clean manually

### 📝 Future Improvements
- Add automatic cleanup of local files after upload
- Add checksum validation (MD5)
- Add email notifications on failure



## Bronze Schema — Field Inventory

**Dataset:** E-Commerce Behavior Data from Multi-Category Store
**Source:** Kaggle (mkechinov)
**Sample Files:** 2019-Oct.csv, 2019-Nov.csv

### Field Inventory (Based on Real Data Inspection)

| # | Column Name | Sample Values | Apparent Type | Edge Cases Observed |
|---|-------------|---------------|---------------|---------------------|
| 1 | `event_time` | 2019-11-01 00:00:00 UTC | Timestamp | All rows have timestamps |
| 2 | `event_type` | view, purchase, cart | String | Values: view, cart, purchase, remove_from_cart |
| 3 | `product_id` | 1003461, 5000088 | Long | Always numeric, no nulls |
| 4 | `category_id` | 2053013555631882655 | Long | Always numeric, no nulls |
| 5 | `category_code` | electronics.smartphone, **empty** | String | **NULL/empty values exist!** (rows 3, 4 in samples) |
| 6 | `brand` | xiaomi, lg, **empty** | String | **NULL/empty values exist!** (row with sofa brand is empty) |
| 7 | `price` | 489.07, 28.31 | Double | Always numeric, no nulls |
| 8 | `user_id` | 520088904, 530496790 | Long | Always numeric, no nulls |
| 9 | `user_session` | 4d3b30da-a5e4-49df-b1a8-ba5943f1dd33 | String | Always present (UUID format) |

### Observations from Real Data:

| Observation | Evidence | Impact |
|-------------|----------|--------|
| `category_code` is sometimes empty | Row 3: `,,creed,28.31` (empty between commas) | Must be nullable |
| `brand` is sometimes empty | Row 4: `sofa,,543.10` (empty between commas) | Must be nullable |
| `user_session` is always a UUID | Format: `4d3b30da-a5e4-49df-b1a8-ba5943f1dd33` | String, non-nullable |
| `price` is always numeric | All samples have decimal values | Double, non-nullable |
| `event_type` is always present | view, purchase, cart | String, non-nullable |
| `category_id` is extremely large | 2053013555631882655 | Long, non-nullable |

### Important Decision:

Since we observed empty values for `category_code` and `brand` in the real data, **these fields MUST be nullable** in our Bronze schema. If we mark them non-nullable, Spark will reject valid rows that have empty values.



## Bronze Schema Design

### Explicit StructType Schema (Based on Real Data)

| Column | Spark Type | Nullable? | Reasoning |
|--------|------------|-----------|-----------|
| `event_time` | TimestampType | No | Every event has a timestamp in every row |
| `event_type` | StringType | No | Always one of: view, cart, purchase, remove_from_cart |
| `product_id` | LongType | No | Always a numeric ID |
| `category_id` | LongType | No | Always a numeric category ID |
| `category_code` | StringType | **Yes** | **Real data has empty values!** (must allow null) |
| `brand` | StringType | **Yes** | **Real data has empty values!** (must allow null) |
| `price` | DoubleType | No | Always a decimal number |
| `user_id` | LongType | No | Always a numeric user ID |
| `user_session` | StringType | No | Always a UUID string |

### Complete PySpark Schema:

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    TimestampType, LongType, DoubleType
)

BRONZE_EVENT_SCHEMA = StructType([
    StructField("event_time", TimestampType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("product_id", LongType(), nullable=False),
    StructField("category_id", LongType(), nullable=False),
    StructField("category_code", StringType(), nullable=True),   # IMPORTANT: nullable
    StructField("brand", StringType(), nullable=True),           # IMPORTANT: nullable
    StructField("price", DoubleType(), nullable=False),
    StructField("user_id", LongType(), nullable=False),
    StructField("user_session", StringType(), nullable=False)
])


## Silver Layer: Watermark Design

### Watermark Storage Location
A Delta table at `s3a://silver/_watermarks/` is the preferred storage mechanism. It keeps the watermark state alongside the Silver data in the same storage layer, is easy to inspect, and supports atomic updates with Delta transactions.

### Watermark Table Schema
The watermark table should contain:
- `pipeline_name` — identifies the pipeline or job producing the Silver data
- `last_processed_event_time` — the highest event timestamp successfully processed
- `updated_at` — timestamp of the most recent watermark update
- `last_batch_id` — identifier of the batch that produced the last successful commit

### Update Timing (CRITICAL!)
The watermark advances only after the MERGE operation completes successfully and the transaction is committed. This ensures the watermark represents durable, fully applied data and prevents partially processed state from being treated as complete.

### What Goes Wrong If You Advance First
If the watermark advances before the MERGE succeeds, a downstream retry can skip rows that were never merged. Example: the pipeline processes a batch ending at `2026-01-10T10:00:00Z`, updates the watermark to that timestamp, then crashes during the MERGE. A retry starts from the next batch, but the earlier batch’s rows are now considered already processed and are silently skipped, causing data loss.

### Initial Run (No Watermark)
When the watermark table is empty, the initial Silver run should process the full available Bronze range from the earliest available data and then write the initial watermark after the first successful MERGE commit.

### What Value Does Watermark Track?
The watermark should track `event_time`, not `ingested_at`. `event_time` is the business event timestamp and is the correct basis for incremental processing. `ingested_at` is a pipeline metadata field and can vary depending on ingestion timing.

### Edge Case: Late-Arriving Data
If data arrives later than the watermark threshold, it should not be dropped. The pipeline should continue to process late-arriving events by re-reading the relevant Bronze partitions or by using a bounded reprocess window, then re-running the MERGE logic with the same deterministic deduplication key.


## Silver Layer: Synthetic Deduplication Key

### Why We Need a Synthetic Key
The dataset does not have a natural primary key because clickstream events are observational records rather than true entity records. Multiple rows can represent the same event from the same user and product at the same time across retries, re-ingests, or replays, so a deterministic synthetic key is required to make the Silver table idempotent.

### Key Design
The synthetic key should be built from these fields in this exact order:
1. `user_id`
2. `event_type`
3. `product_id`
4. `event_time`

The fields must be concatenated using the pipe character `|`.

### Hash Function & Format
- Hash: SHA-256
- Concatenation Format: `field1|field2|field3|field4`
- Timestamp Format: ISO-8601 UTC (e.g., `2019-11-01T00:00:00Z`)

### Determinism Test (Manual Verification)
Using the sample row from the provided data:
- `event_time = 2019-11-01 00:00:00 UTC`
- `event_type = view`
- `product_id = 1003461`
- `user_id = 520088904`

The canonical string is:
`2019-11-01T00:00:00Z|view|1003461|520088904`

The SHA-256 hash is:
`095b66e6d760b7b354d776dbec3cedcfed585a8c6aaadab40ac46b2ce775c1fb`

### Implementation in Spark (Pseudocode)
```sql
sha2(concat_ws("|", user_id, event_type, product_id, date_format(event_time, "yyyy-MM-dd'T'HH:mm:ss'Z'")), 256)
```


## Silver Layer: MERGE INTO Behavior

### Match Condition
The MERGE should use the deterministic synthetic key as the join condition:
`ON target.event_key = source.event_key`

### Behavior on Match (WHEN MATCHED)
No `WHEN MATCHED` branch is needed. Clickstream events are immutable facts, so a matched row should be treated as already present and left unchanged.

### Behavior on No Match (WHEN NOT MATCHED)
All rows that do not already exist in the Silver table should be inserted.

### Complete MERGE Statement
```sql
MERGE INTO silver_events AS target
USING staged_events AS source
ON target.event_key = source.event_key
WHEN NOT MATCHED THEN
  INSERT *
```

### Why This Design Fits Clickstream Data
This insert-only MERGE pattern is correct because clickstream events are append-only facts. There is no business need to update an existing event once it has been written into Silver, and preserving immutability avoids accidental overwrites or reprocessing drift.


## Silver Layer: Business Rule Filters

### Filter Order (Performance-Optimized)
The filters should run in this order for efficiency:
1. `user_session IS NOT NULL`
2. `price >= 0`
3. `event_time BETWEEN '2019-01-01' AND CURRENT_DATE()`

This ordering removes the most likely invalid rows early, reducing the amount of data that needs to pass through subsequent checks.

### Filter 1: Null user_session
Predicate:
```sql
WHERE user_session IS NOT NULL
```

### Filter 2: Negative Price
Predicate:
```sql
WHERE price >= 0
```

### Filter 3: Event_time Range
Predicate:
```sql
WHERE event_time >= '2019-01-01' AND event_time <= CURRENT_DATE()
```

### Logging Each Filter
Each filter should emit a structured JSON log with the number of rows dropped, the filter name, and the batch identifier so the lineage of business-rule decisions is easy to audit.

### Why No Quarantine for Business Rules?
No quarantine table is required for these business rules because they represent intentional data-quality decisions rather than schema violations. These rules are part of the Silver transformation contract and should be enforced as part of the normal filter flow.


## Silver Layer: category_code Parsing

### Source Data Format
The Bronze column `category_code` may appear as dot-delimited strings such as:
- `electronics.smartphone`
- `appliances.kitchen.washer`

### Parsing Specification
- Target columns: `category_l1`, `category_l2`, `category_l3`
- Split the value on `.` and map the first three levels to the target columns
- Missing levels should become `NULL`

### Edge Cases Table
| Input | category_l1 | category_l2 | category_l3 |
|-------|-------------|-------------|-------------|
| `electronics.smartphone` | `electronics` | `smartphone` | `NULL` |
| `appliances.kitchen.washer` | `appliances` | `kitchen` | `washer` |
| `electronics` | `electronics` | `NULL` | `NULL` |
| `NULL` | `NULL` | `NULL` | `NULL` |

### Implementation in Spark (Pseudocode)
```sql
split(category_code, '\.') as parts
when(size(parts) >= 1, parts[0]).otherwise(null) as category_l1
when(size(parts) >= 2, parts[1]).otherwise(null) as category_l2
when(size(parts) >= 3, parts[2]).otherwise(null) as category_l3
```

### Why "Extra Levels" Are Dropped
Only the first three levels are preserved to keep the Silver schema simple and stable. Extra levels beyond the third are intentionally dropped because the agreed contract only requires three hierarchical levels.


## Silver Layer: Implementation Guide Reconciliation

### Check Against Requirements
| Guide Requirement | Our Design | Status |
|-------------------|------------|--------|
| Watermark stored in Delta | Delta table at `s3a://silver/_watermarks/` | ✅ |
| Watermark updates after MERGE | Yes, after successful commit | ✅ |
| Synthetic deduplication key | `user_id|event_type|product_id|event_time` hashed with SHA-256 | ✅ |
| MERGE uses event_key | Yes | ✅ |
| Filters applied before dedup | Yes, business rules are applied before deduplication | ✅ |
| category_code parsing matches expectations | `category_l1`, `category_l2`, `category_l3` | ✅ |

### Deviations from Guide
No material deviations were introduced. The design stays aligned with the implementation guide while making the watermark and business-rule behavior explicit.

### Consistency Check
The design is consistent with the implementation guide: the MERGE match condition uses `event_key`, business-rule filters are applied before deduplication, and the parsed category columns align with the agreed Silver contract.


## Silver Design: Learning Checkpoint

### Q1: Why must the watermark advance only after successful MERGE?
Because advancing too early can cause silent data loss if the MERGE fails after the watermark update. The next run may skip the batch that was never applied.

### Q2: What fields make up the deduplication key and format?
The deduplication key is built from `user_id|event_type|product_id|event_time`, using ISO-8601 UTC formatting for the timestamp and then hashing the concatenated string with SHA-256.

### Q3: Why no update branch in MERGE?
Clickstream events are immutable facts, so there is no need to update an existing row once it has been written to Silver.


## Crash Recovery Test - Resilience Report

**Date:** 2026-07-29
**Test Engineer:** Self
**Pipeline:** Silver Incremental Transform

### Test Scenario
Simulated a crash immediately after MERGE completion and verification, but before watermark advancement.

### Injection Method
Environment variable `SIMULATE_CRASH_AFTER_MERGE=true` triggered a runtime error after `merge_verify_complete` but before `advance_watermark`.

### Expected Behavior (Per Design)
- MERGE commits data to Silver ✅
- Watermark remains at previous value ✅
- Next run reprocesses same range, MERGE skips existing rows ✅
- No data loss, no duplication ✅

### Observed Results

| Phase | Expected | Actual | Status |
|-------|----------|--------|--------|
| Initial run | MERGE completes, crash before watermark | MERGE completed, watermark NOT advanced | ✅ |
| Torn state | Data in Silver, watermark not advanced | Data present, watermark at old value | ✅ |
| Recovery run | Re-running an already-merged batch inserts 0 rows; watermark advances | Not yet covered by an automated test | ⏳ |

### Verification Queries

**Pre-crash Silver count:** 2,700,000
**Post-crash Silver count:** 2,700,005 (5 new rows)
**Pre-recovery watermark:** 2026-07-28 23:59:59
**Post-recovery watermark:** 2026-07-29 00:00:00

### Conclusion
**Intended behavior at this boundary — not yet covered by an automated failure-injection test:**
- Rows already committed to Silver are not lost by re-processing the range
- Re-inserting the already-merged batch adds 0 rows (MERGE `event_key` match); within-batch duplicate keys are collapsed deterministically before the write
- Watermark advancement is the last step, so a crash before it means the next run reprocesses the range

### Why This Works
1. Delta ACID guarantees ensure MERGE commits atomically
2. `event_key` matching ensures idempotent MERGE operations
3. Watermark advancement is the last step, so a crash before it means the next run simply reprocesses the range (which is safe)

### Future Considerations
- This test should be re-run after any changes to the Silver transform logic
- The crash injection code is kept as a documented testing tool

## Silver Layer: Volume Check Evolution

### Day 11 (Week 2) - Simplified Version
```yaml
- row_count > 1000:
    name: "Bronze table has data"
    description: "Table should contain at least 1000 rows"

   Note: This was an honest simplification because we didn't have multiple days of run history yet.

Day 17 (Week 3) - Evolved Version
sql
WITH current_batch AS (
  SELECT COUNT(*) as current_count
  FROM silver_ecommerce_events
  WHERE event_date >= CURRENT_DATE() - INTERVAL 1 DAY
),
trailing_avg AS (
  SELECT AVG(batch_count) as avg_count
  FROM (
    SELECT COUNT(*) as batch_count
    FROM silver_ecommerce_events
    WHERE event_date < CURRENT_DATE() - INTERVAL 1 DAY
    GROUP BY event_date
    ORDER BY event_date DESC
    LIMIT 3
  )
)
SELECT 
  current_count,
  avg_count,
  ABS(current_count - avg_count) / avg_count as deviation_rate
FROM current_batch, trailing_avg
WHERE ABS(current_count - avg_count) / avg_count > 0.30
Why This Evolution:

We now have multiple days of real run history (Week 2-3)

Trailing-average comparison is more meaningful than a static threshold

Catches gradual volume changes (data source growth) and sudden anomalies

30% tolerance is generous enough to avoid false alarms with limited history

What This Check Catches:

Upstream data volume changes (business growth)

Pipeline bugs causing data loss

Pipeline bugs causing duplication

Unexpected changes in source data


## Raw Layer: Quality Checks Implementation Decision

**Date:** 2026-08-11

### Decision: Custom Python Script (boto3-based)

### Reasoning:

**Why NOT Soda YAML:**
- Raw data is not in a Delta/Parquet format yet
- Soda Core's Spark/DuckDB connectors expect tabular data
- Raw CSVs are unstructured at this stage (no schema enforced)
- Forcing Soda would require additional parsing complexity

**Why Custom boto3 Script:**
- Raw data is just files in MinIO
- Checks needed: existence, file size, partition structure, row count
- These are simpler to implement with direct object storage inspection
- Avoids unnecessary tool complexity
- Follows same pattern as Day 6's MinIO client code

**What Raw Checks Are Appropriate:**
1. ✅ Expected partition structure exists (`ingested_date=YYYY-MM-DD/`)
2. ✅ CSV files are non-empty (size > 0)
3. ✅ Basic row count exists (approximate line count)
4. ✅ Most recent partition is recent (freshness check)

**Why This Matters:**
- This is a pragmatic tool choice decision
- Avoids over-engineering a simple need
- Demonstrates tool-fit thinking (valuable interview signal)

### Implementation

A small Python script at `soda/run_raw_checks.py` using `boto3` client.


## Data Quality Framework: Three-Layer Check Suite Design

### Layer Comparison

| Layer | Format | Tool | Focus |
|-------|--------|------|-------|
| Raw | Python script | boto3 | Structural/existence |
| Bronze | YAML | Soda Core | Schema, nulls, freshness |
| Silver | YAML | Soda Core | Dedup, business rules, volume |

### Why Different Tools for Different Layers

**Raw:** Files in MinIO, no schema yet → simple script is cleaner
**Bronze:** Delta table, schema enforced → Soda Core ideal
**Silver:** Delta table, business logic applied → Soda Core ideal

### Consistency Across Suites

All checks share:
- Same MinIO connection configuration
- Same JSON logging pattern
- Same exit code behavior (0 = pass, non-zero = fail)
- Same failure-path testing discipline

### Check Suites Summary

| Check Suite | # Checks | What It Validates |
|-------------|----------|-------------------|
| Raw | 5 | Bucket exists, partition exists, files non-empty, freshness, row count |
| Bronze | 11 | Schema, nulls, freshness, volume, duplicates |
| Silver | 5 | Duplicates, business rules, category validity, trailing-average volume |

### Run Order

1. Raw checks → fail early if ingestion broke
2. Bronze checks → validate shape
3. Silver checks → validate business logic outcomes


## Quality Gate: Unified Runner Design

**Date:** 2026-08-11

### Decision: Run All Suites Regardless of Early Failures

**Reasoning:**
- Provides complete picture of all failures in one run
- A Raw failure shouldn't hide Bronze/Silver issues
- Better debugging experience (see everything wrong at once)
- Safer for production (know if multiple layers are broken)

### Overall Exit Code Computation
overall_exit_code = 0 if ALL suites passed else 1


**Implementation:**
- Track each suite's result independently
- `overall_passed = True` initially
- If any suite fails, `overall_passed = False`
- Return `0` if overall_passed else `1`

**This prevents the "later pass masks earlier failure" bug.**

### Report Structure

```json
{
  "timestamp": "2026-08-11T12:00:00Z",
  "overall_status": "PASSED" | "FAILED",
  "suites": {
    "raw": {
      "status": "PASSED" | "FAILED",
      "exit_code": 0,
      "output": "...",
      "duration_seconds": 1.2
    },
    "bronze": {
      "status": "PASSED" | "FAILED",
      "exit_code": 0,
      "output": "...",
      "duration_seconds": 3.5
    },
    "silver": {
      "status": "PASSED" | "FAILED",
      "exit_code": 0,
      "output": "...",
      "duration_seconds": 4.1
    }
  },
  "summary": {
    "passed": 3,
    "failed": 0,
    "total": 3
  }
}

Why This Design
Same structured logging pattern as all other scripts

Produces a single, audit-ready artifact

Clear pass/fail signal for Airflow

Clear visibility into all failures


## Day 29 - Dimensional Design: Grain, Surrogate Keys & SCD Type 2 Strategy

### 1. Hand-Rolled Type 2 SQL vs. dbt Snapshots for `dim_product`
- **Decision:** Hand-roll SCD Type 2 history in `dim_product.sql` using window functions (`LAG()`, `LEAD()`, and running sums).
- **Reasoning:** In an event stream architecture, point-in-time attribute changes (`price`, `category_code`) are embedded directly inside events in `int_events_enriched`. Rather than taking external operational database snapshots, deriving historical validity windows (`valid_from`, `valid_to`) from the event stream guarantees deterministic point-in-time correctness for every event.

### 2. Tracked vs. Untracked Attributes (`dim_product`)
- **Type 2 Tracked Columns (trigger a new version row):** `price`, `category_l1`, `category_l2`, `category_l3`.
- **Type 1 Untracked Columns (overwritten in place):** `brand`.
- **Reasoning:** Financial and category reporting require evaluating historical purchases against the price and taxonomy in effect at event time. Brand is static per product ID.

### 3. Surrogate Key Strategy
- `dim_date`: Integer key `YYYYMMDD` (e.g., `20191001`).
- `dim_customer`: MD5 hash of `user_id` (`md5(cast(user_id as varchar))`).
- `dim_product`: MD5 hash of `product_id` + `valid_from` (`md5(cast(product_id as varchar) || '_' || cast(valid_from as varchar))`).

### 4. `dim_customer` Scope (Type 1 Thin Dimension)
- **Attributes:** `customer_key`, `user_id`, `first_seen_at`, `first_seen_date_key`.
- **Decision:** Intentionally thin Type 1 dimension. Aggregates (e.g., total lifetime orders, lifetime spend) belong in Gold-layer marts rather than inflating the dimension.

### 5. Fact-to-Dimension Time-Ranged Join Condition
- To resolve historical product version per event:
  `fact.event_time >= dim_product.valid_from AND (fact.event_time < dim_product.valid_to OR dim_product.valid_to IS NULL)`


### 6. `fact_purchases` Grain
- **Grain:** One row per purchase event (`event_type = 'purchase'`).


## Day 36 - Gold Mart Design Specifications

### 1. Business Questions
- `mart_daily_summary`: "For any given day, what were total events, total purchases, total revenue, and unique active users, broken down by category?"
- `mart_customer_retention`: "Of customers who first purchased in month X, what fraction returned to purchase again in months X+1, X+2, X+3...?"
- `mart_category_performance`: "Which product categories are growing or shrinking in revenue and order volume, period over period?"

### 2. Grain Specifications
- `mart_daily_summary`: One row per `(date, category_l1)`.
- `mart_customer_retention`: One row per `(cohort_month, months_since_first_purchase)`.
- `mart_category_performance`: One row per `(category_l1, period)` where period is monthly (`DATE_TRUNC('month', date)`).

### 3. Materialization Strategy
- `mart_daily_summary`: `incremental` using `unique_key = ['date', 'category_l1']`. Rebuilding historical daily rollups from scratch every run is wasteful; incremental processing efficiently appends/updates daily slices. Filter: `event_date > (SELECT MAX(date) FROM {{ this }})`.
- `mart_customer_retention`: `table`. Retention curves require full recomputation across historical cohorts as new purchase activity updates retention status for past cohorts.
- `mart_category_performance`: `table`. Period-over-period comparisons across window partitions require complete historical context.

### 4. Cohort Retention Logic Spec
1. Derive `cohort_month` per customer from their first purchase in `fact_purchases` (`DATE_TRUNC('month', MIN(purchased_at))`).
2. Generate complete calendar month grid from `dim_date` spanning min cohort month to max event date.
3. Cross-join customers to all calendar months from their `cohort_month` onwards.
4. Calculate `months_since_first_purchase` = integer month diff between activity month and `cohort_month`.
5. Left-join purchase activity from `fact_purchases` to set `made_purchase` flag (true/false).
6. Aggregate by `(cohort_month, months_since_first_purchase)` to compute `cohort_size`, `retained_count`, and `retention_rate`.

### 5. Growth Rate Spec & Zero-Division Handling
- `prior_period_revenue` = `LAG(revenue) OVER (PARTITION BY category_l1 ORDER BY period)`.
- `revenue_growth_rate` = `(revenue - prior_period_revenue) / NULLIF(prior_period_revenue, 0)`.
- Explicit labeling (`growth_rate_label`):
  - When `prior_period_revenue IS NULL` → `'new_category'`
  - When `prior_period_revenue = 0` → `'no_prior_revenue'`
  - Otherwise → `'comparable'`

### 6. Planned Exposures Metadata (for Day 41)
- `daily_executive_dashboard`: Consumes `mart_daily_summary`.
- `retention_analytics_dashboard`: Consumes `mart_customer_retention`.
- `category_performance_dashboard`: Consumes `mart_category_performance`.


## Phase 6 Completion Summary (Day 42 — 2026-08-26)

### Final Validation Results
| Run | Command | Result |
|-----|---------|--------|
| Full refresh (build) | `dbt run --full-refresh` | ✅ 13/13 models built — 0 ERR |
| Full test suite | `dbt test` | ✅ 84/84 PASS — 0 ERR, 0 WARN |
| Scoped exposure rebuild | `dbt run --select +exposure:daily_executive_dashboard` | ✅ 10 upstream models — correct lineage |

### Incremental Equivalence Proof (`mart_daily_summary`)
1. `dbt run --full-refresh --select mart_daily_summary` → built correctly
2. `dbt run --select mart_daily_summary` (incremental) → ran with `is_incremental()` filter, NO-OP on stable data (no new rows inserted because max(date) already at ceiling)
3. Grain uniqueness test `assert_mart_daily_summary_unique_grain` → PASS in both runs

### Domain Invariants Verified (`mart_customer_retention`)
- `assert_retention_month_zero_is_100` → PASS: retention_rate = 1.0 for all cohorts at months_since_first_purchase = 0
- `assert_retention_counts_non_increasing` → PASS: retained_count <= cohort_size for all rows

### Key Files Delivered
| File | Purpose |
|------|---------|
| `dbt/models/marts/mart_daily_summary.sql` | Incremental daily rollup by category |
| `dbt/models/intermediate/int_customer_month_activity.sql` | Customer-cohort month grid |
| `dbt/models/marts/mart_customer_retention.sql` | Cohort retention table |
| `dbt/models/marts/mart_category_performance.sql` | Monthly category revenue growth |
| `dbt/models/marts/exposures.yml` | 3 downstream dashboard consumers |
| `dbt/tests/assert_retention_month_zero_is_100.sql` | Domain invariant: month-0 = 100% |
| `dbt/tests/assert_retention_counts_non_increasing.sql` | Domain invariant: retained ≤ cohort |
| `dbt/tests/assert_mart_daily_summary_unique_grain.sql` | Grain uniqueness |
| `dbt/tests/assert_mart_category_performance_unique_grain.sql` | Grain uniqueness |

### Git Tag
`v0.9-week6-gold-marts-complete` — on merge commit into `main`


## Day 43 — Airflow Orchestration Design (Phase 6 / Week 7)

### 1. Integration Strategy: `BashOperator` vs. Astronomer Cosmos
- **Decision:** Use `BashOperator` with layer-granular task grouping (`dbt_run_staging`, `dbt_run_intermediate`, `dbt_run_dims_facts`, `dbt_run_marts`, `dbt_test_gate`).
- **Rationale:** The dbt project uses DuckDB (`dev.duckdb`) stored directly in the workspace directory. Cosmos provides native model-level Airflow DAG generation, but requires running dbt through specialized containerized setup or Python virtual environments. Layer-based `BashOperator` execution provides clean, explicit layer isolation, straightforward debugging inside the mounted Docker volume, and predictable execution without external parser complexity while keeping per-layer failure visibility high.

### 2. Complete Task Dependency Graph & Failure Semantics
```
ingest_raw
   │
   ▼
bronze_transform
   │
   ▼
bronze_quality_gate  ◄── [MUST PASS: soda scan exit 0]
   │
   ▼
silver_transform
   │
   ▼
silver_quality_gate  ◄── [MUST PASS: soda scan exit 0]
   │
   ▼
dbt_run_staging
   │
   ▼
dbt_run_intermediate
   │
   ▼
dbt_run_dims_facts
   │
   ▼
dbt_run_marts
   │
   ▼
dbt_test_gate
```

**Quality Gate Halting Semantics:** Downstream steps do NOT merely depend on prior step *completion*, but on explicit *success* (`exit_code == 0`). Airflow's default `trigger_rule="all_success"` enforces this natively because both `soda/run_soda_scan.py` and `checks/run_quality_gate.py` return exit code `1` on check failure, causing Airflow to mark downstream tasks as `upstream_failed` and halt pipeline progression immediately.

### 3. Task Command & Parameter Mapping
All tasks run inside the Airflow container (`cwd` = working dir). Spark tasks shell out with `docker exec spark`, so the Spark job/script path is `/workspace/...` (the mount inside the **spark** container); quality gates also run through `docker exec spark`. dbt commands run directly in the **airflow** container.

| Task Name | Container | Executor Command | Working Dir | Jinja Parameters |
|-----------|-----------|------------------|-------------|------------------|
| `ingest_raw` | airflow | `python /opt/airflow/project/ingestion/kaggle_ingest.py --data-dir /opt/airflow/project/data/demo_sample --force` | `/opt/airflow/project` | N/A |
| `bronze_transform` | spark | `docker exec spark /opt/spark/bin/spark-submit --driver-memory 2g --executor-memory 2g --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/bronze_transform.py` | `/opt/airflow/project` | N/A |
| `bronze_quality_gate` | spark | `docker exec spark python3 /workspace/checks/run_quality_gate.py --layer bronze` | `/opt/airflow/project` | N/A |
| `silver_transform` | spark | `docker exec spark /opt/spark/bin/spark-submit --driver-memory 2g --executor-memory 2g --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/silver_transform.py --batch-id {{ run_id }}` | `/opt/airflow/project` | `{{ run_id }}` |
| `silver_quality_gate` | spark | `docker exec spark python3 /workspace/checks/run_quality_gate.py --layer silver` | `/opt/airflow/project` | N/A |
| `dbt_run_staging` | airflow | `dbt run --select staging --profiles-dir .` | `/opt/airflow/project/dbt` | N/A |
| `dbt_run_intermediate` | airflow | `dbt run --select intermediate --profiles-dir .` | `/opt/airflow/project/dbt` | N/A |
| `dbt_run_dims_facts` | airflow | `dbt run --select core --profiles-dir .` | `/opt/airflow/project/dbt` | N/A |
| `dbt_run_marts` | airflow | `dbt run --select marts --profiles-dir .` | `/opt/airflow/project/dbt` | N/A |
| `dbt_test_gate` | airflow | `dbt test --profiles-dir .` | `/opt/airflow/project/dbt` | N/A |

### 4. Task Configuration (Retries, Backoff & Timeouts)
Every task sets `execution_timeout` individually: ingestion 10m; Bronze/Silver transforms 25m; quality gates 15m; dbt layer builds 15m; `dbt_test_gate` 10m. Retry/backoff is tuned **per task group**, overriding the DAG `default_args` baseline (`retries=3`, `retry_delay=2m`, `retry_exponential_backoff=True`, `max_retry_delay=15m`):

| Task Group | Tasks | `retries` | `retry_delay` | `retry_exponential_backoff` | `max_retry_delay` |
|---|---|---|---|---|---|
| Ingestion | `ingest_raw` | 4 | 1m | yes | 8m |
| Spark transforms | `bronze_transform`, `silver_transform` | 2 | 3m | yes | 15m |
| Quality gates | `bronze_quality_gate`, `silver_quality_gate` | 1 | 1m | no | — |
| dbt builds | `dbt_run_staging`, `dbt_run_intermediate`, `dbt_run_dims_facts`, `dbt_run_marts` | 2 | 2m | yes | 10m |
| dbt test gate | `dbt_test_gate` | 0 | — | no | — |

External/network ingestion retries most; data-quality gates and the dbt test gate retry little because their failures reflect bad data rather than transient faults. These retry values are declared in the DAG and are not yet verified by a live Airflow run (scheduler-level behavior is Step 10).

### 5. Failure Notification Specification (Dual Alerting: Slack Primary + Email Fallback)
- **Callback Hook:** `on_failure_callback` defined on DAG `default_args` (triggers strictly on final attempt failure after retries are exhausted).
- **Slack Payload:** Incoming Webhook containing DAG ID, Task ID, Run ID, Execution Date, Execution Host, Exception/Log Link (`task_instance.log_url`).
- **Email Payload:** Native Airflow SMTP alert sent to project maintainer if `SLACK_WEBHOOK_URL` fails or is unconfigured.

### 6. Scheduling & Backfill Policy
- **Schedule:** `@daily` (runs daily at 00:00 UTC).
- **Catchup:** `catchup=False`. Historical backfills are executed on-demand via the native `airflow dags backfill` CLI to prevent uncontrolled retroactive DAG execution cascades.


## Day 45 — Reliability Proof: Crash Recovery Under Airflow Automatic Retries

### Verification & Findings
1. **Verification Test:** Re-injected `SIMULATE_CRASH_AFTER_MERGE=true` inside `silver_transform.py` during an Airflow-managed DAG execution.
2. **Airflow Behavior:** `silver_transform` failed on attempt 1 immediately after Delta `MERGE` completed but prior to `watermark_advance`. Airflow's scheduler automatically transitioned the task to `up_for_retry` state with exponential backoff.
3. **Automatic Recovery:** On Attempt 2 (with `SIMULATE_CRASH_AFTER_MERGE=false`), the script re-read the watermark (which remained at the pre-crash timestamp), identified the un-watermarked events, and performed a second deterministic `MERGE` using SHA-256 `event_key`.
4. **Idempotency (tested behavior):** Re-running an already-merged batch inserts 0 rows; duplicate keys inside a batch are collapsed deterministically (earliest `ingested_at`, then `source_file`) before the write. Covered by `tests/test_silver_dedup.py`, run in CI. The crash-recovery retry outcome described above is not yet covered by an automated test.


## Day 48 — Operational Runbook: Multi-Day Backfills via Airflow Native CLI

### Backfill Execution Standard
Historical backfills MUST be triggered explicitly using the Airflow CLI rather than toggling `catchup=True` on the main DAG. This prevents resource starvation and ensures deterministic batch execution order.

```bash
# Execute backfill for historical date range (e.g. 2026-01-01 to 2026-01-07)
docker compose exec airflow-webserver airflow dags backfill \
  --start-date 2026-01-01 \
  --end-date 2026-01-07 \
  --reset-dagruns \
  ecommerce_lakehouse
```

### Post-Backfill Verification Checks
1. **Silver History Verification:** Query Delta table `DESCRIBE HISTORY delta.`s3a://silver/ecommerce_events/`` to verify commit version increments match the backfilled date count cleanly.
2. **Incremental vs. Full-Refresh Equivalence:** Compare `mart_daily_summary` row counts and aggregates between sequential incremental backfill runs and `dbt run --full-refresh --select mart_daily_summary` to verify exact analytical equivalence.


## Phase 7 Completion Summary (Day 49 — 2026-08-26)

### Final Validation Results
| Run / Verification | Executed Command | Result |
|--------------------|------------------|--------|
| DAG Syntax & Load | `python airflow/dags/lakehouse_pipeline.py` | ✅ Clean import & valid graph |
| Full Pipeline Run | Airflow Webserver manual trigger | ✅ 10/10 tasks PASS end-to-end |
| Quality Gate Halting | Injected deliberate quality gate failure | ✅ Upstream failed status propagated, downstream halted |
| Automatic Retry Crash Recovery | Manual injection: `SIMULATE_CRASH_AFTER_MERGE=true` | ⏳ Not yet covered by an automated test |
| Failure Notification | Callback trigger | ✅ Slack webhook dispatched + email fallback ready |
| dbt Mart Test Suite | `dbt test` within DAG execution | ✅ 84/84 PASS — 0 ERR, 0 WARN |

### Key Files Delivered
| File | Purpose |
|------|---------|
| `airflow/dags/lakehouse_pipeline.py` | 10-step orchestrated pipeline DAG (`ecommerce_lakehouse`) |
| `airflow/plugins/slack_alert.py` | Dual failure alert callback plugin (Slack primary + email fallback) |
| `Dockerfile.airflow` | Custom Airflow container image with `dbt-duckdb` & project dependencies |
| `docker-compose.yml` | Updated with Airflow build config, environment variables, and project volume mount |

### Git Tag
`v1.0-week7-orchestration-complete` — on merge commit into `main`


## Week 8 — CI/CD Pipeline Automation with GitHub Actions

### Workflow Architecture (`.github/workflows/ci.yml`)
The continuous integration pipeline automates code quality, linting, dbt model compilation, and script integrity checks across three parallel jobs:

1. **`lint-code`**: Executes `flake8` across all Python source directories (`ingestion/`, `spark_jobs/`, `checks/`, `airflow/`). Configured via `.flake8` with max line length 120 and standard PEP8 ignores.
2. **`dbt-check`**: Validates dbt project parsing (`dbt parse`) and model compilation (`dbt compile`) using DuckDB adapter. Configured via `.sqlfluff` for SQL dialect formatting rules.
3. **`quality-gate-check`**: Runs `py_compile` syntax verification on all critical pipeline entrypoints to ensure zero runtime syntax errors.

### Configuration Artifacts Created
- `.flake8`: Python linting rules & exclusions.
- `.sqlfluff`: DuckDB dbt SQL formatting rules.
- `.github/workflows/ci.yml`: GitHub Actions pipeline specification.

### Git Tag
`v1.1-week8-cicd-complete` — on merge commit into `main`


## Week 9 — Combined Data Platform & Performance Suite

### 1. Part A: Data Contract Enforcement & Schema Registry (`contracts/`)
- **YAML Contract Definition (`contracts/schemas/ecommerce_events_v1.yml`)**: Enforces explicit dataset metadata, column data types, required fields (`event_time`, `event_type`, `product_id`, `user_id`, `user_session`), enum value restrictions (`view`, `cart`, `remove_from_cart`, `purchase`), price boundaries (`price >= 0.0`), and nullability thresholds.
- **Contract CLI Engine (`contracts/contract_cli.py`)**: CLI validation runner inspecting incoming raw clickstream batches. 
- **Quarantine & Alerting Routing**: Valid batches pass with exit code `0`. Violating batches fail with exit code `1`, write a structured violation JSON (`data/quarantine/quarantine_<ts>_<file>.json`), and attempt a breaking-batch Slack alert (no-op with a logged warning when `SLACK_WEBHOOK_URL` is unset). The registry stores the real computed status (`VALID` / `VIOLATIONS_FOUND`). Schema-diff classification (`COMPATIBLE` / `WARNING` / `BREAKING`) is produced by the separate `diff` sub-command and is not posted to Slack.

### 2. Part B: PySpark & Spark SQL Query Optimization Suite (`spark_jobs/`)
- **Optimization Harness (`spark_jobs/spark_benchmark.py`)**: Evaluates performance across three core distributed computing dimensions:
  1. **Storage Optimization**: Baseline Delta Lake vs. `OPTIMIZE` with `ZORDER BY (user_id, event_time)` — demonstrated **~3.0x speedup** via file coalescing and data-skipping statistics.
  2. **Join Strategy Tuning**: Sort-Merge Join vs. `broadcast(dim_product)` Hash Join — demonstrated **~3.2x speedup** by eliminating multi-gigabyte network shuffles.
  3. **Shuffle Partition Tuning**: Default 200 shuffle partitions vs. core-aligned 8 partitions — reduced task scheduling overhead for small-to-medium batch sizes.
- **Case Study Artifact**: Formatted Markdown report generated at [`docs/performance_benchmarks.md`](performance_benchmarks.md).

### Git Tag
`v1.2-week9-contracts-benchmarking-complete` — on merge commit into `main`







