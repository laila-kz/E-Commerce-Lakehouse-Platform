# 📚 E-Commerce Data Lakehouse Data Dictionary & Catalog

A unified reference catalog mapping schema definitions, column data types, business descriptions, and constraints across all Medallion layers (Raw ➔ Bronze ➔ Silver ➔ Gold).

---

## 📥 Raw Zone (`s3a://raw/ecommerce_events/`)
Unmodified CSV clickstream event dumps from Kaggle API.

| Column | Type | Business Description | Nullable |
|--------|------|----------------------|----------|
| `event_time` | String | Event timestamp in UTC (`YYYY-MM-DD HH:MM:SS UTC`) | No |
| `event_type` | String | User activity (`view`, `cart`, `remove_from_cart`, `purchase`) | No |
| `product_id` | Long | Unique numeric product identifier | No |
| `category_id` | Long | Raw category hierarchy ID | Yes |
| `category_code` | String | Dot-separated category hierarchy (`electronics.smartphone`) | Yes |
| `brand` | String | Product brand name | Yes |
| `price` | Double | Item price in USD | No |
| `user_id` | Long | Unique customer/user identifier | No |
| `user_session` | String | Frontend browser session UUID | No |

---

## 🥉 Bronze Layer (`s3a://bronze/ecommerce_events/`)
Schema-enforced Delta Lake table with lineage metadata added.

| Column | Type | Business Description | Nullable |
|--------|------|----------------------|----------|
| `event_time` | Timestamp | Standardized UTC timestamp | No |
| `event_type` | String | Categorical event action | No |
| `product_id` | Long | Cleaned product identifier | No |
| `category_id` | Long | System category ID | Yes |
| `category_code` | String | Category classification path | Yes |
| `brand` | String | Standardized brand | Yes |
| `price` | Double | Non-negative price value | No |
| `user_id` | Long | Cleaned customer identifier | No |
| `user_session` | String | Browser session ID | No |
| `ingested_at` | Timestamp | Processing timestamp when written to Bronze | No |
| `source_file` | String | S3 input filepath origin | No |
| `pipeline_run_id` | String | Spark pipeline run UUID | No |
| `batch_id` | String | Spark pipeline run UUID | No |
| `event_date` | Date | Partition key derived from `event_time` | No |

---

## 🥈 Silver Layer (`s3a://silver/ecommerce_events/`)
Deduplicated incremental Delta table with SHA-256 event keys and watermark tracking.

| Column | Type | Business Description | Key Role |
|--------|------|----------------------|----------|
| `event_key` | String | `sha2(concat_ws("|", user_id, event_type, product_id, event_time as UTC ISO string "yyyy-MM-dd'T'HH:mm:ss'Z'"), 256)` | Primary Deduplication Key |
| `event_time` | Timestamp | Validated event timestamp | Filter & Watermark Column |
| `event_type` | String | Cleaned event type | Categorical |
| `product_id` | Long | Verified product ID | Dimension Join Key |
| `user_id` | Long | Customer ID | Dimension Join Key |
| `user_session` | String | Session UUID | Sessionization Key |

---

## 🥇 Gold Layer (dbt Marts & Dimensions)

### `dim_product` (Type 2 Slowly Changing Dimension)
| Column | Type | Description |
|--------|------|-------------|
| `product_sk` | String | Surrogate key (hashed business key) |
| `product_id` | Long | Business product ID |
| `brand` | String | Historical brand version |
| `price` | Double | Historical price version |
| `valid_from` | Timestamp | Version activation timestamp |
| `valid_to` | Timestamp | Version expiration timestamp (or NULL if current) |
| `is_current` | Boolean | True for active current record version |

### `dim_customer` (Type 1 Thin Dimension)
| Column | Type | Description |
|--------|------|-------------|
| `customer_key` | String | Surrogate key (`md5(user_id)`) |
| `user_id` | Long | Business customer identifier |
| `first_seen_at` | Timestamp | First observed event timestamp |
| `first_seen_date_key` | Integer | First observed event date key (`YYYYMMDD`) |

> **Gold materialization note:** dbt models (dimensions, facts, marts) materialize to the local DuckDB file (`dev.duckdb`), not to a `gold` object-store bucket. The `gold` bucket exists for infra parity but is not written by dbt.

### `mart_daily_summary` (Incremental Daily Rollup)
| Column | Type | Description |
|--------|------|-------------|
| `summary_date` | Date | Granularity date key |
| `category_l1` | String | Primary category grouping |
| `total_events` | Long | Total clickstream volume |
| `unique_users` | Long | Daily active user count |
| `total_revenue` | Double | Total purchase revenue ($) |
| `conversion_rate` | Double | `purchases / total_views` |

### `mart_customer_retention` (Cohort Retention Mart)
| Column | Type | Description |
|--------|------|-------------|
| `first_purchase_month` | String | Cohort creation month (`YYYY-MM`) |
| `months_since_first_purchase` | Integer | Cohort age index (0, 1, 2...) |
| `cohort_size` | Long | Initial customer cohort size |
| `retained_count` | Long | Active customers in relative month |
| `retention_rate` | Double | `retained_count / cohort_size` |
