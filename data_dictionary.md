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
Schema: Bronze columns plus `event_key` and the parsed category hierarchy (`parse_category_code`, `compute_event_key` in `spark_jobs/silver_transform.py`); partitioned by `event_date`.

| Column | Type | Business Description | Key Role |
|--------|------|----------------------|----------|
| `event_key` | String | SHA-256 dedup key over `user_id|event_type|product_id|event_time` (UTC ISO) | Primary Deduplication Key |
| `event_time` | Timestamp | Validated event timestamp | Filter & Watermark Column |
| `event_type` | String | Cleaned event type (`view`, `cart`, `remove_from_cart`, `purchase`) | Categorical |
| `product_id` | Long | Verified product ID | Dimension Join Key |
| `category_id` | Long | Raw category hierarchy ID | Categorical |
| `category_code` | String | Dot-separated category hierarchy (`electronics.smartphone`) | Categorical |
| `category_l1` | String | Top-level category parsed from `category_code` | Categorical |
| `category_l2` | String | Second-level category (NULL if fewer levels present) | Categorical |
| `category_l3` | String | Third-level category (NULL if fewer levels present) | Categorical |
| `brand` | String | Product brand | Categorical |
| `price` | Double | Item price in USD | Measure |
| `user_id` | Long | Customer identifier | Dimension Join Key |
| `user_session` | String | Frontend browser session UUID | Sessionization Key |
| `ingested_at` | Timestamp | Processing timestamp when the row was written to Bronze | Pipeline Metadata |
| `source_file` | String | S3 input filepath origin | Lineage |
| `pipeline_run_id` | String | Spark pipeline run UUID | Lineage |
| `batch_id` | String | Spark pipeline run UUID (batch scoped) | Lineage |
| `event_date` | Date | Partition key derived from `event_time` (`yyyy-MM-dd`) | Partition Key |

---

## 🥇 Gold Layer (dbt Marts & Dimensions)

All Gold models are defined in `dbt/models/marts/` and materialize to the local DuckDB file (`dbt/dev.duckdb`). Column lists below are regenerated from the `dbt/models/marts/*.sql` models and their `schema.yml` files.

### `dim_product` (Type 2 Slowly Changing Dimension) — `dbt/models/marts/dim_product.sql`
| Column | Type | Description |
|--------|------|-------------|
| `product_key` | String | Surrogate key (`md5(product_id || valid_from)`) |
| `product_id` | Long | Business product ID |
| `product_version` | Long | Version index per product (1, 2, ...) |
| `brand` | String | Brand as of version start |
| `category_code` | String | Category hierarchy as of version start |
| `category_l1` | String | Top-level category at version start |
| `category_l2` | String | Second-level category at version start |
| `category_l3` | String | Third-level category at version start |
| `price` | Double | Price as of version start |
| `valid_from` | Timestamp | Version activation timestamp |
| `valid_to` | Timestamp | Version expiration timestamp (NULL if current) |
| `is_current` | Boolean | True for the active current version |

### `dim_customer` (Type 1 Thin Dimension) — `dbt/models/marts/dim_customer.sql`
| Column | Type | Description |
|--------|------|-------------|
| `customer_key` | String | Surrogate key (`md5(user_id)`) |
| `user_id` | Long | Business customer identifier |
| `first_seen_at` | Timestamp | First observed event timestamp |
| `first_seen_date_key` | Integer | First observed event date key (`YYYYMMDD`) |

### `dim_date` (Date Dimension) — `dbt/models/marts/dim_date.sql`
| Column | Type | Description |
|--------|------|-------------|
| `date_key` | Integer | Integer date key (`YYYYMMDD`) |
| `date_day` | Date | Calendar date (`2019-01-01` to `2028-01-01`) |
| `day_of_week` | Long | Day of week (`0` = Sunday .. `6` = Saturday) |
| `day_name` | String | Full day name (`Monday`, ...) |
| `month` | Long | Calendar month (1-12) |
| `month_name` | String | Full month name (`January`, ...) |
| `quarter` | Long | Calendar quarter (1-4) |
| `year` | Long | Calendar year |
| `is_weekend` | Boolean | True when `day_of_week` is 0 or 6 |

### `fact_events` (Event-Grain Fact) — `dbt/models/marts/fact_events.sql`
One row per event, joined to all three dimensions with time-ranged `dim_product` resolution.
| Column | Type | Description |
|--------|------|-------------|
| `event_key` | String | Primary key (inherited from `stg_events`) |
| `session_id` | String | Synthetic session ID (`user_id`_`user_session_seq`) |
| `user_session_seq` | Long | Session sequence index per user |
| `date_key` | Integer | FK to `dim_date` |
| `customer_key` | String | FK to `dim_customer` |
| `product_key` | String | FK to `dim_product` (time-range resolved version) |
| `product_id` | Long | Business product ID |
| `user_id` | Long | Business customer identifier |
| `event_type` | String | Event type |
| `price` | Double | Item price at event time (USD) |
| `event_time` | Timestamp | Event timestamp |
| `raw_user_session` | String | Original Silver session UUID |
| `ingested_at` | Timestamp | Silver write timestamp |
| `batch_id` | String | Source batch UUID |

### `fact_purchases` (Purchase-Grain Fact) — `dbt/models/marts/fact_purchases.sql`
One row per `purchase` event; carries the revenue measure.
| Column | Type | Description |
|--------|------|-------------|
| `purchase_key` | String | Primary key (inherits `event_key`) |
| `date_key` | Integer | FK to `dim_date` |
| `customer_key` | String | FK to `dim_customer` |
| `product_key` | String | FK to `dim_product` (time-range resolved version) |
| `product_id` | Long | Business product ID |
| `user_id` | Long | Business customer identifier |
| `session_id` | String | Synthetic session ID |
| `revenue` | Double | Purchase revenue (price at purchase time) |
| `quantity` | Long | Fixed quantity (1) |
| `purchased_at` | Timestamp | Purchase event timestamp |
| `purchased_date` | Date | Purchase event date |
| `ingested_at` | Timestamp | Silver write timestamp |
| `batch_id` | String | Source batch UUID |

### `mart_daily_summary` (Incremental Daily Rollup) — `dbt/models/marts/mart_daily_summary.sql`
| Column | Type | Description |
|--------|------|-------------|
| `date` | Date | Granularity date |
| `category_l1` | String | Top-level product category (`unknown` when NULL) |
| `total_events` | Long | Total event count (all event types) |
| `total_purchases` | Long | Total purchase event count |
| `total_revenue` | Double | Sum of purchase revenue ($) |
| `active_users` | Long | Distinct active user count |
| `last_recomputed_at` | Timestamp | Incremental watermark (max consumed `ingested_at`) |

### `mart_customer_retention` (Cohort Retention Mart) — `dbt/models/marts/mart_customer_retention.sql`
| Column | Type | Description |
|--------|------|-------------|
| `cohort_month` | Date | First-purchase month (month-truncated) |
| `months_since_first_purchase` | Long | Cohort age index (0, 1, 2, ...) |
| `cohort_size` | Long | Distinct customers in the cohort |
| `retained_count` | Long | Distinct customers purchasing in this relative month |
| `retention_rate` | Double | `retained_count / cohort_size` (rounded to 4 dp) |

### `mart_category_performance` (Category Monthly Performance) — `dbt/models/marts/mart_category_performance.sql`
| Column | Type | Description |
|--------|------|-------------|
| `category_l1` | String | Top-level category (`unknown` when NULL) |
| `period` | Date | Monthly period (month-truncated) |
| `total_orders` | Long | Distinct purchase count in period |
| `total_revenue` | Double | Total revenue in period ($) |
| `prior_period_revenue` | Double | Previous period revenue (NULL for first period) |
| `prior_period_orders` | Long | Previous period orders (NULL for first period) |
| `revenue_growth_rate` | Double | Period-over-period revenue growth (rounded to 4 dp) |
| `growth_rate_label` | String | `new_category` / `no_prior_revenue` / `comparable` |
