#!/usr/bin/env python3
"""
Silver Transform - Incremental processing with watermarking and deduplication
"""

import os
import sys
import uuid
import json
import logging
import argparse
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    sha2,
    size,
    split,
    to_utc_timestamp,
    when,
    row_number,
)
from pyspark.sql.window import Window

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed")
    pass


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "event": getattr(record, "event", record.msg),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if hasattr(record, "extra"):
            log_entry.update(record.extra)

        return json.dumps(log_entry)


def setup_logging():
    """Configure structured JSON logging"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Silver Transform - Incremental processing with watermarking"
    )

    parser.add_argument(
        "--batch-id",
        default=str(uuid.uuid4()),
        help="Batch ID for this run",
    )
    parser.add_argument(
        "--bronze-path",
        default="s3a://bronze/ecommerce_events/",
        help="Path to Bronze Delta table",
    )
    parser.add_argument(
        "--silver-path",
        default="s3a://silver/ecommerce_events/",
        help="Path to Silver Delta table",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help="Days of allowed lateness buffer when filtering by watermark",
    )
    parser.add_argument(
        "--mode",
        default="incremental",
        choices=["incremental", "backfill"],
        help="Processing mode: incremental (watermark-guided) or backfill (watermark-isolated)",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date for backfills (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional end date for backfills (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
    )
    return parser.parse_args()


def create_spark_session(logger):
    """Create Spark session with MinIO S3A configuration and Delta Lake support"""

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    logger.info(
        "Creating Spark session",
        extra={"event": "spark_session_start", "endpoint": endpoint},
    )

    spark = (
        SparkSession.builder.appName("Silver Transform")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.lakehouse.optimizer.EcommerceSparkExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    logger.info("Spark session created", extra={"event": "spark_session_success"})
    return spark


def read_watermark(spark, silver_path, logger):
    """
    Read current watermark from silver._watermarks Delta table
    Returns: watermark_value (timestamp) or None if no watermark exists
    """
    watermark_path = f"{silver_path}_watermarks"

    logger.info(
        "Reading watermark",
        extra={"event": "watermark_read_start", "path": watermark_path},
    )

    try:
        df = spark.read.format("delta").load(watermark_path)
        row = df.filter(col("pipeline_name") == "ecommerce_silver").first()

        if row:
            watermark_value = row.last_processed_event_time
            logger.info(
                "Watermark found",
                extra={
                    "event": "watermark_found",
                    "value": str(watermark_value),
                    "updated_at": str(row.updated_at),
                },
            )
            return watermark_value
        else:
            logger.info(
                "No watermark found for pipeline",
                extra={"event": "watermark_missing_pipeline"},
            )
            return None

    except Exception as e:
        logger.info(
            "No watermark table found - first run",
            extra={"event": "watermark_table_missing", "error": str(e)},
        )
        return None


def read_bronze_new_rows(
    spark,
    bronze_path,
    watermark_value,
    logger,
    lookback_days=3,
    mode="incremental",
    start_date=None,
    end_date=None,
):
    """
    Read Bronze rows.
    - If mode == 'backfill': reads within optional [start_date, end_date] or all rows.
    - If mode == 'incremental' and watermark exists: applies allowed lateness lookback buffer
      (event_time >= watermark - lookback_days) so late-arriving records are not dropped.
    """
    logger.info(
        "Reading Bronze rows",
        extra={
            "event": "bronze_read_start",
            "path": bronze_path,
            "mode": mode,
            "watermark": str(watermark_value) if watermark_value else "NONE (first run)",
            "lookback_days": lookback_days if watermark_value and mode == "incremental" else 0,
        },
    )

    bronze_df = spark.read.format("delta").load(bronze_path)

    if mode == "backfill":
        if start_date:
            bronze_df = bronze_df.filter(col("event_time") >= start_date)
        if end_date:
            bronze_df = bronze_df.filter(col("event_time") <= end_date)
        logger.info(
            "Backfill range applied",
            extra={"event": "backfill_filter_applied", "start": start_date, "end": end_date},
        )
    elif watermark_value:
        if isinstance(watermark_value, (datetime,)):
            effective_cutoff = watermark_value - timedelta(days=lookback_days)
        else:
            try:
                dt = datetime.fromisoformat(str(watermark_value).replace("Z", "+00:00"))
                effective_cutoff = dt - timedelta(days=lookback_days)
            except Exception:
                effective_cutoff = watermark_value

        bronze_df = bronze_df.filter(col("event_time") >= effective_cutoff)
        logger.info(
            "Filtering by watermark with lateness buffer",
            extra={
                "event": "watermark_filter_applied",
                "watermark": str(watermark_value),
                "lookback_cutoff": str(effective_cutoff),
            },
        )
    else:
        logger.info(
            "No watermark - processing all data",
            extra={"event": "watermark_no_filter"},
        )

    row_count = bronze_df.count()
    logger.info(
        "Bronze rows read",
        extra={
            "event": "bronze_read_complete",
            "row_count": row_count,
            "mode": mode,
            "filtered_by_watermark": watermark_value is not None and mode == "incremental",
        },
    )

    return bronze_df


def apply_business_filters(df, logger):
    """
    Apply filters in order:
    1. user_session IS NOT NULL
    2. price >= 0
    3. event_time >= '2019-01-01' AND event_time <= CURRENT_DATE()
    """
    initial_count = df.count()
    logger.info(
        "Applying business filters",
        extra={"event": "filter_start", "rows_before": initial_count},
    )

    null_session_count = df.filter(col("user_session").isNull()).count()
    df = df.filter(col("user_session").isNotNull())
    logger.info(
        "Filter: null user_session",
        extra={
            "event": "filter_null_session",
            "rows_dropped": null_session_count,
            "rows_remaining": df.count(),
        },
    )

    negative_price_count = df.filter(col("price") < 0).count()
    df = df.filter(col("price") >= 0)
    logger.info(
        "Filter: negative price",
        extra={
            "event": "filter_negative_price",
            "rows_dropped": negative_price_count,
            "rows_remaining": df.count(),
        },
    )

    out_of_range_count = df.filter(
        (col("event_time") < "2019-01-01") | (col("event_time") > datetime.now())
    ).count()
    df = df.filter(
        (col("event_time") >= "2019-01-01") & (col("event_time") <= datetime.now())
    )
    logger.info(
        "Filter: event_time out of range",
        extra={
            "event": "filter_date_range",
            "rows_dropped": out_of_range_count,
            "rows_remaining": df.count(),
        },
    )

    final_count = df.count()
    logger.info(
        "Filters complete",
        extra={
            "event": "filter_complete",
            "rows_before": initial_count,
            "rows_after": final_count,
            "total_dropped": initial_count - final_count,
        },
    )

    return df


def parse_category_code(df, logger):
    """
    Parse category_code into category_l1, category_l2, category_l3
    Handle fewer than 3 levels with NULL
    """
    logger.info("Parsing category_code", extra={"event": "category_parse_start"})

    df = df.withColumn("category_parts", split(col("category_code"), "\\."))

    df = df.withColumn(
        "category_l1",
        when(size("category_parts") >= 1, col("category_parts")[0]).otherwise(None),
    )
    df = df.withColumn(
        "category_l2",
        when(size("category_parts") >= 2, col("category_parts")[1]).otherwise(None),
    )
    df = df.withColumn(
        "category_l3",
        when(size("category_parts") >= 3, col("category_parts")[2]).otherwise(None),
    )

    df = df.drop("category_parts")

    logger.info(
        "Category parsing complete",
        extra={"event": "category_parse_complete"},
    )

    return df


def compute_event_key(df, logger):
    """
    Compute synthetic deduplication key using SHA-256 and event_date for partitioning
    """
    logger.info("Computing deduplication key & partition date", extra={"event": "event_key_start"})

    df = df.withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))

    df = df.withColumn(
        "event_time_iso",
        date_format(to_utc_timestamp(col("event_time"), "UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )

    df = df.withColumn(
        "event_key",
        sha2(
            concat_ws(
                "|",
                col("user_id").cast("string"),
                col("event_type"),
                col("product_id").cast("string"),
                col("event_time_iso"),
            ),
            256,
        ),
    )

    df = df.drop("event_time_iso")

    key_count = df.select("event_key").distinct().count()
    total_count = df.count()

    logger.info(
        "Deduplication key computed",
        extra={
            "event": "event_key_complete",
            "total_rows": total_count,
            "distinct_keys": key_count,
            "duplicate_rate": round(((total_count - key_count) / total_count) * 100, 4) if total_count > 0 else 0,
        },
    )

    return df


def within_batch_dedup(df, logger):
    """
    Remove duplicate event_keys WITHIN a single batch before any write or MERGE.

    Keeps the earliest ingested row per event_key (deterministic tie-break
    on source_file), so a batch always produces at most one row per key.
    """
    logger.info(
        "Starting within-batch deduplication",
        extra={
            "event": "within_batch_dedup_start",
            "rows_before": df.count(),
            "distinct_keys": df.select("event_key").distinct().count(),
        },
    )

    window_spec = Window.partitionBy("event_key").orderBy(
        col("ingested_at").asc(),
        col("source_file").asc(),
    )

    deduped_df = (
        df.withColumn("dedup_rn", row_number().over(window_spec))
        .filter(col("dedup_rn") == 1)
        .drop("dedup_rn")
    )

    rows_after = deduped_df.count()

    logger.info(
        "Within-batch deduplication complete",
        extra={
            "event": "within_batch_dedup_complete",
            "rows_before": df.count(),
            "rows_after": rows_after,
            "rows_dropped": df.count() - rows_after,
        },
    )

    return deduped_df


def check_duplicates(df, logger):
    """Check for duplicate event_key values and log results"""
    duplicates = df.groupBy("event_key").count().filter(col("count") > 1)
    duplicate_count = duplicates.count()

    if duplicate_count > 0:
        logger.info(
            "Duplicate keys found",
            extra={
                "event": "duplicate_keys_found",
                "duplicate_groups": duplicate_count,
                "sample": duplicates.limit(3).collect()
            }
        )
    else:
        logger.info(
            "No duplicate keys found",
            extra={"event": "duplicate_keys_none"}
        )


def ensure_silver_table_exists(spark, silver_path, source_df, logger):
    """
    Check if Silver Delta table exists. If not, create it via initial write.
    Returns: (table_exists, created_new)
    """
    logger.info(
        "Checking if Silver table exists",
        extra={"event": "silver_table_check", "path": silver_path}
    )

    try:
        spark.read.format("delta").load(silver_path)
        logger.info(
            "Silver table exists",
            extra={"event": "silver_table_exists"}
        )
        return True, False
    except Exception as e:
        logger.info(
            "Silver table does not exist - creating",
            extra={"event": "silver_table_creating", "error": str(e)}
        )

        source_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("event_date") \
            .save(silver_path)

        logger.info(
            "Silver table created",
            extra={"event": "silver_table_created", "row_count": source_df.count()}
        )
        return True, True


def merge_into_silver(spark, silver_path, source_df, logger):
    """
    Perform MERGE INTO Silver table using event_key.
    NO WHEN MATCHED clause (events are immutable).
    Runner is responsible for pass-through of ingested_at/source_file,
    so source_df is the intermediate WITH both lineage columns.
    Returns: expected number of new distinct event_keys that MERGE should insert.
    """
    logger.info(
        "Starting MERGE into Silver",
        extra={"event": "merge_start", "source_rows": source_df.count()}
    )

    existing = spark.read.format("delta").load(silver_path)
    new_keys = source_df.select("event_key").distinct().join(
        existing.select("event_key").distinct(),
        on="event_key",
        how="left_anti",
    )
    expected_new_keys = new_keys.count()

    delta_table = DeltaTable.forPath(spark, silver_path)

    delta_table.alias("target") \
        .merge(
            source_df.alias("source"),
            "target.event_key = source.event_key"
    ) \
        .whenNotMatchedInsertAll() \
        .execute()

    logger.info(
        "MERGE execution completed",
        extra={
            "event": "merge_executed",
            "expected_new_keys": expected_new_keys,
        }
    )

    return expected_new_keys


def read_latest_version(spark, silver_path):
    """
    Return the latest committed Delta version for silver_path,
    or None when the table does not exist yet.
    """
    try:
        history_df = spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}`")
        latest = history_df.orderBy(col("version").desc()).first()
        return latest.version if latest else None
    except Exception:
        return None


def verify_merge_success(spark, silver_path, expected_rows, logger, version_before=None):
    """
    Verify the last write/MERGE succeeded by checking DESCRIBE HISTORY.
    Returns: dict with verification results.

    Delta skips no-op commits by default (spark.databricks.delta.skipRecordingEmptyCommits),
    so a MERGE that inserts 0 rows leaves the history unchanged. When version_before is
    provided and no new version was committed, the run is accepted only if expected_rows
    is 0 (a legitimate no-op); otherwise it is a failure. Passing the version captured
    before the run prevents comparing this run against a stale history entry.
    """
    logger.info(
        "Verifying MERGE via DESCRIBE HISTORY",
        extra={"event": "merge_verify_start", "expected_rows": expected_rows}
    )

    history_df = spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}`")
    latest = history_df.orderBy(col("version").desc()).first()

    if not latest:
        logger.error(
            "No history entries found",
            extra={"event": "merge_verify_failed", "reason": "no_history"}
        )
        raise RuntimeError("No Delta history entries found")

    if version_before is not None and latest.version == version_before:
        operation = "NO_CHANGE"
        verified_metric = 0
        verification_passed = expected_rows == 0
    else:
        operation = latest.operation
        metrics = latest.operationMetrics or {}

        if operation in ["WRITE", "CREATE TABLE AS SELECT"]:
            num_written = int(metrics.get("numOutputRows", -1))
            verification_passed = num_written == expected_rows
            verified_metric = num_written
        else:
            num_inserted = int(metrics.get("numTargetRowsInserted", 0))
            verification_passed = num_inserted == expected_rows
            verified_metric = num_inserted

    logger.info(
        "MERGE verification complete",
        extra={
            "event": "merge_verify_complete",
            "operation": operation,
            "verified_metric": verified_metric,
            "expected_rows": expected_rows,
            "verified": verification_passed
        }
    )

    return {
        "verified": verification_passed,
        "operation": operation,
        "verified_metric": verified_metric,
        "expected_rows": expected_rows,
        "version": latest.version
    }


def advance_watermark(spark, silver_path, batch_df, batch_id, logger):
    """
    Advance watermark to max(event_time) of this batch
    ONLY called after MERGE is verified successful
    """
    max_event_time = batch_df.selectExpr("max(event_time) as max_time").collect()[0][0]

    logger.info(
        "Advancing watermark",
        extra={
            "event": "watermark_advance_start",
            "max_event_time": str(max_event_time),
            "batch_id": batch_id
        }
    )

    watermark_path = f"{silver_path}_watermarks"

    watermark_data = [{
        "pipeline_name": "ecommerce_silver",
        "last_processed_event_time": max_event_time,
        "updated_at": datetime.now(),
        "last_batch_id": batch_id
    }]

    watermark_df = spark.createDataFrame(watermark_data)

    try:
        existing = spark.read.format("delta").load(watermark_path)

        updated = existing.join(
            watermark_df,
            existing.pipeline_name == watermark_df.pipeline_name,
            "left_outer"
        ).select(
            when(
                watermark_df.pipeline_name.isNotNull(),
                watermark_df.last_processed_event_time
            ).otherwise(existing.last_processed_event_time).alias("last_processed_event_time"),
            when(
                watermark_df.pipeline_name.isNotNull(),
                watermark_df.updated_at
            ).otherwise(existing.updated_at).alias("updated_at"),
            when(
                watermark_df.pipeline_name.isNotNull(),
                watermark_df.last_batch_id
            ).otherwise(existing.last_batch_id).alias("last_batch_id"),
            existing.pipeline_name
        )

        updated.write.format("delta").mode("overwrite").save(watermark_path)

    except Exception:
        watermark_df.write.format("delta").mode("append").save(watermark_path)

    logger.info(
        "Watermark advanced",
        extra={
            "event": "watermark_advance_complete",
            "max_event_time": str(max_event_time),
            "batch_id": batch_id
        }
    )


def main():
    args = parse_args()
    logger = setup_logging()
    logger.setLevel(getattr(logging, args.log_level))

    logger.info(
        "Silver transform started",
        extra={
            "event": "job_started",
            "batch_id": args.batch_id,
            "bronze_path": args.bronze_path,
            "silver_path": args.silver_path,
        },
    )

    spark = create_spark_session(logger)
    try:
        watermark_value = read_watermark(spark, args.silver_path, logger) if args.mode == "incremental" else None
        bronze_df = read_bronze_new_rows(
            spark,
            args.bronze_path,
            watermark_value,
            logger,
            lookback_days=args.lookback_days,
            mode=args.mode,
            start_date=args.start_date,
            end_date=args.end_date,
        )

        if bronze_df.count() == 0:
            logger.info(
                "No new rows to process",
                extra={"event": "no_new_rows", "status": "skipped"}
            )
            return

        filtered_df = apply_business_filters(bronze_df, logger)
        enriched_df = parse_category_code(filtered_df, logger)
        final_df = compute_event_key(enriched_df, logger)

        check_duplicates(final_df, logger)

        final_df = within_batch_dedup(final_df, logger)

        version_before = read_latest_version(spark, args.silver_path)

        table_exists, created_new = ensure_silver_table_exists(
            spark, args.silver_path, final_df, logger
        )

        expected_new_keys = final_df.count()
        if created_new:
            logger.info(
                "Initial Silver table created",
                extra={"event": "silver_first_run", "rows": expected_new_keys}
            )
        else:
            expected_new_keys = merge_into_silver(
                spark, args.silver_path, final_df, logger
            )

        verification = verify_merge_success(
            spark, args.silver_path, expected_new_keys, logger,
            version_before=version_before,
        )

        if not verification["verified"]:
            raise RuntimeError("MERGE verification failed")

        # ****************************************************
        # ===== SIMULATED CRASH INJECTION POINTS =====
        # Named failure test matrix hooks for failure-injection testing
        if (
            os.environ.get("SIMULATE_CRASH_AFTER_MERGE") == "true"
            or os.environ.get("FAIL_AFTER_SILVER_MERGE") == "true"
            or os.environ.get("FAIL_BEFORE_WATERMARK_UPDATE") == "true"
        ):
            logger.warning(
                "SIMULATED CRASH triggered: MERGE verified, halting before watermark advance",
                extra={
                    "event": "simulated_crash",
                    "location": "after_silver_merge_before_watermark",
                    "batch_id": args.batch_id,
                }
            )
            raise RuntimeError("Simulated crash: MERGE committed, watermark NOT advanced")
        # ****************************************************

        # Advance watermark ONLY in incremental mode
        if args.mode == "incremental":
            advance_watermark(spark, args.silver_path, final_df, args.batch_id, logger)
        else:
            logger.info(
                "Backfill mode active - skipping production watermark update to preserve forward state",
                extra={"event": "watermark_skipped_for_backfill", "batch_id": args.batch_id}
            )

        logger.info(
            "Silver transform completed",
            extra={
                "event": "job_completed",
                "status": "success",
                "batch_id": args.batch_id,
                "mode": args.mode,
                "rows_processed": final_df.count()
            }
        )

    except Exception as e:
        logger.error(
            "Job failed",
            extra={"event": "job_failure", "error": str(e)}
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
