#!/usr/bin/env python3
"""
Bronze Transform - Read raw data, enforce schema, quarantine malformed rows
Add lineage metadata and write to Delta Lake
"""

import argparse
from datetime import datetime
import json
import logging
import os
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, to_date
from schemas import BRONZE_EVENT_SCHEMA

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
            "line": record.lineno
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
        description="Bronze Transform - Read raw data, enforce schema, quarantine bad rows"
    )

    parser.add_argument(
        "--batch-id",
        default=str(uuid.uuid4()),
        help="Batch ID for this run (default: auto-generated UUID)"
    )

    parser.add_argument(
        "--input-path",
        default="s3a://raw/ecommerce_events/",
        help="Path to raw data in MinIO (default: s3a://raw/ecommerce_events/)"
    )

    parser.add_argument(
        "--output-path",
        default="s3a://bronze/",
        help="Path to output data in MinIO (default: s3a://bronze/)"
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (default: INFO)"
    )

    return parser.parse_args()


def create_spark_session(logger):
    """Create Spark session with MinIO S3A configuration and Delta Lake support"""

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    logger.info(
        "Creating Spark session with MinIO config",
        extra={
            "event": "spark_session_start",
            "endpoint": endpoint
        }
    )

    spark = SparkSession.builder \
        .appName("Bronze Transform") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.lakehouse.optimizer.EcommerceSparkExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    logger.info(
        "Spark session created with Delta support",
        extra={"event": "spark_session_success"}
    )

    return spark


def read_raw_data(spark, input_path, logger):
    """
    Read raw data from MinIO with enforced schema
    Returns: DataFrame with _corrupt_record column
    """
    logger.info(
        "Reading raw data from MinIO",
        extra={
            "event": "read_start",
            "input_path": input_path
        }
    )

    df = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(BRONZE_EVENT_SCHEMA) \
        .csv(input_path)

    logger.info(
        "Raw data read successfully",
        extra={"event": "read_success"}
    )

    return df


def split_data(df, logger):
    """
    Split DataFrame into valid and quarantined rows
    Returns: (valid_df, quarantine_df)
    """
    logger.info(
        "Splitting data into valid and quarantined rows",
        extra={"event": "split_start"}
    )

    # Check if _corrupt_record column exists
    if "_corrupt_record" not in df.columns:
        logger.info(
            "No _corrupt_record column found - all data is clean!",
            extra={"event": "no_corrupt_records"}
        )
        valid_df = df
        quarantine_df = df.limit(0)

        total_count = df.count()
        valid_count = total_count
        quarantine_count = 0

        logger.info(
            "Data split complete (all clean)",
            extra={
                "event": "split_complete",
                "total_rows": total_count,
                "valid_rows": valid_count,
                "quarantine_rows": quarantine_count,
                "quarantine_rate": 0.0
            }
        )

        return valid_df, quarantine_df

    quarantine_df = df.filter(col("_corrupt_record").isNotNull())
    valid_df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    total_count = df.count()
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()

    assert valid_count + quarantine_count == total_count, \
        f"Count mismatch: valid({valid_count}) + quarantine({quarantine_count}) != total({total_count})"

    logger.info(
        "Data split complete",
        extra={
            "event": "split_complete",
            "total_rows": total_count,
            "valid_rows": valid_count,
            "quarantine_rows": quarantine_count,
            "quarantine_rate": round((quarantine_count / total_count) * 100, 4) if total_count > 0 else 0
        }
    )

    return valid_df, quarantine_df


def write_quarantine(df, output_path, batch_id, logger):
    """
    Write quarantined rows to MinIO as Parquet
    """
    if df.count() == 0:
        logger.info(
            "No quarantined rows to write",
            extra={"event": "quarantine_skip"}
        )
        return

    quarantine_path = f"{output_path}_quarantine/batch_id={batch_id}/"

    logger.info(
        "Writing quarantined rows",
        extra={
            "event": "quarantine_write_start",
            "path": quarantine_path,
            "row_count": df.count()
        }
    )

    df.write \
        .mode("overwrite") \
        .parquet(quarantine_path)

    logger.info(
        "Quarantine write complete",
        extra={
            "event": "quarantine_write_success",
            "path": quarantine_path
        }
    )


# ===== TASK 1: Add Lineage Columns =====
def add_lineage_columns(df, batch_id, logger):
    """
    Add lineage metadata columns to the DataFrame
    """
    logger.info(
        "Adding lineage columns",
        extra={
            "event": "lineage_start",
            "batch_id": batch_id
        }
    )

    df_with_lineage = df \
        .withColumn("ingested_at", current_timestamp()) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("pipeline_run_id", lit(batch_id)) \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("event_date", to_date(col("event_time")))

    logger.info(
        "Lineage columns added",
        extra={
            "event": "lineage_success",
            "columns": ["ingested_at", "source_file", "pipeline_run_id", "batch_id", "event_date"]
        }
    )

    return df_with_lineage


# ===== TASK 2: Write to Delta =====
def write_delta(df, output_path, logger):
    """
    Write DataFrame as Delta table with date partitioning
    """
    if df.count() == 0:
        logger.info(
            "No valid rows to write to Delta",
            extra={"event": "delta_write_skip"}
        )
        return

    logger.info(
        "Writing Delta table",
        extra={
            "event": "delta_write_start",
            "output_path": output_path,
            "row_count": df.count()
        }
    )

    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("event_date") \
        .save(output_path)

    logger.info(
        "Delta write complete",
        extra={
            "event": "delta_write_success",
            "output_path": output_path,
            "row_count": df.count()
        }
    )


def main():
    """Main entry point"""
    args = parse_args()
    logger = setup_logging()
    logger.setLevel(getattr(logging, args.log_level))

    logger.info(
        "Bronze transform started",
        extra={
            "event": "job_started",
            "batch_id": args.batch_id,
            "input_path": args.input_path,
            "output_path": args.output_path
        }
    )

    spark = create_spark_session(logger)

    try:
        # Read raw data with schema enforcement
        raw_df = read_raw_data(spark, args.input_path, logger)
        total_count = raw_df.count()

        # Split into valid and quarantined
        valid_df, quarantine_df = split_data(raw_df, logger)

        # Write quarantined rows
        write_quarantine(quarantine_df, args.output_path, args.batch_id, logger)

        # ===== TASK 1: Add Lineage Columns =====
        valid_df_with_lineage = add_lineage_columns(valid_df, args.batch_id, logger)

        # ===== TASK 2: Write to Delta =====
        delta_path = f"{args.output_path}ecommerce_events/"
        write_delta(valid_df_with_lineage, delta_path, logger)

        # Log summary
        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()

        logger.info(
            "Bronze transform summary",
            extra={
                "event": "job_summary",
                "batch_id": args.batch_id,
                "total_rows_processed": total_count,
                "valid_rows": valid_count,
                "quarantined_rows": quarantine_count,
                "quarantine_rate": round((quarantine_count / total_count) * 100, 4) if total_count > 0 else 0,
                "delta_path": delta_path,
                "status": "success"
            }
        )

    except Exception as e:
        logger.error(
            "Job failed",
            extra={
                "event": "job_failure",
                "error": str(e),
                "error_type": type(e).__name__
            }
        )
        raise

    finally:
        spark.stop()

    logger.info(
        "Bronze transform completed",
        extra={
            "event": "job_completed",
            "batch_id": args.batch_id,
            "status": "success"
        }
    )


if __name__ == "__main__":
    main()
