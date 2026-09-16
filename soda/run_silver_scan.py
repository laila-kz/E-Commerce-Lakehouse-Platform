#!/usr/bin/env python3
"""
Soda Core Scan Runner - Silver Layer (Python API)
"""
import sys
from pyspark.sql import SparkSession
from soda.scan import Scan


def run_silver_scan():
    print("=" * 60)
    print("🔍 SODA CORE SILVER QUALITY SCAN")
    print("=" * 60)
    print("📊 Table: s3a://silver/ecommerce_events/")
    print("📝 Checks: soda/checks/silver_checks.yml")
    print("=" * 60)

    # 1. Initialize PySpark session with S3 & Delta configurations
    spark = SparkSession.builder \
        .appName("SodaSilverScan") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # 2. Register Silver Delta table as a temporary view for Soda
    try:
        df = spark.read.format("delta").load("s3a://silver/ecommerce_events/")
        df.createOrReplaceTempView("silver_ecommerce_events")
        print(f"✅ Silver table loaded! Rows: {df.count():,}")
    except Exception as e:
        print(f"❌ Error loading Silver table: {e}")
        spark.stop()
        return 1

    # 3. Configure and execute Soda Scan in-memory
    from pathlib import Path
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    scan = Scan()
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark)
    scan.add_configuration_yaml_file(str(PROJECT_ROOT / "soda/configurations/spark_configuration.yml"))
    scan.add_sodacl_yaml_file(str(PROJECT_ROOT / "soda/checks/silver_checks.yml"))

    print("🚀 Executing Soda Scan...")
    exit_code = scan.execute()

    print("\n" + "=" * 60)
    print("📊 SCAN COMPLETE")
    print("=" * 60)

    if exit_code == 0:
        print("✅ ALL SILVER CHECKS PASSED!")
    else:
        print(f"❌ SOME SILVER CHECKS FAILED! Exit code: {exit_code}")

    spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(run_silver_scan())
