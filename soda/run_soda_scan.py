#!/usr/bin/env python3
"""
Soda Core Scan Runner
Run quality checks against Bronze Delta table
"""

import os
import sys

# Add Spark python paths if not already in sys.path
spark_python = "/opt/spark/python"
py4j_zip = "/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
if os.path.exists(spark_python) and spark_python not in sys.path:
    sys.path.insert(0, spark_python)
if os.path.exists(py4j_zip) and py4j_zip not in sys.path:
    sys.path.insert(0, py4j_zip)


DELTA_JARS = ",".join([
    "/opt/spark/jars-extra/delta-spark_2.12-3.1.0.jar",
    "/opt/spark/jars-extra/delta-storage-3.1.0.jar",
    "/opt/spark/jars-extra/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar",
])


def run_soda_scan(table_path, checks_file, configuration_file):
    """
    Run Soda scan against a Delta table using PySpark and Soda Core Python SDK
    """
    print("=" * 60)
    print("🔍 SODA CORE DATA QUALITY SCAN")
    print("=" * 60)
    print(f"📊 Table: {table_path}")
    print(f"📝 Checks: {checks_file}")
    print(f"⚙️  Config: {configuration_file}")
    print("=" * 60)
    print("\nRunning checks...")

    from pyspark.sql import SparkSession
    from soda.scan import Scan

    spark = SparkSession.builder \
        .appName("Soda Core Data Quality Scan") \
        .config("spark.jars", DELTA_JARS) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    df = spark.read.format("delta").load(table_path)

    # Determine table view name based on table path or checks file
    if "test_bad_events" in table_path:
        view_name = "test_bad_events"
    else:
        view_name = "bronze_ecommerce_events"

    df.createOrReplaceTempView(view_name)

    scan = Scan()
    scan.disable_telemetry()
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark)
    scan.add_sodacl_yaml_file(checks_file)

    exit_code = scan.execute()

    # Print Soda logs / check results
    print(scan.get_logs_text())

    print("\n" + "=" * 60)
    print("📊 SCAN COMPLETE")
    print("=" * 60)

    if exit_code == 0:
        print("✅ ALL CHECKS PASSED!")
    else:
        print("❌ SOME CHECKS FAILED!")
        print(f"Exit code: {exit_code}")

    spark.stop()
    return exit_code


if __name__ == "__main__":
    table_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://bronze/ecommerce_events/"
    checks_file = sys.argv[2] if len(sys.argv) > 2 else "soda/checks/bronze_checks.yml"
    configuration_file = sys.argv[3] if len(sys.argv) > 3 else "soda/configurations/spark_configuration.yml"

    exit_code = run_soda_scan(table_path, checks_file, configuration_file)
    sys.exit(exit_code)
