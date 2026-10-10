import os
import shutil
import sys
import tempfile

import pytest

try:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    HAVE_SPARK = True
except Exception:
    HAVE_SPARK = False

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    if not HAVE_SPARK:
        pytest.skip("pyspark/delta not installed - Spark-based tests skipped", allow_module_level=True)
    warehouse = tempfile.mkdtemp(prefix="silver-dedup-warehouse-")
    scratch = tempfile.mkdtemp(prefix="silver-dedup-scratch-")

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("silver-dedup-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.local.dir", scratch)
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark

    spark.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
    shutil.rmtree(scratch, ignore_errors=True)
