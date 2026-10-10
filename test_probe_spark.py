import sys

sys.path.insert(0, "/opt/spark/python")

import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

TMP = tempfile.mkdtemp(prefix="delta_probe_")
SILVER = f"{TMP}/silver"

spark = (
    SparkSession.builder.appName("probe")
    .master("local[2]")
    .config("spark.jars", "/opt/spark/jars-extra/delta-spark_2.12-3.1.0.jar,/opt/spark/jars-extra/delta-storage-3.1.0.jar")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

rows = [
    ("a", "2019-11-01", 1),
    ("b", "2019-11-02", 2),
]
df = spark.createDataFrame(rows, ["event_key", "event_date", "price"])
df.write.format("delta").mode("overwrite").save(SILVER)

read_back = spark.read.format("delta").load(SILVER).count()
print(f"READBACK_COUNT={read_back}")

hist = spark.sql(f"DESCRIBE HISTORY delta.`{SILVER}`").select("version", "operation", "operationMetrics").collect()[0]
print(f"OP={hist.operation}")
print(f"METRICS={hist.operationMetrics}")

spark.stop()