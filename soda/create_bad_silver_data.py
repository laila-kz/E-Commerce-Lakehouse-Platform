"""
Create intentionally bad Silver test data to verify Soda checks fail
"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    date_format,
    lit,
    sha2,
    size,
    split,
    to_date,
    to_utc_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Create Spark session
spark = SparkSession.builder \
    .appName("Create Bad Silver Data") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("📊 Creating intentionally bad Silver test data...")

# Create bad data with:
# 1. Duplicate event_key (same user_id, event_type, product_id, event_time)
# 2. Negative price
# 3. Null user_session
# 4. Null category_l1 (parsing failure)

bad_data = []

# Good row
bad_data.append((datetime.now(), "view", 777777, 999999, "electronics.test",
                "test_brand", 99.99, 777777, "session_good"))

# 1. Duplicate event_key (same user_id, event_type, product_id, event_time)
bad_data.append((datetime.now(), "view", 888888, 999999, "electronics.test",
                "test_brand", 99.99, 888888, "session_dup_1"))
bad_data.append((datetime.now(), "view", 888888, 999999, "electronics.test",
                "test_brand", 99.99, 888888, "session_dup_2"))

# 2. Negative price
bad_data.append((datetime.now(), "view", 999999, 999999, "electronics.test",
                "test_brand", -50.00, 999999, "session_neg_price"))

# 3. Null user_session
bad_data.append((datetime.now(), "view", 111111, 999999, "electronics.test", "test_brand", 99.99, 111111, None))

# 4. Null category_l1 (will happen if category_code is empty or null)
bad_data.append((datetime.now(), "view", 222222, 999999, "", "test_brand", 99.99, 222222, "session_empty_cat"))

schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])

df = spark.createDataFrame(bad_data, schema)

# Add lineage and compute event_key (same logic as silver_transform)

# Format event_time as ISO-8601 UTC
df = df.withColumn(
    "event_time_iso",
    date_format(to_utc_timestamp("event_time", "UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
)

# Compute event_key
df = df.withColumn(
    "event_key",
    sha2(
        concat_ws(
            "|",
            col("user_id").cast("string"),
            col("event_type"),
            col("product_id").cast("string"),
            col("event_time_iso")
        ),
        256
    )
)

# Add lineage columns
df = df.withColumn("ingested_at", current_timestamp())
df = df.withColumn("source_file", lit("bad_test_data.csv"))
df = df.withColumn("pipeline_run_id", lit("test_run_bad"))
df = df.withColumn("batch_id", lit("test_batch_bad"))
df = df.withColumn("event_date", to_date("event_time"))

# Parse category_code
df = df.withColumn("category_parts", split(col("category_code"), "\\."))
df = df.withColumn("category_l1", when(size("category_parts") >= 1, col("category_parts")[0]).otherwise(None))
df = df.withColumn("category_l2", when(size("category_parts") >= 2, col("category_parts")[1]).otherwise(None))
df = df.withColumn("category_l3", when(size("category_parts") >= 3, col("category_parts")[2]).otherwise(None))
df = df.drop("category_parts", "event_time_iso")

print("📊 Writing bad data to test path...")
df.write.format("delta").mode("overwrite").save("s3a://silver/test_bad_events/")
print("✅ Bad data written to s3a://silver/test_bad_events/")

spark.stop()
