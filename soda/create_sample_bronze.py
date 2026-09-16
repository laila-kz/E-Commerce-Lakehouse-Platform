"""
Create sample valid Bronze Delta table data for testing Soda Core checks
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col
from datetime import datetime

spark = SparkSession.builder \
    .appName("Create Sample Bronze Data") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Generating 1500 valid sample events...")
data = [
    (datetime.now(), "view", 100000 + i, 200000 + i, "electronics.smartphone",
     "brand_a", 299.99 + i, 500000 + i, f"session_{i}")
    for i in range(1500)
]

columns = ["event_time", "event_type", "product_id", "category_id",
           "category_code", "brand", "price", "user_id", "user_session"]
df = spark.createDataFrame(data, columns)

df_with_lineage = df \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source_file", lit("sample_events.csv")) \
    .withColumn("pipeline_run_id", lit("sample_run_001")) \
    .withColumn("batch_id", lit("sample_batch_001")) \
    .withColumn("event_date", to_date(col("event_time")))

df_with_lineage.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save("s3a://bronze/ecommerce_events/")

print("✅ Sample Bronze Delta table created successfully at s3a://bronze/ecommerce_events/")
spark.stop()
