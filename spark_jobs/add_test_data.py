# spark_jobs/add_test_data.py
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("Add Test Data") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create 5 test rows with current timestamp
test_data = [
    (datetime.now(), "view", 999999, 999999, "electronics.test", "test_brand", 99.99, 999999, "test_session_1"),
    (datetime.now(), "cart", 999998, 999999, "electronics.test", "test_brand", 99.99, 999998, "test_session_2"),
    (datetime.now(), "purchase", 999997, 999999, "electronics.test", "test_brand", 99.99, 999997, "test_session_3"),
]

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

df = spark.createDataFrame(test_data, schema)
df.write.format("delta").mode("append").save("s3a://bronze/ecommerce_events/")
print("✅ Test data added to Bronze")
spark.stop()
