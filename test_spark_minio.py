from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create Spark session
spark = SparkSession.builder \
    .appName("MinIO Connectivity Test") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("✅ Spark session created!")

# Create test data
data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
print("📊 Test data:")
df.show()

# Write to MinIO
print("💾 Writing to MinIO...")
df.write.mode("overwrite").parquet("s3a://raw/connectivity_test/")
print("✅ Write successful!")

# Read back from MinIO
print("📖 Reading back from MinIO...")
read_df = spark.read.parquet("s3a://raw/connectivity_test/")
print("✅ Read successful!")
print("📊 Read back data:")
read_df.show()

print("🎉 SUCCESS! Spark can talk to MinIO!")

spark.stop()
