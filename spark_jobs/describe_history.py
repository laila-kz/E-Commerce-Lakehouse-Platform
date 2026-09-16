from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Describe History") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("📊 DESCRIBE HISTORY - Bronze Table")
print("=" * 60)
spark.sql("DESCRIBE HISTORY delta.`s3a://bronze/ecommerce_events/`").show(truncate=False)

print("\n📊 Total rows: ", spark.sql("SELECT COUNT(*) FROM delta.`s3a://bronze/ecommerce_events/`").collect()[0][0])
spark.stop()

# docker cp spark_jobs/describe_history.py spark:/opt/spark/work-dir/
# docker compose exec spark /opt/spark/bin/spark-submit describe_history.py
