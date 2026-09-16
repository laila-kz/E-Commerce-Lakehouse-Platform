"""
Scratch script for Bronze schema exploration
NOTE: This is NOT part of the pipeline - it's for exploration only!
Based on actual data from 2019-Oct.csv and 2019-Nov.csv
"""

from pyspark.sql import SparkSession
from schemas import BRONZE_EVENT_SCHEMA

# Create Spark session
spark = SparkSession.builder \
    .appName("Bronze Schema Exploration") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("✅ Spark session created!")

# Read the raw data with the Bronze schema
print("\n📊 Attempting to read raw CSV with Bronze schema...")
print("=" * 60)

# Find the latest partition
# You may need to adjust the path based on your actual ingested_date
sample_path = "s3a://raw/ecommerce_events/ingested_date=2026-07-20/"

try:
    # Read the CSV with explicit schema
    df = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(BRONZE_EVENT_SCHEMA) \
        .csv(sample_path)

    print("✅ Schema applied successfully!")

    # Show the schema
    print("\n📋 Schema from Spark:")
    print("=" * 60)
    df.printSchema()

    # Count records
    record_count = df.count()
    print(f"\n📊 Total Records: {record_count:,}")

    # Show sample data (first 10 rows)
    print("\n📊 Sample Data (first 10 rows):")
    print("=" * 60)
    df.show(10, truncate=False)

    # Check for null values in key columns
    print("\n🔍 Null Value Check:")
    print("=" * 60)

    null_counts = {
        'event_time': df.filter(df.event_time.isNull()).count(),
        'category_code': df.filter(df.category_code.isNull()).count(),
        'brand': df.filter(df.brand.isNull()).count(),
        'user_session': df.filter(df.user_session.isNull()).count(),
        'price': df.filter(df.price.isNull()).count()
    }

    for col, count in null_counts.items():
        percentage = (count / record_count) * 100
        print(f"{col:15} | NULL count: {count:8} | {percentage:5.2f}%")

    # Check for malformed rows
    if '_corrupt_record' in df.columns:
        corrupt_count = df.filter(df._corrupt_record.isNotNull()).count()
        print(f"\n❌ Malformed rows: {corrupt_count}")
        if corrupt_count > 0:
            print("\nSample malformed rows:")
            df.filter(df._corrupt_record.isNotNull()).select('_corrupt_record').show(5, truncate=False)

except Exception as e:
    print(f"❌ Error: {e}")
    print("\n💡 Tip: Check that your data exists at: " + sample_path)

# Stop Spark
spark.stop()
