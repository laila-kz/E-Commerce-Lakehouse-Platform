"""
Test malformed row handling with Bronze schema
Based on your actual data format
"""

from pyspark.sql import SparkSession
from schemas import BRONZE_EVENT_SCHEMA

# Create Spark session
spark = SparkSession.builder \
    .appName("Malformed Row Test") \
    .getOrCreate()

print("✅ Spark session created!")

# Create a test CSV with YOUR actual format
test_data = """event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
2019-11-01 00:00:00 UTC,view,1003461,2053013555631882655,electronics.smartphone,xiaomi,489.07,520088904,4d3b30da-a5e4-49df-b1a8-ba5943f1dd33
2019-11-01 00:00:00 UTC,view,5000088,2053013566100866035,appliances.sewing_machine,janome,293.65,530496790,8e5f4f83-366c-4f70-860e-ca7417414283
2019-11-01 00:00:01 UTC,view,17302664,2053013553853497655,,creed,NOT_A_NUMBER,561587266,755422e7-9040-477b-9bd2-6a6e8fd97387
2019-11-01 00:00:01 UTC,view,3601530,2053013563810775923,appliances.kitchen.washer,lg,712.87,518085591,3bfb58cd-7892-48cc-8020-2f17e6de6e7f
2019-11-01 00:00:01 UTC,view,1004775,2053013555631882655,electronics.smartphone,xiaomi,183.27,558856683,313628f1-68b8-460d-84f6-cec7a8796ef2"""

# Save to a temporary file
with open("/tmp/test_data.csv", "w") as f:
    f.write(test_data)

print("📊 Test file created with malformed row (price = NOT_A_NUMBER)")

# Read with the Bronze schema
df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(BRONZE_EVENT_SCHEMA) \
    .csv("/tmp/test_data.csv")

print("\n📋 Schema:")
df.printSchema()

print("\n📊 All Data:")
df.show(truncate=False)

print("\n🔍 Malformed rows (with _corrupt_record):")
df.filter(df._corrupt_record.isNotNull()).show(truncate=False)

print("\n✅ Clean rows (price is numeric):")
df.filter(df.price.isNotNull()).show(truncate=False)

print("\n❌ Malformed rows (price is NULL):")
df.filter(df.price.isNull()).show(truncate=False)

spark.stop()
