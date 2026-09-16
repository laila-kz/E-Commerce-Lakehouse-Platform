"""
Explicit schemas for the data lakehouse
All schemas are defined as Spark StructType objects
Based on actual data from 2019-Oct.csv and 2019-Nov.csv
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType
)

BRONZE_EVENT_SCHEMA = StructType([
    # event_time: Always populated, needs to be parsed as timestamp
    StructField("event_time", TimestampType(), nullable=False),

    # event_type: Always populated (view, cart, purchase, etc)
    StructField("event_type", StringType(), nullable=False),

    # product_id: Always populated, store as Long for joins
    StructField("product_id", LongType(), nullable=False),

    # category_id: Always populated, store as Long
    StructField("category_id", LongType(), nullable=False),

    # category_code: CAN BE EMPTY in your data! (see: ,,creed)
    # Allow nulls, Silver will handle business rules
    StructField("category_code", StringType(), nullable=True),

    # brand: CAN BE EMPTY in your data! (see: ,543.10,)
    # Allow nulls, Silver will handle business rules
    StructField("brand", StringType(), nullable=True),

    # price: Always populated, store as Double for precision
    StructField("price", DoubleType(), nullable=False),

    # user_id: Always populated, store as Long
    StructField("user_id", LongType(), nullable=False),

    # user_session: Always populated (UUID format)
    StructField("user_session", StringType(), nullable=False)
])

# Helper function to print schema


def print_schema():
    """Print the Bronze schema in a readable format"""
    print("Bronze Event Schema (Based on Actual Data):")
    print("=" * 50)
    for field in BRONZE_EVENT_SCHEMA.fields:
        nullable = "YES" if field.nullable else "NO"
        print(f"{field.name:15} | {field.dataType.typeName():12} | Nullable: {nullable}")


if __name__ == "__main__":
    print_schema()
