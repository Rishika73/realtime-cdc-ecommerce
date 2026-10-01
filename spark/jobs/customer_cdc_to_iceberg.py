from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,coalesce
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)

spark = (
    SparkSession.builder
    .appName("EcommerceCustomerCDCToIceberg")
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.local.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.local.warehouse",
        "/opt/iceberg/warehouse"
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

payload_schema = StructType([
    StructField("before", customer_schema, True),
    StructField("after", customer_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

spark.sql("""
CREATE TABLE IF NOT EXISTS local.ecommerce.customer_cdc (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    city STRING,
    state STRING,
    operation STRING,
    event_timestamp_ms BIGINT
)
USING iceberg
""")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "ecommerce.public.customers")
    .option("startingOffsets", "earliest")
    .load()
)

json_df = kafka_df.select(
    col("value").cast("string").alias("json_value")
)

parsed_df = json_df.select(
    from_json(
        col("json_value"),
        StructType([
            StructField("payload", payload_schema, True)
        ])
    ).alias("data")
)

cdc_df = parsed_df.select(
    coalesce(
        col("data.payload.after.customer_id"),
        col("data.payload.before.customer_id")
    ).alias("customer_id"),

    coalesce(
        col("data.payload.after.first_name"),
        col("data.payload.before.first_name")
    ).alias("first_name"),

    coalesce(
        col("data.payload.after.last_name"),
        col("data.payload.before.last_name")
    ).alias("last_name"),

    coalesce(
        col("data.payload.after.email"),
        col("data.payload.before.email")
    ).alias("email"),

    coalesce(
        col("data.payload.after.phone"),
        col("data.payload.before.phone")
    ).alias("phone"),

    coalesce(
        col("data.payload.after.city"),
        col("data.payload.before.city")
    ).alias("city"),

    coalesce(
        col("data.payload.after.state"),
        col("data.payload.before.state")
    ).alias("state"),

    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp_ms")
).filter(
    col("customer_id").isNotNull()
)
query = (
    cdc_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/iceberg/warehouse/checkpoints/customer_cdc"
    )
    .toTable("local.ecommerce.customer_cdc")
)

query.awaitTermination()
