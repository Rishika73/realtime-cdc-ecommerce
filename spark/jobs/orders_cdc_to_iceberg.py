from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BinaryType,
    DecimalType
)

spark = (
    SparkSession.builder
    .appName("EcommerceOrdersCDCToIceberg")
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


def decode_decimal(value):
    if value is None:
        return None

    unscaled_value = int.from_bytes(
        value,
        byteorder="big",
        signed=True
    )

    return Decimal(unscaled_value) / Decimal(100)


decode_decimal_udf = udf(
    decode_decimal,
    DecimalType(10, 2)
)


order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True),

    # Debezium Kafka Connect Decimal is represented as Base64 bytes.
    StructField("order_total", BinaryType(), True),

    StructField("order_timestamp", LongType(), True),
    StructField("updated_at", LongType(), True)
])


payload_schema = StructType([
    StructField("before", order_schema, True),
    StructField("after", order_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])


spark.sql("""
CREATE TABLE IF NOT EXISTS local.ecommerce.orders_cdc (
    order_id INT,
    customer_id INT,
    order_status STRING,
    order_total DECIMAL(10,2),
    operation STRING,
    event_timestamp_ms BIGINT
)
USING iceberg
""")


kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "ecommerce.public.orders")
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
        col("data.payload.after.order_id"),
        col("data.payload.before.order_id")
    ).alias("order_id"),

    coalesce(
        col("data.payload.after.customer_id"),
        col("data.payload.before.customer_id")
    ).alias("customer_id"),

    coalesce(
        col("data.payload.after.order_status"),
        col("data.payload.before.order_status")
    ).alias("order_status"),

    decode_decimal_udf(
        coalesce(
            col("data.payload.after.order_total"),
            col("data.payload.before.order_total")
        )
    ).alias("order_total"),

    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp_ms")
).filter(
    col("order_id").isNotNull()
)


query = (
    cdc_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/iceberg/warehouse/checkpoints/orders_cdc"
    )
    .toTable("local.ecommerce.orders_cdc")
)


query.awaitTermination()
