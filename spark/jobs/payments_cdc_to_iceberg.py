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
    .appName("EcommercePaymentsCDCToIceberg")
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


payment_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", BinaryType(), True),
    StructField("payment_timestamp", LongType(), True),
    StructField("updated_at", LongType(), True)
])


payload_schema = StructType([
    StructField("before", payment_schema, True),
    StructField("after", payment_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])


spark.sql("""
CREATE TABLE IF NOT EXISTS local.ecommerce.payments_cdc (
    payment_id INT,
    order_id INT,
    payment_method STRING,
    payment_status STRING,
    payment_amount DECIMAL(10,2),
    operation STRING,
    event_timestamp_ms BIGINT
)
USING iceberg
""")


kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "ecommerce.public.payments")
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
        col("data.payload.after.payment_id"),
        col("data.payload.before.payment_id")
    ).alias("payment_id"),

    coalesce(
        col("data.payload.after.order_id"),
        col("data.payload.before.order_id")
    ).alias("order_id"),

    coalesce(
        col("data.payload.after.payment_method"),
        col("data.payload.before.payment_method")
    ).alias("payment_method"),

    coalesce(
        col("data.payload.after.payment_status"),
        col("data.payload.before.payment_status")
    ).alias("payment_status"),

    decode_decimal_udf(
        coalesce(
            col("data.payload.after.payment_amount"),
            col("data.payload.before.payment_amount")
        )
    ).alias("payment_amount"),

    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp_ms")
).filter(
    col("payment_id").isNotNull()
)


query = (
    cdc_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/iceberg/warehouse/checkpoints/payments_cdc"
    )
    .toTable("local.ecommerce.payments_cdc")
)

query.awaitTermination()
