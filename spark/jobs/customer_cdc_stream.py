from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)

spark = (
    SparkSession.builder
    .appName("EcommerceCustomerCDC")
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
    col("data.payload.after.customer_id").alias("customer_id"),
    col("data.payload.after.first_name").alias("first_name"),
    col("data.payload.after.last_name").alias("last_name"),
    col("data.payload.after.email").alias("email"),
    col("data.payload.after.city").alias("city"),
    col("data.payload.after.state").alias("state"),
    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp_ms")
)

query = (
    cdc_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
