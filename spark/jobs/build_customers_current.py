from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BuildCustomersCurrent")
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

spark.sql("""
CREATE TABLE IF NOT EXISTS local.ecommerce.customers_current (
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

spark.sql("""
MERGE INTO local.ecommerce.customers_current AS target

USING (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        state,
        operation,
        event_timestamp_ms
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY event_timestamp_ms DESC
            ) AS rn
        FROM local.ecommerce.customer_cdc
    )
    WHERE rn = 1
) AS source

ON target.customer_id = source.customer_id

WHEN MATCHED AND source.operation = 'd'
THEN DELETE

WHEN MATCHED THEN UPDATE SET
    target.first_name = source.first_name,
    target.last_name = source.last_name,
    target.email = source.email,
    target.phone = source.phone,
    target.city = source.city,
    target.state = source.state,
    target.operation = source.operation,
    target.event_timestamp_ms = source.event_timestamp_ms

WHEN NOT MATCHED AND source.operation <> 'd'
THEN INSERT (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    operation,
    event_timestamp_ms
)
VALUES (
    source.customer_id,
    source.first_name,
    source.last_name,
    source.email,
    source.phone,
    source.city,
    source.state,
    source.operation,
    source.event_timestamp_ms
)
""")

spark.sql("""
SELECT
    customer_id,
    first_name,
    last_name,
    city,
    state,
    operation
FROM local.ecommerce.customers_current
ORDER BY customer_id
""").show(truncate=False)

spark.stop()
