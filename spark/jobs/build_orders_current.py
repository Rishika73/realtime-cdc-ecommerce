from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BuildOrdersCurrent")
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
CREATE TABLE IF NOT EXISTS local.ecommerce.orders_current (
    order_id INT,
    customer_id INT,
    order_status STRING,
    order_total DECIMAL(10,2),
    operation STRING,
    event_timestamp_ms BIGINT
)
USING iceberg
""")

spark.sql("""
MERGE INTO local.ecommerce.orders_current AS target

USING (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_total,
        operation,
        event_timestamp_ms
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY event_timestamp_ms DESC
            ) AS rn
        FROM local.ecommerce.orders_cdc
    )
    WHERE rn = 1
) AS source

ON target.order_id = source.order_id

WHEN MATCHED AND source.operation = 'd'
THEN DELETE

WHEN MATCHED THEN UPDATE SET
    target.customer_id = source.customer_id,
    target.order_status = source.order_status,
    target.order_total = source.order_total,
    target.operation = source.operation,
    target.event_timestamp_ms = source.event_timestamp_ms

WHEN NOT MATCHED AND source.operation <> 'd'
THEN INSERT (
    order_id,
    customer_id,
    order_status,
    order_total,
    operation,
    event_timestamp_ms
)
VALUES (
    source.order_id,
    source.customer_id,
    source.order_status,
    source.order_total,
    source.operation,
    source.event_timestamp_ms
)
""")

spark.sql("""
SELECT
    order_id,
    customer_id,
    order_status,
    order_total,
    operation
FROM local.ecommerce.orders_current
ORDER BY order_id
""").show(truncate=False)

spark.stop()
