from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BuildPaymentsCurrent")
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
CREATE TABLE IF NOT EXISTS local.ecommerce.payments_current (
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

spark.sql("""
MERGE INTO local.ecommerce.payments_current AS target

USING (
    SELECT
        payment_id,
        order_id,
        payment_method,
        payment_status,
        payment_amount,
        operation,
        event_timestamp_ms
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY payment_id
                ORDER BY event_timestamp_ms DESC
            ) AS rn
        FROM local.ecommerce.payments_cdc
    )
    WHERE rn = 1
) AS source

ON target.payment_id = source.payment_id

WHEN MATCHED AND source.operation = 'd'
THEN DELETE

WHEN MATCHED THEN UPDATE SET
    target.order_id = source.order_id,
    target.payment_method = source.payment_method,
    target.payment_status = source.payment_status,
    target.payment_amount = source.payment_amount,
    target.operation = source.operation,
    target.event_timestamp_ms = source.event_timestamp_ms

WHEN NOT MATCHED AND source.operation <> 'd'
THEN INSERT (
    payment_id,
    order_id,
    payment_method,
    payment_status,
    payment_amount,
    operation,
    event_timestamp_ms
)
VALUES (
    source.payment_id,
    source.order_id,
    source.payment_method,
    source.payment_status,
    source.payment_amount,
    source.operation,
    source.event_timestamp_ms
)
""")

spark.sql("""
SELECT
    payment_id,
    order_id,
    payment_method,
    payment_status,
    payment_amount,
    operation
FROM local.ecommerce.payments_current
ORDER BY payment_id
""").show(truncate=False)

spark.stop()
