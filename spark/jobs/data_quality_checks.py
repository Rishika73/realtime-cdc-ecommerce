from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceDataQuality")
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

customers = spark.table("local.ecommerce.customers_current")
orders = spark.table("local.ecommerce.orders_current")
payments = spark.table("local.ecommerce.payments_current")

checks = []

checks.append((
    "unique_customer_ids",
    customers.count() == customers.select("customer_id").distinct().count()
))

checks.append((
    "unique_order_ids",
    orders.count() == orders.select("order_id").distinct().count()
))

checks.append((
    "unique_payment_ids",
    payments.count() == payments.select("payment_id").distinct().count()
))

checks.append((
    "no_null_customer_ids",
    customers.filter("customer_id IS NULL").count() == 0
))

checks.append((
    "positive_order_totals",
    orders.filter("order_total <= 0").count() == 0
))

checks.append((
    "positive_payment_amounts",
    payments.filter("payment_amount <= 0").count() == 0
))

checks.append((
    "valid_order_customers",
    orders.join(
        customers,
        orders.customer_id == customers.customer_id,
        "left_anti"
    ).count() == 0
))

checks.append((
    "valid_payment_orders",
    payments.join(
        orders,
        payments.order_id == orders.order_id,
        "left_anti"
    ).count() == 0
))

failed = []

for name, passed in checks:
    status = "PASS" if passed else "FAIL"
    print(f"{status}: {name}")

    if not passed:
        failed.append(name)

assert not failed, f"Data quality checks failed: {failed}"

print("\nAll data quality checks passed.")

spark.stop()
