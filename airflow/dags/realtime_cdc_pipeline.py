from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime


PROJECT_ROOT = Path(__file__).resolve().parents[2]


with DAG(
    dag_id="realtime_cdc_ecommerce_pipeline",
    description="Orchestrates Iceberg current-state builds, data quality, and dbt analytics",
    start_date=datetime(2026, 10, 1, tz="America/New_York"),
    schedule=None,
    catchup=False,
    tags=["cdc", "kafka", "spark", "iceberg", "dbt"],
) as dag:

    build_customers_current = BashOperator(
        task_id="build_customers_current",
        cwd=str(PROJECT_ROOT),
        bash_command="""
        docker exec ecommerce-spark /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
          /opt/spark/jobs/build_customers_current.py
        """,
    )

    build_orders_current = BashOperator(
        task_id="build_orders_current",
        cwd=str(PROJECT_ROOT),
        bash_command="""
        docker exec ecommerce-spark /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
          /opt/spark/jobs/build_orders_current.py
        """,
    )

    build_payments_current = BashOperator(
        task_id="build_payments_current",
        cwd=str(PROJECT_ROOT),
        bash_command="""
        docker exec ecommerce-spark /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
          /opt/spark/jobs/build_payments_current.py
        """,
    )

    data_quality = BashOperator(
        task_id="data_quality_checks",
        cwd=str(PROJECT_ROOT),
        bash_command="""
        docker exec ecommerce-spark /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/.ivy2 \
          --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
          /opt/spark/jobs/data_quality_checks.py
        """,
    )

    check_thrift_server = BashOperator(
        task_id="check_spark_thrift_server",
        cwd=str(PROJECT_ROOT),
        bash_command="""
        docker exec ecommerce-spark \
          /opt/spark/bin/beeline \
          -u jdbc:hive2://localhost:10000 \
          -e "SELECT 1;"
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        cwd=str(PROJECT_ROOT),
        bash_command="""
        ./dbt/.venv/bin/dbt run \
          --project-dir dbt \
          --select customer_order_summary
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=str(PROJECT_ROOT),
        bash_command="""
        ./dbt/.venv/bin/dbt test \
          --project-dir dbt \
          --select customer_order_summary
        """,
    )

    [
        build_customers_current,
        build_orders_current,
        build_payments_current,
    ] >> data_quality

    data_quality >> check_thrift_server >> dbt_run >> dbt_test
