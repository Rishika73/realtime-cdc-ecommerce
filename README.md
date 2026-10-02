# Real-Time CDC E-Commerce Data Platform

An end-to-end real-time data engineering project that captures PostgreSQL changes using Debezium CDC, streams them through Kafka, processes them with Spark Structured Streaming, stores historical and current-state data in Apache Iceberg, validates data quality, builds analytics with dbt, and orchestrates the workflow with Airflow.

## Architecture

[View detailed architecture](docs/architecture.md)

```text
PostgreSQL
    |
    v
Debezium CDC
    |
    v
Apache Kafka
    |
    v
Spark Structured Streaming
    |
    +----------------------+
    |                      |
    v                      v
Iceberg CDC History   Iceberg Current State
    |                      |
    +----------+-----------+
               |
               v
       Data Quality Checks
               |
               v
              dbt
               |
               v
      Customer Analytics
               |
               v
            Airflow
```

## Tech Stack

- PostgreSQL
- Debezium
- Apache Kafka
- Apache Spark 4.1
- Spark Structured Streaming
- Apache Iceberg
- dbt
- Apache Airflow
- Docker / Docker Compose
- Python
- SQL

## Key Features

- PostgreSQL logical replication with Debezium CDC
- Kafka topics for customers, orders, payments, products, and order items
- Spark Structured Streaming consumers
- CDC support for `r`, `c`, `u`, and `d` operations
- Delete-aware processing using Debezium `before` and `after` payloads
- Append-only Iceberg CDC history tables
- Current-state tables built with Iceberg `MERGE`
- PostgreSQL `NUMERIC` / Kafka Connect Decimal decoding
- Referential-integrity and data-quality validation
- dbt analytics models and tests
- Airflow orchestration
- Dockerized local infrastructure

## Data Model

### CDC History Tables

```text
local.ecommerce.customer_cdc
local.ecommerce.orders_cdc
local.ecommerce.payments_cdc
```

### Current-State Tables

```text
local.ecommerce.customers_current
local.ecommerce.orders_current
local.ecommerce.payments_current
```

CDC history preserves every event, while current-state tables contain only the latest active record for each entity.

## Data Quality

The Spark validation job checks:

- Unique customer, order, and payment IDs
- Non-null customer IDs
- Positive order totals
- Positive payment amounts
- Valid customer references on orders
- Valid order references on payments

Result:

```text
PASS: unique_customer_ids
PASS: unique_order_ids
PASS: unique_payment_ids
PASS: no_null_customer_ids
PASS: positive_order_totals
PASS: positive_payment_amounts
PASS: valid_order_customers
PASS: valid_payment_orders

All data quality checks passed.
```

## dbt Analytics

The dbt model:

```text
customer_order_summary
```

combines:

```text
customers_current
orders_current
payments_current
```

to produce:

- Total orders
- Total order value
- Total paid amount
- Customer location information

Example output:

| Customer | City | State | Orders | Order Value | Paid Amount |
|---|---|---|---:|---:|---:|
| Ava Patel | Orlando | TX | 1 | 219.98 | 219.98 |
| Noah Kim | Houston | WA | 1 | 74.99 | 74.99 |
| Mia Johnson | San Francisco | IL | 1 | 149.98 | 149.98 |
| Liam Garcia | Denver | AZ | 1 | 59.99 | 59.99 |
| Emma Brown | Miami | MA | 1 | 134.98 | 134.98 |

## dbt Tests

```text
PASS=5
WARN=0
ERROR=0
SKIP=0
TOTAL=5
```

Tests cover:

- `customer_id` not null
- `customer_id` unique
- `total_orders` not null
- `total_order_value` not null
- `total_paid_amount` not null

## Airflow Orchestration

```text
build_customers_current ─┐
build_orders_current ────┼──> data_quality_checks
build_payments_current ──┘
                               |
                               v
                    check_spark_thrift_server
                               |
                               v
                           dbt_run
                               |
                               v
                          dbt_test
```

All seven tasks completed successfully in the final DAG run.

## Airflow Successful Run

![Airflow Successful Run](docs/screenshots/airflow_success.png)

## Project Structure

```text
realtime-cdc-ecommerce/
├── airflow/
│   └── dags/
│       └── realtime_cdc_pipeline.py
├── dbt/
│   ├── models/
│   │   ├── customer_order_summary.sql
│   │   └── schema.yml
│   └── dbt_project.yml
├── debezium/
│   └── postgres-connector.json
├── docs/
│   ├── architecture.md
│   └── screenshots/
│       └── airflow_success.png
├── postgres/
│   └── init/
│       ├── 01_schema.sql
│       └── 02_seed_data.sql
├── spark/
│   └── jobs/
│       ├── customer_cdc_stream.py
│       ├── customer_cdc_to_iceberg.py
│       ├── orders_cdc_to_iceberg.py
│       ├── payments_cdc_to_iceberg.py
│       ├── build_customers_current.py
│       ├── build_orders_current.py
│       ├── build_payments_current.py
│       └── data_quality_checks.py
├── docker-compose.yml
├── .gitignore
└── README.md
```

## Run the Project

Start the infrastructure:

```bash
docker compose up -d
```

Register the Debezium connector:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @debezium/postgres-connector.json \
  http://localhost:8083/connectors
```

Run a CDC streaming job:

```bash
docker exec -it ecommerce-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  /opt/spark/jobs/customer_cdc_to_iceberg.py
```

Run dbt:

```bash
./dbt/.venv/bin/dbt run \
  --project-dir dbt \
  --select customer_order_summary
```

Run dbt tests:

```bash
./dbt/.venv/bin/dbt test \
  --project-dir dbt \
  --select customer_order_summary
```

Run Airflow:

```bash
export AIRFLOW_HOME="$PWD/airflow"
./airflow/.venv/bin/airflow standalone
```

Trigger the DAG:

```bash
./airflow/.venv/bin/airflow dags trigger realtime_cdc_ecommerce_pipeline
```

## Engineering Highlights

This project demonstrates:

- Change Data Capture
- Event-driven data pipelines
- Kafka-based streaming
- Spark Structured Streaming
- Apache Iceberg
- CDC history and current-state modeling
- Upserts and deletes with `MERGE`
- Decimal decoding
- Data quality validation
- dbt analytics engineering
- Workflow orchestration with Airflow
- Dockerized data infrastructure

## Project Status

✅ PostgreSQL CDC  
✅ Debezium  
✅ Kafka  
✅ Spark Structured Streaming  
✅ Apache Iceberg  
✅ CDC history  
✅ Current-state MERGE processing  
✅ Delete handling  
✅ Decimal decoding  
✅ Data quality validation  
✅ dbt analytics  
✅ dbt tests  
✅ Airflow orchestration  
✅ End-to-end successful DAG execution
