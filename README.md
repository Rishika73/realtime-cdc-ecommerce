# Real-Time CDC E-Commerce Data Platform

An end-to-end real-time data engineering project that captures transactional changes from PostgreSQL using Change Data Capture (CDC), streams them through Apache Kafka, processes them with Spark Structured Streaming, stores historical and current-state data in Apache Iceberg, validates data quality, builds analytics models with dbt, and orchestrates downstream workflows with Apache Airflow.

## Architecture

```text
PostgreSQL
    |
    | CDC
    v
Debezium
    |
    v
Apache Kafka
    |
    v
Spark Structured Streaming
    |
    +-----------------------------+
    |                             |
    v                             v
Iceberg CDC History        Iceberg Current State
    |                             |
    +-------------+---------------+
                  |
                  v
          Data Quality Checks
                  |
                  v
                 dbt
                  |
                  v
        Customer Revenue Analytics
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
- CDC operation handling:
  - `r` — snapshot/read
  - `c` — insert
  - `u` — update
  - `d` — delete
- Delete-aware CDC processing using Debezium `before` and `after` payloads
- Apache Iceberg CDC history tables
- Current-state tables built using Iceberg `MERGE`
- Kafka Connect Decimal decoding for PostgreSQL `NUMERIC` fields
- Data quality and referential integrity validation
- dbt analytics models and tests
- Airflow orchestration of downstream processing
- Fully containerized local development environment

## End-to-End Data Flow

```text
PostgreSQL
    |
    v
Debezium
    |
    v
Kafka Topics
    |
    v
Spark Structured Streaming
    |
    v
Apache Iceberg CDC Tables
    |
    v
Current-State MERGE Tables
    |
    v
Data Quality Checks
    |
    v
dbt Analytics
    |
    v
Airflow Orchestration
```

## CDC Topics

Debezium publishes PostgreSQL changes into Kafka topics including:

```text
ecommerce.public.customers
ecommerce.public.products
ecommerce.public.orders
ecommerce.public.order_items
ecommerce.public.payments
```

## CDC Operation Types

Debezium events use the following operation values:

```text
r = snapshot/read
c = create
u = update
d = delete
```

The pipeline preserves these events in append-only Iceberg CDC history tables.

## Iceberg Tables

### CDC History Tables

```text
local.ecommerce.customer_cdc
local.ecommerce.orders_cdc
local.ecommerce.payments_cdc
```

These tables preserve historical CDC events.

### Current-State Tables

```text
local.ecommerce.customers_current
local.ecommerce.orders_current
local.ecommerce.payments_current
```

These tables contain the latest active state of each business entity.

## CDC Current-State Processing

The pipeline uses Apache Iceberg `MERGE` logic to convert raw CDC history into current-state tables.

Example logic:

```text
Latest event = r/c/u
    -> INSERT or UPDATE current-state table

Latest event = d
    -> DELETE from current-state table
```

Delete events remain available in CDC history while deleted records are removed from the current-state layer.

## Customer CDC Example

A customer update flows through the platform like this:

```text
PostgreSQL UPDATE
    |
    v
Debezium
    |
    v
Kafka
    |
    v
Spark Structured Streaming
    |
    v
customer_cdc
    |
    v
customers_current
```

Example current customer state:

| customer_id | first_name | last_name | city | state | operation |
|---:|---|---|---|---|---|
| 1 | Ava | Patel | Orlando | TX | u |
| 2 | Noah | Kim | Houston | WA | u |
| 3 | Mia | Johnson | San Francisco | IL | u |
| 4 | Liam | Garcia | Denver | AZ | u |
| 5 | Emma | Brown | Miami | MA | u |

## Orders CDC

Orders are captured from:

```text
ecommerce.public.orders
```

The pipeline preserves the order CDC history and builds:

```text
local.ecommerce.orders_current
```

Example current order state:

| order_id | customer_id | order_status | order_total | operation |
|---:|---:|---|---:|---|
| 1 | 1 | DELIVERED | 219.98 | u |
| 2 | 2 | COMPLETED | 74.99 | r |
| 3 | 3 | SHIPPED | 149.98 | r |
| 4 | 4 | PROCESSING | 59.99 | r |
| 5 | 5 | COMPLETED | 134.98 | r |

## Payments CDC

Payments are captured from:

```text
ecommerce.public.payments
```

The pipeline creates:

```text
local.ecommerce.payments_current
```

Example payment state:

| payment_id | order_id | payment_method | payment_status | payment_amount | operation |
|---:|---:|---|---|---:|---|
| 1 | 1 | CREDIT_CARD | PAID | 219.98 | r |
| 2 | 2 | PAYPAL | PAID | 74.99 | r |
| 3 | 3 | CREDIT_CARD | PAID | 149.98 | r |
| 4 | 4 | CREDIT_CARD | PAID | 59.99 | u |
| 5 | 5 | APPLE_PAY | PAID | 134.98 | r |

## PostgreSQL Decimal Handling

PostgreSQL `NUMERIC` columns are emitted by Debezium as Kafka Connect Decimal values.

For example:

```text
"order_total":"Ve4="
```

The Spark pipeline decodes the binary decimal representation into proper decimal values such as:

```text
219.98
74.99
149.98
59.99
134.98
```

This allows order and payment amounts to be stored correctly as:

```text
DECIMAL(10,2)
```

## Data Quality

The Spark data-quality job validates:

- Unique customer IDs
- Unique order IDs
- Unique payment IDs
- Non-null customer IDs
- Positive order totals
- Positive payment amounts
- Valid customer references on orders
- Valid order references on payments

Current validation result:

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

The project contains a dbt analytics model:

```text
customer_order_summary
```

It combines:

```text
customers_current
orders_current
payments_current
```

to produce customer-level commerce metrics.

The model calculates:

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

The dbt model includes tests for:

- `customer_id` not null
- `customer_id` unique
- `total_orders` not null
- `total_order_value` not null
- `total_paid_amount` not null

Current dbt test result:

```text
PASS=5
WARN=0
ERROR=0
SKIP=0
TOTAL=5
```

## Airflow Orchestration

Apache Airflow orchestrates the downstream batch and analytics workflow.

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

A complete DAG run successfully executed all seven tasks.

Successful tasks:

```text
build_customers_current      success
build_orders_current         success
build_payments_current       success
data_quality_checks          success
check_spark_thrift_server    success
dbt_run                      success
dbt_test                     success
```

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
│   └── screenshots/
├── iceberg/
│   └── warehouse/
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

## Running the Platform

Start the infrastructure:

```bash
docker compose up -d
```

The Docker environment contains:

```text
PostgreSQL
ZooKeeper
Kafka
Debezium Kafka Connect
Spark
```

Verify containers:

```bash
docker ps
```

## Register Debezium Connector

The connector configuration is stored at:

```text
debezium/postgres-connector.json
```

Register it with Kafka Connect:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @debezium/postgres-connector.json \
  http://localhost:8083/connectors
```

## Run Customer CDC Streaming

```bash
docker exec -it ecommerce-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  /opt/spark/jobs/customer_cdc_to_iceberg.py
```

## Run Orders CDC Streaming

```bash
docker exec -it ecommerce-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  /opt/spark/jobs/orders_cdc_to_iceberg.py
```

## Run Payments CDC Streaming

```bash
docker exec -it ecommerce-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  /opt/spark/jobs/payments_cdc_to_iceberg.py
```

## Build Current-State Tables

Customers:

```bash
docker exec -it ecommerce-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  /opt/spark/jobs/build_customers_current.py
```

Orders:

```bash
docker exec -it ecommerce-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  /opt/spark/jobs/build_orders_current.py
```

Payments:

```bash
docker exec -it ecommerce-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  /opt/spark/jobs/build_payments_current.py
```

## Run Data Quality Checks

```bash
docker exec -it ecommerce-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  /opt/spark/jobs/data_quality_checks.py
```

## Run dbt

Run the analytics model:

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

## Run Airflow

Set Airflow home:

```bash
export AIRFLOW_HOME="$PWD/airflow"
```

Start Airflow locally:

```bash
./airflow/.venv/bin/airflow standalone
```

Trigger the DAG:

```bash
./airflow/.venv/bin/airflow dags trigger realtime_cdc_ecommerce_pipeline
```

Check DAG runs:

```bash
./airflow/.venv/bin/airflow dags list-runs realtime_cdc_ecommerce_pipeline
```

## Engineering Highlights

This project demonstrates practical experience with:

- Change Data Capture
- Event-driven architecture
- Distributed streaming systems
- Kafka topic design
- Debezium CDC
- Spark Structured Streaming
- Incremental processing
- Apache Iceberg
- CDC history modeling
- Current-state data modeling
- Upserts and deletes with `MERGE`
- PostgreSQL decimal decoding
- Data quality validation
- Referential integrity checks
- Analytics engineering with dbt
- dbt testing
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
