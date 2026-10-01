# Architecture

```mermaid
flowchart LR
    A[PostgreSQL] -->|CDC| B[Debezium]
    B --> C[Apache Kafka]

    C --> D[Spark Structured Streaming]

    D --> E[Iceberg CDC History]
    D --> F[Iceberg Current State]

    E --> G[customer_cdc]
    E --> H[orders_cdc]
    E --> I[payments_cdc]

    F --> J[customers_current]
    F --> K[orders_current]
    F --> L[payments_current]

    J --> M[Data Quality Checks]
    K --> M
    L --> M

    M --> N[dbt Analytics]
    N --> O[customer_order_summary]
    O --> P[dbt Tests]

    Q[Apache Airflow] --> J
    Q --> K
    Q --> L
    Q --> M
    Q --> N
    Q --> P
