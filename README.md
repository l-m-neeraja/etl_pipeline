# Incremental E-Commerce ETL Pipeline using Apache Airflow

## Project Overview

This project implements an automated incremental ETL pipeline for e-commerce transaction data using:

- Apache Airflow (workflow orchestration)
- PostgreSQL (analytical data warehouse)
- Docker & Docker Compose (containerized setup)
- Pandas (data processing)
- Pytest (unit testing)

The main goal is to simulate a real-world production data pipeline that:

- Processes only new or updated records (incremental loading)
- Validates data quality before loading
- Separates invalid records into a quarantine table
- Loads clean data into a partitioned fact table
- Runs automatically using Airflow

---

## Architecture Summary

The pipeline follows this flow:

1. Generate daily transaction data (CSV files)
2. Ingest incremental data into staging table
3. Perform data quality checks
4. Move invalid records to quarantine table
5. UPSERT valid records into partitioned fact table

All steps are orchestrated by an Airflow DAG:
`ecommerce_transactions_etl`

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone <your_repo_url>
cd my_etl_pipeline
```

### 2. Configure environment variables

Copy the example file:

```bash
cp .env.example .env
```

You can modify credentials if needed.

### 3. Start the services

```bash
docker-compose up -d --build
```

### 4. Access Airflow

Open in browser:

```
http://localhost:8080
```

Login:
```
Username: admin
Password: admin
```

---

## Running the Pipeline

1. Enable the DAG `ecommerce_transactions_etl`
2. Click “Trigger DAG”
3. Monitor execution in Graph View

If everything is correct, all tasks will turn green.

---

## Incremental Loading Strategy

The pipeline does NOT reload full historical data.

It uses:

- `transaction_id` (unique key)
- `updated_at` timestamp

Logic:

- If transaction_id does not exist → INSERT
- If transaction_id exists but updated_at is newer → UPDATE
- If record is unchanged → IGNORE

This ensures efficient incremental processing.

---

## Data Quality Checks Implemented

The following checks are applied on staging data:

1. Schema validation
2. Uniqueness of transaction_id
3. Completeness (no NULLs in required fields)
4. Validity (amount must be > 0)
5. Referential integrity (product_id must exist in products table)

Invalid records are moved to:

```
quarantine.transactions_errors
```

Valid records are loaded into:

```
public.fact_transactions
```

---

## Database Design

### staging.transactions
Stores raw incremental data before validation.

### public.fact_transactions
- Partitioned by transaction_date (range partitioning)
- Primary key: (transaction_id, transaction_date)
- Uses UPSERT with ON CONFLICT

### quarantine.transactions_errors
Stores invalid records along with error messages.

### public.products
Dimension table used for referential integrity checks.

---

## Partitioning Strategy

The fact table uses PostgreSQL native range partitioning based on transaction_date.

Monthly partitions are created to:

- Improve performance
- Support time-based queries
- Enable scalable design

---

## Idempotency

The pipeline is fully idempotent:

- Ingestion checks updated_at before loading
- Fact table uses ON CONFLICT DO UPDATE
- Quarantine table uses unique constraint + ON CONFLICT DO NOTHING

Re-running the DAG produces the same final result without duplicates.

---

## Unit Testing

Unit tests are written for:

- Incremental logic
- Data quality checks

Run tests using:

```bash
pytest tests/
```

All tests should pass.

---

## Project Structure

```
my_etl_pipeline/
├── dags/
├── scripts/
├── sql/
├── tests/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

---

## Conclusion

This project demonstrates a production-style incremental ETL pipeline using Airflow and PostgreSQL. It includes workflow orchestration, data validation, partitioned modeling, idempotent design, and automated testing.

The system is reproducible, containerized, and designed following data engineering best practices.
