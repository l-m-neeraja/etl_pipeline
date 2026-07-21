# Scheduled E-Commerce ETL Pipeline using Apache Airflow & MySQL

This repository implements a containerized, scheduled, and idempotent ETL pipeline designed to ingest, validate, transform, and load e-commerce orders into an analytical MySQL data warehouse.

## Project Architecture

The architecture consists of:
1.  **Orchestrator**: Apache Airflow (running on a PostgreSQL metadata DB backend).
2.  **Staging & Analytics Warehouse**: MySQL 8.0 containing:
    *   `raw_orders`: Ingest staging table.
    *   `fact_orders`: Analytical warehouse destination.
    *   `error_records`: Quarantine table for data quality failures.
    *   `processed_files`: File-level tracking table.
3.  **ETL Logic**: Vectorized/row-wise Pandas checks and SQL upserts.

---

## Getting Started

### 1. Prerequisites
Ensure you have Docker and Docker Compose installed, and allocate at least 4GB of RAM to Docker Desktop.

### 2. Configure Environment Variables
Copy the example environment variables file and configure your credentials:
```bash
cp .env.example .env
```

### 3. Start the Pipeline
Launch the multi-container stack in the background:
```bash
docker-compose up -d --build
```
This command automatically builds the custom Airflow image (using `Dockerfile.etl`), launches the databases, runs schema migrations, creates the default admin user, and initializes the database tables using [create_tables.sql](file:///d:/G_P_P/my_etl_pipeline/sql/create_tables.sql).

### 4. Access the Airflow UI
*   **URL**: [http://localhost:8080](http://localhost:8080)
*   **Username**: `admin`
*   **Password**: `admin`

---

## Running and Testing the ETL Pipeline

### Triggering the DAG
1.  Access the Airflow UI.
2.  Unpause/Enable the `ecommerce_etl` DAG.
3.  Click **Trigger DAG** in the top-right menu to run the pipeline manually.
4.  View execution logs directly in the Graph View tasks to see statistics.

### Running Unit Tests
Unit tests validate the core data transformation and data quality rules entirely outside of the Airflow scheduler:
```bash
pytest tests/
```

---

## Idempotency and Data Quality Quarantine Strategy

### 1. Idempotency Design
To prevent duplicate records on DAG reruns:
*   **Ingestion Hook**: Scans and loads only new order files. Ingested files are logged in `processed_files`. If a file is reprocessed, MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` ensures no duplicate records are generated.
*   **Warehouse Hook**: Incremental loading to `fact_orders` uses `ON DUPLICATE KEY UPDATE` to avoid duplicate order IDs and safely overwrite fields if changes occurred.

### 2. Data Quality Quarantine Strategy
Data quality checks can sometimes cause pipelines to crash, blocking downstream analytics. This pipeline implements a robust **Quarantine Pattern** to prevent pipeline failures:

*   **Ingestion Phase**: Ingested records from CSV files are cleaned of data-type anomalies. To satisfy MySQL's primary key constraint on `raw_orders(order_id)` without crashing the ingestion:
    *   Rows with null/missing `order_id` are temporarily mapped to `MISSING_ID_{idx}_{timestamp}` placeholders.
    *   Duplicate `order_id` values within the batch are temporarily mapped to `{order_id}_DUP_{dup_index}` placeholders.
*   **Transformation & Validation Phase**:
    *   The `transform_and_validate_data` task queries unprocessed staging data. It checks all rules:
        *   **Rule 1 (Completeness & Uniqueness)**: `order_id` is present and unique within the batch (detecting our staging placeholders).
        *   **Rule 2 (Price Validity)**: `item_price` is a positive numeric (> 0).
        *   **Rule 3 (Quantity Validity)**: `quantity` is a positive integer (> 0).
        *   **Rule 4 (Date Validity)**: `order_date` is a valid parseable datetime.
    *   **Segregation**: Clean records are enriched with `total_order_value = item_price * quantity` and proceed. Bad records are split into `invalid_df`.
    *   **Quarantine Table**: Bad records are written into `error_records` immediately. The placeholder IDs are restored to their original invalid values, and the complete row is serialized to JSON in `source_data`, alongside the failure message in `error_message` and the current timestamp in `detected_at`.
    *   **Result**: Valid orders proceed to the final warehouse load task seamlessly. Quarantined records can be reviewed by engineers or BI analysts and corrected without stopping the flow of clean data.
