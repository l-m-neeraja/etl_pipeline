-- Create Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- Staging Table
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Fact Table
CREATE TABLE IF NOT EXISTS public.fact_transactions (
    transaction_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (transaction_id, transaction_date)
) PARTITION BY RANGE (transaction_date);


-- Example Partition (2026 Jan)
CREATE TABLE IF NOT EXISTS public.fact_transactions_y2026m01
PARTITION OF public.fact_transactions
FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

-- Quarantine Table
CREATE TABLE IF NOT EXISTS quarantine.transactions_errors (
    error_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    amount NUMERIC(10, 2),
    transaction_date DATE,
    status VARCHAR(20),
    updated_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT NOT NULL,
    error_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Products Dimension
CREATE TABLE IF NOT EXISTS public.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fact_transaction_date
ON public.fact_transactions (transaction_date);

CREATE INDEX IF NOT EXISTS idx_fact_customer_id
ON public.fact_transactions (customer_id);
