CREATE DATABASE IF NOT EXISTS ecommerce_data;
USE ecommerce_data;

-- Staging Table
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    product_name TEXT,
    item_price DECIMAL(10, 2),
    quantity INT,
    order_date DATETIME,
    load_timestamp DATETIME
);

-- Analytics Fact Table
CREATE TABLE IF NOT EXISTS fact_orders (
    order_sk INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(255) UNIQUE,
    customer_id VARCHAR(255),
    product_name TEXT,
    total_order_value DECIMAL(10, 2),
    order_date DATETIME,
    processed_timestamp DATETIME
);

-- Quarantine Table
CREATE TABLE IF NOT EXISTS error_records (
    error_id INT AUTO_INCREMENT PRIMARY KEY,
    source_data JSON,
    error_message TEXT,
    detected_at DATETIME
);

-- Processed Files Tracking
CREATE TABLE IF NOT EXISTS processed_files (
    file_name VARCHAR(255) PRIMARY KEY,
    ingested_at DATETIME
);

