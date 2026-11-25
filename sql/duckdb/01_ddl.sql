CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_name VARCHAR NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_sk VARCHAR NOT NULL,
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    brand VARCHAR,
    list_price DECIMAL(12, 2) NOT NULL,
    attribute_hash VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    full_name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    region VARCHAR,
    segment VARCHAR,
    attribute_hash VARCHAR NOT NULL,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR PRIMARY KEY,
    customer_sk VARCHAR,
    product_sk VARCHAR,
    date_sk INTEGER,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_ts TIMESTAMP,
    quantity INTEGER,
    unit_price DECIMAL(12, 2),
    extended_amount DECIMAL(14, 2),
    currency VARCHAR,
    status VARCHAR,
    updated_at TIMESTAMP,
    ingest_date DATE
);
