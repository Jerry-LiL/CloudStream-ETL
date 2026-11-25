CREATE OR REPLACE TEMPORARY TABLE staging_dates (
    date_sk NUMBER(8, 0),
    full_date DATE,
    year NUMBER(4, 0),
    quarter NUMBER(1, 0),
    month NUMBER(2, 0),
    month_name STRING,
    day_of_month NUMBER(2, 0),
    day_name STRING,
    is_weekend BOOLEAN
);

CREATE OR REPLACE TEMPORARY TABLE staging_products (
    product_sk STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    list_price NUMBER(12, 2),
    attribute_hash STRING,
    updated_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TEMPORARY TABLE staging_customers (
    customer_sk STRING,
    customer_id STRING,
    full_name STRING,
    email STRING,
    city STRING,
    region STRING,
    segment STRING,
    attribute_hash STRING,
    updated_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TEMPORARY TABLE staging_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    order_ts TIMESTAMP_NTZ,
    quantity NUMBER(10, 0),
    unit_price NUMBER(12, 2),
    extended_amount NUMBER(14, 2),
    currency STRING,
    status STRING,
    updated_at TIMESTAMP_NTZ,
    date_sk NUMBER(8, 0),
    ingest_date DATE
);
