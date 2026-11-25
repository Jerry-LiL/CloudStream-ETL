CREATE TABLE IF NOT EXISTS dim_date (
    date_sk NUMBER(8, 0) NOT NULL,
    full_date DATE NOT NULL,
    year NUMBER(4, 0) NOT NULL,
    quarter NUMBER(1, 0) NOT NULL,
    month NUMBER(2, 0) NOT NULL,
    month_name STRING NOT NULL,
    day_of_month NUMBER(2, 0) NOT NULL,
    day_name STRING NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_sk)
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_sk STRING NOT NULL,
    product_id STRING NOT NULL,
    product_name STRING,
    category STRING,
    brand STRING,
    list_price NUMBER(12, 2) NOT NULL,
    attribute_hash STRING NOT NULL,
    updated_at TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT pk_dim_products PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk STRING NOT NULL,
    customer_id STRING NOT NULL,
    full_name STRING,
    email STRING,
    city STRING,
    region STRING,
    segment STRING,
    attribute_hash STRING NOT NULL,
    effective_from TIMESTAMP_NTZ NOT NULL,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_customers PRIMARY KEY (customer_sk)
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id STRING NOT NULL,
    customer_sk STRING,
    product_sk STRING,
    date_sk NUMBER(8, 0),
    customer_id STRING,
    product_id STRING,
    order_ts TIMESTAMP_NTZ,
    quantity NUMBER(10, 0),
    unit_price NUMBER(12, 2),
    extended_amount NUMBER(14, 2),
    currency STRING,
    status STRING,
    updated_at TIMESTAMP_NTZ,
    ingest_date DATE,
    CONSTRAINT pk_fact_orders PRIMARY KEY (order_id)
);
