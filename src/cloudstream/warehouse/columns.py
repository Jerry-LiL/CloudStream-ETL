"""Staging columns. The MERGE scripts and both loaders share these names."""

CUSTOMER_COLUMNS = [
    "customer_sk",
    "customer_id",
    "full_name",
    "email",
    "city",
    "region",
    "segment",
    "attribute_hash",
    "updated_at",
]

PRODUCT_COLUMNS = [
    "product_sk",
    "product_id",
    "product_name",
    "category",
    "brand",
    "list_price",
    "attribute_hash",
    "updated_at",
]

DATE_COLUMNS = [
    "date_sk",
    "full_date",
    "year",
    "quarter",
    "month",
    "month_name",
    "day_of_month",
    "day_name",
    "is_weekend",
]

ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_ts",
    "quantity",
    "unit_price",
    "extended_amount",
    "currency",
    "status",
    "updated_at",
    "date_sk",
    "ingest_date",
]

# Used when a partition produced no parquet files, so MERGE still has a typed table.
EMPTY_TABLES = {
    "staging_customers": """
        customer_sk VARCHAR,
        customer_id VARCHAR,
        full_name VARCHAR,
        email VARCHAR,
        city VARCHAR,
        region VARCHAR,
        segment VARCHAR,
        attribute_hash VARCHAR,
        updated_at TIMESTAMP
    """,
    "staging_products": """
        product_sk VARCHAR,
        product_id VARCHAR,
        product_name VARCHAR,
        category VARCHAR,
        brand VARCHAR,
        list_price DECIMAL(12, 2),
        attribute_hash VARCHAR,
        updated_at TIMESTAMP
    """,
    "staging_dates": """
        date_sk INTEGER,
        full_date DATE,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        month_name VARCHAR,
        day_of_month INTEGER,
        day_name VARCHAR,
        is_weekend BOOLEAN
    """,
    "staging_orders": """
        order_id VARCHAR,
        customer_id VARCHAR,
        product_id VARCHAR,
        order_ts TIMESTAMP,
        quantity INTEGER,
        unit_price DECIMAL(12, 2),
        extended_amount DECIMAL(14, 2),
        currency VARCHAR,
        status VARCHAR,
        updated_at TIMESTAMP,
        date_sk INTEGER,
        ingest_date DATE
    """,
}
