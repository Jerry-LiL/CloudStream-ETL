-- Attach the warehouse surrogate keys, then upsert facts by order_id.
-- Matching on updated_at means reloading an older partition cannot clobber
-- a status that a later partition already applied.
CREATE OR REPLACE TABLE staging_fact_resolved AS
SELECT
    o.order_id,
    c.customer_sk,
    p.product_sk,
    o.date_sk,
    o.customer_id,
    o.product_id,
    o.order_ts,
    o.quantity,
    o.unit_price,
    o.extended_amount,
    o.currency,
    o.status,
    o.updated_at,
    o.ingest_date
FROM staging_orders AS o
LEFT JOIN dim_products AS p
    ON p.product_id = o.product_id
LEFT JOIN dim_customers AS c
    ON c.customer_id = o.customer_id
   AND o.order_ts >= c.effective_from
   AND (c.effective_to IS NULL OR o.order_ts < c.effective_to)
QUALIFY row_number() OVER (
    PARTITION BY o.order_id
    ORDER BY c.effective_from DESC NULLS LAST
) = 1;

MERGE INTO fact_orders AS t
USING staging_fact_resolved AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at >= t.updated_at THEN
    UPDATE SET
        customer_sk = s.customer_sk,
        product_sk = s.product_sk,
        date_sk = s.date_sk,
        customer_id = s.customer_id,
        product_id = s.product_id,
        order_ts = s.order_ts,
        quantity = s.quantity,
        unit_price = s.unit_price,
        extended_amount = s.extended_amount,
        currency = s.currency,
        status = s.status,
        updated_at = s.updated_at,
        ingest_date = s.ingest_date
WHEN NOT MATCHED THEN
    INSERT (
        order_id, customer_sk, product_sk, date_sk, customer_id, product_id,
        order_ts, quantity, unit_price, extended_amount, currency, status,
        updated_at, ingest_date
    )
    VALUES (
        s.order_id, s.customer_sk, s.product_sk, s.date_sk, s.customer_id, s.product_id,
        s.order_ts, s.quantity, s.unit_price, s.extended_amount, s.currency, s.status,
        s.updated_at, s.ingest_date
    );
