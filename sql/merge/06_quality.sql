SELECT 'duplicate_order_id' AS check_name, COUNT(*) AS failed_rows
FROM (
    SELECT order_id
    FROM fact_orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
) duplicate_orders
UNION ALL
SELECT 'multiple_current_customer', COUNT(*)
FROM (
    SELECT customer_id
    FROM dim_customers
    WHERE is_current
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) duplicate_current
UNION ALL
SELECT 'duplicate_customer_sk', COUNT(*)
FROM (
    SELECT customer_sk
    FROM dim_customers
    GROUP BY customer_sk
    HAVING COUNT(*) > 1
) duplicate_customer_keys
UNION ALL
SELECT 'duplicate_product_id', COUNT(*)
FROM (
    SELECT product_id
    FROM dim_products
    GROUP BY product_id
    HAVING COUNT(*) > 1
) duplicate_products
UNION ALL
SELECT 'duplicate_date_sk', COUNT(*)
FROM (
    SELECT date_sk
    FROM dim_date
    GROUP BY date_sk
    HAVING COUNT(*) > 1
) duplicate_dates
UNION ALL
SELECT 'staging_orders_missing_from_fact', COUNT(*)
FROM staging_orders AS staged
LEFT JOIN fact_orders AS facts
    ON facts.order_id = staged.order_id
WHERE facts.order_id IS NULL
UNION ALL
SELECT 'fact_rows_missing_keys', COUNT(*)
FROM staging_fact_resolved
WHERE customer_sk IS NULL OR product_sk IS NULL OR date_sk IS NULL
UNION ALL
SELECT 'current_customer_missing', COUNT(*)
FROM staging_customers AS staged
LEFT JOIN dim_customers AS customers
    ON customers.customer_id = staged.customer_id
   AND customers.is_current
WHERE customers.customer_id IS NULL;
