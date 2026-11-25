-- SCD Type 2, applied to one partition.
-- Close the open version only when the staged attributes differ and the
-- extract is at least as new as the open row. A replay then finds the new
-- version already current and inserts nothing.
MERGE INTO dim_customers AS t
USING staging_customers AS s
ON t.customer_id = s.customer_id AND t.is_current
WHEN MATCHED AND t.attribute_hash <> s.attribute_hash AND s.updated_at >= t.effective_from THEN
    UPDATE SET
        is_current = FALSE,
        effective_to = s.updated_at;

MERGE INTO dim_customers AS t
USING staging_customers AS s
ON t.customer_id = s.customer_id AND t.is_current
WHEN NOT MATCHED THEN
    INSERT (
        customer_sk, customer_id, full_name, email, city, region, segment,
        attribute_hash, effective_from, effective_to, is_current
    )
    VALUES (
        s.customer_sk, s.customer_id, s.full_name, s.email, s.city, s.region, s.segment,
        s.attribute_hash, s.updated_at, NULL, TRUE
    );
