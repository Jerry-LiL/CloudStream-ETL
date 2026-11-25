-- SCD Type 1. A newer extract updates the current product row in place.
-- An older replay does not match the timestamp predicate, so it is ignored.
MERGE INTO dim_products AS t
USING staging_products AS s
ON t.product_id = s.product_id
WHEN MATCHED AND t.attribute_hash <> s.attribute_hash AND s.updated_at >= t.updated_at THEN
    UPDATE SET
        product_name = s.product_name,
        category = s.category,
        brand = s.brand,
        list_price = s.list_price,
        attribute_hash = s.attribute_hash,
        updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (
        product_sk, product_id, product_name, category, brand, list_price, attribute_hash, updated_at
    )
    VALUES (
        s.product_sk, s.product_id, s.product_name, s.category, s.brand,
        s.list_price, s.attribute_hash, s.updated_at
    );
