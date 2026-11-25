MERGE INTO dim_date AS t
USING staging_dates AS s
ON t.date_sk = s.date_sk
WHEN NOT MATCHED THEN
    INSERT (
        date_sk, full_date, year, quarter, month, month_name, day_of_month, day_name, is_weekend
    )
    VALUES (
        s.date_sk, s.full_date, s.year, s.quarter, s.month, s.month_name,
        s.day_of_month, s.day_name, s.is_weekend
    );
