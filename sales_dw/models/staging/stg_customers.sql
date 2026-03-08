-- Staging: Clean raw customers data

SELECT 
    customer_id,
    TRIM(customer_name) AS customer_name,
    TRIM(UPPER(city)) AS city,
    TRIM(segment) AS segment,
    TRY_TO_DATE(created_date, 'YYYY-MM-DD') AS created_date
FROM {{ source('sales', 'RAW_CUSTOMERS') }}
WHERE customer_id IS NOT NULL

