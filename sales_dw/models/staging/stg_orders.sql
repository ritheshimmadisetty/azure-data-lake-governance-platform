-- Staging: clean raw orders
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    TRY_TO_DATE(order_date, 'YYYY-MM-DD') AS order_date,
    TRIM(status) AS status
FROM {{ source('sales', 'RAW_ORDERS') }}
WHERE order_id    IS NOT NULL
  AND customer_id IS NOT NULL
  AND quantity    > 0