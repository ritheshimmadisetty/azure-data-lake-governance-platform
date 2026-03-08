--Staging: Clean raw products data

SELECT
    product_id,
    TRIM(product_name) AS product_name,
    TRIM(category) AS category,
    unit_price
FROM {{source('sales', 'RAW_PRODUCTS') }}
WHERE product_id IS NOT NULL
  AND unit_price > 0