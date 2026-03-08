SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    o.product_id,
    o.quantity,
    p.unit_price,
    ROUND(o.quantity * p.unit_price, 2)  AS total_amount,
    o.status,
    YEAR(o.order_date)                   AS order_year,
    MONTH(o.order_date)                  AS order_month
FROM {{ ref('stg_orders') }}       o
LEFT JOIN {{ ref('dim_customer') }} c ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_product') }}  p ON o.product_id  = p.product_id