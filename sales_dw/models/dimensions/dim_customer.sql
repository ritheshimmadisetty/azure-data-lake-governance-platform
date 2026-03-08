SELECT
    customer_id,
    customer_name,
    city,
    segment,
    created_date,
    created_date        AS effective_date,
    '9999-12-31'::DATE  AS end_date,
    TRUE                AS is_current
FROM {{ref('stg_customers') }}