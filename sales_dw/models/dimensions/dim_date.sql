WITH date_spine AS (
    SELECT
        DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2023-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 1095))
)
SELECT 
    date_day AS date_id,
    DAY(date_day) AS day_of_month,
    MONTH(date_day) AS month_number,
    MONTHNAME(date_day) AS month_name,
    QUARTER(date_day) AS quarter,
    YEAR(date_day) AS year,
    DAYNAME(date_day) AS day_name,
    CASE WHEN DAYOFWEEK(date_day) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine