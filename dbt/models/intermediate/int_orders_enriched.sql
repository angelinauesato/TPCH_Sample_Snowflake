WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

int_orders_enriched AS (
    SELECT
        *,
        CASE 
            WHEN total_price > 100000 THEN 'High Value'
            WHEN total_price > 20000 THEN 'Medium Value'
            ELSE 'Standard Value'
        END AS order_value_category,
        iff(order_status_code = 'F', true, false) AS is_completed

    FROM orders
)

SELECT * FROM int_orders_enriched