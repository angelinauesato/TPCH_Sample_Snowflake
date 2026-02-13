WITH customers AS (
    SELECT * FROM {{ ref('stg_customer') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

fct_customer_orders AS (
    SELECT
        customer.customer_id,
        customer.customer_name,
        count(orders.order_id) AS total_lifetime_orders,
        sum(orders.total_price) AS total_lifetime_spend,
        min(orders.order_date) AS first_order_date,
        max(orders.order_date) AS most_recent_order_date,
        avg(orders.total_price) AS average_order_valueorder

    FROM customers AS customer

    LEFT JOIN orders
        ON customer.customer_id = orders.customer_id
    
    GROUP BY 1, 2
)

SELECT * FROM fct_customer_orders