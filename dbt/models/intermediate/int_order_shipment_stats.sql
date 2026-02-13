WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

lineitems AS (
    SELECT * FROM {{ ref('stg_lineitem') }}
),

int_order_shipment_stats AS (
    SELECT
        order_id,
        supplier_id,
        ship_date,
        order_date,
        -- Snowflake date subtraction: result is an integer (days)
        datediff('day', order_date, ship_date) AS days_to_ship,
        
        -- Business Logic: WAS it late? (Assumes 3-day target)
        CASE 
            WHEN datediff('day', order_date, ship_date) > 3 THEN true 
            ELSE false 
        END AS is_late_shipment

    FROM lineitems

    LEFT JOIN orders USING (order_id)
)

SELECT * FROM int_order_shipment_stats