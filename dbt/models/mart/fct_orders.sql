WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

order_totals AS (
    SELECT * from {{ ref('int_order_items_aggregated') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customer') }}
),

geo AS (
    SELECT * FROM {{ ref('int_geography_lookup') }}
),

fct_orders AS (
    SELECT
        orders.*,
        totals.gross_item_sales_amount,
        totals.item_discount_amount,
        totals.total_tax_amount,
        totals.total_items,
        geo.nation_name,
        geo.region_name

    FROM orders

    LEFT JOIN order_totals AS totals
        ON orders.order_id = totals.order_id

    LEFT JOIN customers AS customer
        ON orders.customer_id = customer.customer_id

    LEFT JOIN geo
        ON customer.nation_id = geo.nation_id

)

SELECT * FROM fct_orders