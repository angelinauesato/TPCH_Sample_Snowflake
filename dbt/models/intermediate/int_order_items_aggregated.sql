WITH line_items AS (
    SELECT * FROM {{ ref('stg_lineitem') }}
),

int_order_items_aggregated AS (
    SELECT
        order_id,
        sum(extended_price) AS gross_item_sales_amount,
        sum(extended_price * (1 - discount_percentage)) AS item_discount_amount,
        sum(tax_rate) AS total_tax_amount,
        count(lineitem_key) AS total_items

    FROM line_items

    GROUP BY order_id
)

SELECT * FROM int_order_items_aggregated