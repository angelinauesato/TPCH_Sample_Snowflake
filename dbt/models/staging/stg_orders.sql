WITH source AS (
    SELECT * FROM {{ source('tpch_sample_data', 'orders') }}
),

stg_orders AS (
    SELECT
        o_orderkey AS order_id,
        o_custkey AS customer_id,
        o_orderstatus AS order_status_code,
        o_totalprice AS total_price,
        o_orderdate AS order_date,
        -- Split '1-URGENT' into 1 and 'URGENT'
        try_to_number(left(o_orderpriority, 1)) as order_priority_level,
        {{ clean_priority('o_orderpriority') }} as order_priority_label,
        split_part(o_clerk, '#', 2) AS clerk_id,
        o_shippriority AS ship_priority,
        o_comment AS comment

    FROM source
)

SELECT * FROM stg_orders