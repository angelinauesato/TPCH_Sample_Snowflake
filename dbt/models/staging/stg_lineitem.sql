WITH source AS (
    SELECT * FROM {{ source('tpch_sample_data', 'lineitem') }}
),

stg_lineitem AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['l_orderkey', 'l_linenumber']) }} AS lineitem_key,
        l_orderkey AS order_id,
        l_partkey AS part_id,
        l_suppkey AS supplier_id,
        l_linenumber AS line_number,
        l_quantity AS quantity,
        l_extendedprice AS extended_price,
        l_discount AS discount_percentage,
        l_tax AS tax_rate,
        l_returnflag AS return_flag,
        l_linestatus AS line_status,
        l_shipdate AS ship_date,
        l_commitdate AS commit_date,
        l_receiptdate AS receipt_date,
        l_shipinstruct AS ship_instructions,
        l_shipmode AS ship_mode,
        l_comment AS comment

    FROM source
)

SELECT * FROM stg_lineitem