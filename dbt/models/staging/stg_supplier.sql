WITH source AS (
    SELECT * FROM {{ source('tpch_sample_data', 'supplier') }}
),

stg_supplier AS (
    SELECT
        s_suppkey AS supplier_id,
        s_name AS supplier_name,
        s_address AS address,
        s_nationkey AS nation_id,
        s_phone AS phone_number,
        s_acctbal AS account_balance,
        s_comment AS comment

    FROM source
)

SELECT * FROM stg_supplier