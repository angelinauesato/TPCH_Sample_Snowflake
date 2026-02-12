WITH source AS (
    SELECT * FROM {{ source('tpch_sample_data', 'partsupp') }}
),

stg_partsupp AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ps_partkey', 'ps_suppkey']) }} AS part_supplier_key,
        ps_partkey AS part_id,
        ps_suppkey AS supplier_id,
        ps_availqty AS available_quantity,
        ps_supplycost AS supply_cost,
        ps_comment AS comment
    FROM source
)

SELECT * FROM stg_partsupp