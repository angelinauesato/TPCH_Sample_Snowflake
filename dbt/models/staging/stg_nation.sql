WITH source AS (
    SELECT * FROM {{ source('tpch_sample_data', 'nation') }}
),

stg_nation AS (
    SELECT
        n_nationkey AS nation_id,
        n_name AS nation_name,
        n_regionkey AS region_id,
        n_comment AS comment

    FROM source
)

SELECT * FROM stg_nation