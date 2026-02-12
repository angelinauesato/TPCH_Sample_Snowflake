with source AS (
    SELECT * FROM {{ source('tpch_sample_data', 'region') }}
),

stg_region AS (
    SELECT
        r_regionkey AS region_id,
        r_name AS region_name,
        r_comment AS comment
    FROM source
)

SELECT * FROM stg_region