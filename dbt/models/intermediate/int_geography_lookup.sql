WITH nations AS (
    SELECT * FROM {{ ref('stg_nation') }}
),

regions AS (
    SELECT * FROM {{ ref('stg_region') }}
),

int_geography_lookup AS (
    SELECT
        nation.nation_id,
        UPPER(nation.nation_name) AS nation_name,
        UPPER(region.region_name) AS region_name

    FROM nations AS nation

    LEFT JOIN regions as region
        ON nation.region_id = region.region_id
)

SELECT * FROM int_geography_lookup