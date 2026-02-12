WITH suppliers AS (
    SELECT * FROM {{ ref('stg_supplier') }}
),

geo AS (
    SELECT * FROM {{ ref('int_geography_lookup') }}
),

int_supplier_enriched AS (
    SELECT
        supplier.*,
        geo.nation_name,
        geo.region_name

    FROM suppliers AS supplier

    LEFT JOIN geo ON supplier.nation_id = geo.nation_id
)

SELECT * FROM int_supplier_enriched