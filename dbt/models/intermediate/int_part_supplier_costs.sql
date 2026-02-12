WITH parts AS (
    SELECT * FROM {{ ref('stg_part') }}
),

part_suppliers AS (
    SELECT * FROM {{ ref('stg_partsupp') }}
),

int_part_supplier_costs AS (
    SELECT
        p_supplier.part_id,
        part.part_name,
        part.manufacturer,
        p_supplier.supplier_id,
        p_supplier.supply_cost,
        -- Calculate how much higher the cost is than the retail price
        (p_supplier.supply_cost / nullif(part.retail_price, 0)) * 100 AS cost_to_retail_pct

    FROM part_suppliers AS p_supplier

    LEFT JOIN parts AS part ON p_supplier.part_id = part.part_id
)

SELECT * FROM int_part_supplier_costs