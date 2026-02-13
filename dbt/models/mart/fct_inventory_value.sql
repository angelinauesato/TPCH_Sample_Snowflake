

WITH part_suppliers as (
    SELECT * FROM {{ ref('stg_partsupp') }}
),

parts as (
    SELECT * FROM {{ ref('stg_part') }}
),

fct_inventory_value AS (
    SELECT
        p_suplier.part_supplier_key,
        p_suplier.part_id,
        p_suplier.supplier_id,
        parts.part_name,
        parts.manufacturer,
        p_suplier.available_quantity,
        p_suplier.supply_cost,
        -- Total value of stock on hand for this part/supplier combo
        (p_suplier.available_quantity * p_suplier.supply_cost) AS inventory_value_amount

    FROM part_suppliers AS p_suplier

    LEFT JOIN parts
        ON p_suplier.part_id = parts.part_id
)
SELECT * FROM fct_inventory_value