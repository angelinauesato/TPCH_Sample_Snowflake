WITH part_suppliers AS (
    SELECT * FROM {{ ref('stg_partsupp') }}
),

parts AS (
    SELECT * FROM {{ ref('stg_part') }}
),

calculations AS (
    SELECT
        p_supplier.part_id,
        part.part_name,
        p_supplier.supplier_id,
        p_supplier.supply_cost,
        
        -- Calculate the average cost for THIS part across ALL suppliers, 'Big Picture'
        avg(p_supplier.supply_cost) OVER (PARTITION BY p_supplier.part_id) AS avg_part_cost,
        
        -- Calculate the difference FROM average
        p_supplier.supply_cost - avg(p_supplier.supply_cost) OVER (PARTITION BY p_supplier.part_id) AS variance_FROM_avg,
        
        -- Calculate percentage variance
        CASE 
            WHEN avg_part_cost = 0 THEN 0
            ELSE (p_supplier.supply_cost - avg_part_cost) / avg_part_cost * 100 
        END AS variance_pct
        
    FROM part_suppliers AS p_supplier

    LEFT JOIN parts AS part ON p_supplier.part_id = part.part_id
),

int_part_price_variance AS (
    SELECT 
        *,
        -- Flag "Outliers" (suppliers charging 20% more than average)
        CASE 
            WHEN variance_pct > 20 THEN true 
            ELSE false 
        END AS is_high_cost_outlier

    FROM calculations
)

SELECT * FROM int_part_price_variance