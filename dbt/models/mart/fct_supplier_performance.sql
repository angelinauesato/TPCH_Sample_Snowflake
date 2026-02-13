WITH suppliers AS (
    SELECT * FROM {{ ref('stg_supplier') }}
),

geo AS (
    SELECT * FROM {{ ref('int_geography_lookup') }}
),

price_variance AS (
    SELECT * FROM {{ ref('int_part_price_variance') }}
),

shipping AS (
    -- Rolling up shipping stats to the supplier level
    SELECT
        supplier_id,
        avg(days_to_ship) AS avg_days_to_ship,
        count(CASE WHEN is_late_shipment THEN 1 END) AS total_late_shipments,
        count(order_id) AS total_orders

    FROM {{ ref('int_order_shipment_stats') }}

    GROUP BY supplier_id
),

fct_supplier_performance AS (
    SELECT
        supplier.supplier_id,
        supplier.supplier_name,
        geo.nation_name,
        geo.region_name,
        
        -- Price Metrics
        variance.part_name,
        variance.supply_cost,
        variance.variance_pct AS price_variance_pct,
        variance.is_high_cost_outlier,
        
        -- Shipping Metrics
        shipping.avg_days_to_ship,
        shipping.total_late_shipments,
        (shipping.total_late_shipments / nullif(shipping.total_orders, 0)) * 100 AS late_shipment_rate

    FROM suppliers AS supplier

    LEFT JOIN geo
        ON supplier.nation_id = geo.nation_id

    LEFT JOIN price_variance AS variance
        ON supplier.supplier_id = variance.supplier_id

    LEFT JOIN shipping
        ON supplier.supplier_id = shipping.supplier_id
)

SELECT * FROM fct_supplier_performance