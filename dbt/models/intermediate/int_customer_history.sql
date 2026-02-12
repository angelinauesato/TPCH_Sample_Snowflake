WITH customer_snapshots as (
    SELECT * FROM {{ ref('customer_snapshot') }}
),

int_customer_history AS (
    SELECT
        *,
        -- Adding a friendly boolean for "Current" vs "Historical"
        CASE WHEN dbt_valid_to is null
            THEN true
            ELSE false
        END AS is_current_record

    FROM customer_snapshots
)
SELECT * FROM int_customer_history