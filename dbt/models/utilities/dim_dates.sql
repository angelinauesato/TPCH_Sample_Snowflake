{{
    config(
        materialized='table'
    )
}}

WITH generated_dates AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1990-01-01' as date)",
        end_date="cast('2030-01-01' as date)"
    ) }}
),

final_dates AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS sk_date,
        date_day AS date_actual,
        extract(YEAR FROM date_day) AS year_actual,
        extract(MONTH FROM date_day) AS month_actual,
        extract(DAY FROM date_day) AS day_actual,
        to_char(date_day, 'MMMM') AS month_name,
        to_char(date_day, 'DY') as day_name_short,     -- e.g., Mon, Tue
        to_char(date_day, 'DAY') as day_name_full,    -- e.g., Monday
        extract(dayofweek from date_day) as day_of_week, -- 0-6 where 0 is Sunday
        extract(quarter from date_day) as quarter,
        extract(dayofyear from date_day) as day_of_year

    FROM generated_dates
)

SELECT * FROM final_dates