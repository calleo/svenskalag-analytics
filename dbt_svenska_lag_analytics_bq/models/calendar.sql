{{
    config(
        materialized='table'
    )
}}

{{
    dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2019-01-01' AS DATE)",
        end_date="DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR)"
   )
}}
