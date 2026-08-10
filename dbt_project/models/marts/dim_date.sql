{{
    config(
        materialized='table',
        unique_key='date_key'
    )
}}

-- A conformed date dimension. Every fact joins here rather than each dashboard
-- re-deriving "is this a weekend?" in its own dialect.
--
-- `extract(...)` is ANSI and behaves identically on DuckDB and BigQuery for
-- year/quarter/month/day. Day-of-week is the exception — BigQuery numbers
-- Sunday as 1, DuckDB as 0 — so it goes through `dbt_date.day_of_week`, which
-- normalises to the ISO convention (Monday = 1) on both.

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}

),

calendar as (

    select
        cast(date_day as date)                                      as date_key,

        cast({{ dbt.date_trunc('week', 'date_day') }} as date)      as week_start_date,
        cast({{ dbt.date_trunc('month', 'date_day') }} as date)     as month_start_date,
        cast({{ dbt.date_trunc('quarter', 'date_day') }} as date)   as quarter_start_date,
        cast({{ dbt.date_trunc('year', 'date_day') }} as date)      as year_start_date,

        cast(extract(year    from date_day) as integer)             as calendar_year,
        cast(extract(quarter from date_day) as integer)             as calendar_quarter,
        cast(extract(month   from date_day) as integer)             as calendar_month,
        cast(extract(day     from date_day) as integer)             as day_of_month,

        cast({{ dbt_date.day_of_week('date_day', isoweek=True) }} as integer)
                                                                    as iso_day_of_week

    from spine

)

select
    date_key,
    week_start_date,
    month_start_date,
    quarter_start_date,
    year_start_date,

    calendar_year,
    calendar_quarter,
    calendar_month,
    day_of_month,
    iso_day_of_week,

    -- ISO: 1 = Monday ... 7 = Sunday.
    (iso_day_of_week in (6, 7))                                     as is_weekend,

    {{ dbt.concat([
        "cast(calendar_year as " ~ dbt.type_string() ~ ")",
        "'-Q'",
        "cast(calendar_quarter as " ~ dbt.type_string() ~ ")"
    ]) }}                                                           as year_quarter,

    cast(calendar_year * 100 + calendar_month as integer)           as year_month_key

from calendar
