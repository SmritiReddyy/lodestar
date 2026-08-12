{{ config(severity='error') }}

/*
    The pre-aggregated BI table must agree with the fact it summarises.

    An aggregate that drifts from its source is worse than no aggregate at all:
    the dashboard reads plausible numbers that are simply wrong. This compares
    grand totals, so any grouping-key or join mistake in
    `agg_daily_performance` shows up here.
*/

with fact_total as (

    select
        count(*)            as item_count,
        sum(item_price)     as gross_item_revenue,
        sum(item_total)     as total_revenue
    from {{ ref('fct_order_items') }}

),

agg_total as (

    select
        sum(item_count)         as item_count,
        sum(gross_item_revenue) as gross_item_revenue,
        sum(total_revenue)      as total_revenue
    from {{ ref('agg_daily_performance') }}

)

select
    f.item_count            as fact_item_count,
    a.item_count            as agg_item_count,
    f.gross_item_revenue    as fact_revenue,
    a.gross_item_revenue    as agg_revenue,
    f.total_revenue         as fact_total,
    a.total_revenue         as agg_total
from fact_total as f
cross join agg_total as a
where f.item_count != a.item_count
   or abs(f.gross_item_revenue - a.gross_item_revenue) > 0.01
   or abs(f.total_revenue - a.total_revenue) > 0.01
