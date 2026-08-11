{{
    config(
        materialized='table',
        partition_by=partition_config('purchased_date', 'month'),
        cluster_by=cluster_config(['customer_state'])
    )
}}

/*
    Pre-aggregated day x state x category grain, built for the BI layer.

    Dashboards could aggregate `fct_order_items` directly, but every panel would
    then re-scan the lowest-grain table on every refresh. Collapsing to this
    grain up front is what takes the dashboard queries from a full fact scan to
    a small table read; the before/after numbers are in docs/METRICS.md.
*/

with items as (

    select * from {{ ref('fct_order_items') }}

),

orders as (

    select
        purchased_date,
        customer_state,
        order_key,
        is_delivered,
        is_late,
        days_to_delivery,
        avg_review_score
    from {{ ref('fct_orders') }}

),

item_grain as (

    select
        purchased_date,
        customer_state,
        customer_region,
        category_name,
        count(*)                                            as item_count,
        count(distinct order_key)                           as order_count,
        count(distinct seller_key)                          as seller_count,
        sum(item_price)                                     as gross_item_revenue,
        sum(freight_value)                                  as freight_revenue,
        sum(item_total)                                     as total_revenue,
        avg(freight_ratio)                                  as avg_freight_ratio
    from items
    group by purchased_date, customer_state, customer_region, category_name

),

-- Order-level measures cannot be summed from the item grain without double
-- counting, so they are aggregated separately and joined back at day x state.
order_grain as (

    select
        purchased_date,
        customer_state,
        count(*)                                            as orders_placed,
        count(case when is_delivered then 1 end)            as orders_delivered,
        count(case when is_late then 1 end)                 as orders_late,
        avg(cast(days_to_delivery as double))               as avg_days_to_delivery,
        avg(avg_review_score)                               as avg_review_score
    from orders
    group by purchased_date, customer_state

)

select
    i.purchased_date,
    i.customer_state,
    i.customer_region,
    i.category_name,

    i.item_count,
    i.order_count,
    i.seller_count,
    round(cast(i.gross_item_revenue as {{ dbt.type_numeric() }}), 2)  as gross_item_revenue,
    round(cast(i.freight_revenue as {{ dbt.type_numeric() }}), 2)     as freight_revenue,
    round(cast(i.total_revenue as {{ dbt.type_numeric() }}), 2)       as total_revenue,
    round(cast(i.avg_freight_ratio as {{ dbt.type_numeric() }}), 4)   as avg_freight_ratio,

    o.orders_placed,
    o.orders_delivered,
    o.orders_late,
    case
        when coalesce(o.orders_delivered, 0) = 0 then null
        else round(cast(o.orders_late as {{ dbt.type_numeric() }}) / o.orders_delivered, 4)
    end                                                               as late_delivery_rate,
    round(cast(o.avg_days_to_delivery as {{ dbt.type_numeric() }}), 2) as avg_days_to_delivery,
    round(cast(o.avg_review_score as {{ dbt.type_numeric() }}), 3)     as avg_review_score

from item_grain as i
left join order_grain as o
    on i.purchased_date = o.purchased_date
    and i.customer_state = o.customer_state
