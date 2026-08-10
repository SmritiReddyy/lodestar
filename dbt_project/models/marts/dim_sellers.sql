{{
    config(
        materialized='table',
        unique_key='seller_key',
        cluster_by=cluster_config(['seller_state'])
    )
}}

-- Grain: one row per seller, with fulfilment performance attached. Late-rate
-- per seller is the measure the marketplace ops dashboard is built on.

with sellers as (

    select * from {{ ref('stg_olist__sellers') }}

),

geography as (

    select zip_code_prefix, city, state, latitude, longitude
    from {{ ref('dim_geography') }}

),

seller_orders as (

    select
        i.seller_id,
        count(distinct i.order_id)                          as order_count,
        count(*)                                            as items_sold,
        sum(i.item_price)                                   as gross_revenue,
        sum(i.freight_value)                                as freight_charged,
        min(o.purchased_date)                               as first_sale_date,
        max(o.purchased_date)                               as most_recent_sale_date
    from {{ ref('stg_olist__order_items') }} as i
    inner join {{ ref('stg_olist__orders') }} as o
        on i.order_id = o.order_id
    group by i.seller_id

),

seller_delivery as (

    -- Only delivered orders carry a meaningful late flag, so the denominator
    -- is delivered orders rather than all orders.
    select
        i.seller_id,
        count(distinct case when d.is_delivered then i.order_id end)
                                                            as delivered_order_count,
        count(distinct case when d.is_late then i.order_id end)
                                                            as late_order_count,
        avg(cast(d.days_to_delivery as double))             as avg_days_to_delivery
    from {{ ref('stg_olist__order_items') }} as i
    inner join {{ ref('int_orders__delivery_timeline') }} as d
        on i.order_id = d.order_id
    group by i.seller_id

)

select
    s.seller_id                                             as seller_key,
    coalesce(g.city, s.seller_city)                         as seller_city,
    s.seller_state,
    s.seller_region,
    s.zip_code_prefix,
    g.latitude,
    g.longitude,

    coalesce(o.order_count, 0)                              as order_count,
    coalesce(o.items_sold, 0)                               as items_sold,
    round(cast(coalesce(o.gross_revenue, 0) as {{ dbt.type_numeric() }}), 2)
                                                            as gross_revenue,
    round(cast(coalesce(o.freight_charged, 0) as {{ dbt.type_numeric() }}), 2)
                                                            as freight_charged,
    o.first_sale_date,
    o.most_recent_sale_date,

    coalesce(d.delivered_order_count, 0)                    as delivered_order_count,
    coalesce(d.late_order_count, 0)                         as late_order_count,
    case
        when coalesce(d.delivered_order_count, 0) = 0 then null
        else round(cast(d.late_order_count as {{ dbt.type_numeric() }})
                   / d.delivered_order_count, 4)
    end                                                     as late_delivery_rate,
    round(cast(d.avg_days_to_delivery as {{ dbt.type_numeric() }}), 2)
                                                            as avg_days_to_delivery

from sellers as s
left join geography as g
    on s.zip_code_prefix = g.zip_code_prefix
left join seller_orders as o
    on s.seller_id = o.seller_id
left join seller_delivery as d
    on s.seller_id = d.seller_id
