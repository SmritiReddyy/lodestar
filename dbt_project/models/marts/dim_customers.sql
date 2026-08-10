{{
    config(
        materialized='table',
        unique_key='customer_key',
        cluster_by=cluster_config(['customer_state'])
    )
}}

-- Grain: one row per `customer_id` — the per-order key the facts join on.
-- `customer_unique_id` carries the person identity, and the lifetime measures
-- are computed at that level so repeat-purchase analysis is possible without
-- re-deriving it in every query.

with customers as (

    select * from {{ ref('stg_olist__customers') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_status,
        purchased_at,
        purchased_date
    from {{ ref('stg_olist__orders') }}

),

item_summary as (

    select order_id, order_item_total from {{ ref('int_orders__item_summary') }}

),

geography as (

    select zip_code_prefix, city, state, region, latitude, longitude
    from {{ ref('dim_geography') }}

),

-- Person-level rollup, keyed on the stable id rather than the per-order one.
person_lifetime as (

    select
        c.customer_unique_id,
        count(distinct o.order_id)                          as lifetime_order_count,
        sum(coalesce(i.order_item_total, 0))                as lifetime_gross_value,
        min(o.purchased_date)                               as first_order_date,
        max(o.purchased_date)                               as most_recent_order_date
    from customers as c
    inner join orders as o
        on c.customer_id = o.customer_id
    left join item_summary as i
        on o.order_id = i.order_id
    group by c.customer_unique_id

)

select
    c.customer_id                                           as customer_key,
    c.customer_unique_id,

    c.zip_code_prefix,
    coalesce(g.city, c.customer_city)                       as customer_city,
    c.customer_state,
    c.customer_region,
    g.latitude,
    g.longitude,

    p.lifetime_order_count,
    round(cast(p.lifetime_gross_value as {{ dbt.type_numeric() }}), 2)
                                                            as lifetime_gross_value,
    p.first_order_date,
    p.most_recent_order_date,

    (p.lifetime_order_count > 1)                            as is_repeat_customer,

    -- Flags rows whose zip prefix has no entry in the geolocation reference
    -- data. Kept as a column instead of dropped so coverage is measurable.
    (g.zip_code_prefix is null)                             as is_missing_geography

from customers as c
left join person_lifetime as p
    on c.customer_unique_id = p.customer_unique_id
left join geography as g
    on c.zip_code_prefix = g.zip_code_prefix
