{{
    config(
        materialized='view'
    )
}}

-- Line items rolled up to order grain. `gross_item_revenue` deliberately
-- excludes freight so revenue and shipping cost stay separable downstream.

with items as (

    select * from {{ ref('stg_olist__order_items') }}

),

products as (

    select product_id, category_name from {{ ref('stg_olist__products') }}

),

joined as (

    select
        i.order_id,
        i.line_number,
        i.product_id,
        i.seller_id,
        i.item_price,
        i.freight_value,
        i.item_total,
        p.category_name
    from items as i
    left join products as p
        on i.product_id = p.product_id

),

aggregated as (

    select
        order_id,
        count(*)                                as item_count,
        count(distinct product_id)              as distinct_product_count,
        count(distinct seller_id)               as distinct_seller_count,
        sum(item_price)                         as gross_item_revenue,
        sum(freight_value)                      as freight_revenue,
        sum(item_total)                         as order_item_total,
        max(item_price)                         as max_item_price,
        min(item_price)                         as min_item_price
    from joined
    group by order_id

),

dominant_category as (

    -- The category the order spent most of its money in. Gives every order a
    -- single categorical label for dashboard slicing without double counting.
    select
        order_id,
        category_name                           as primary_category
    from (
        select
            order_id,
            category_name,
            sum(item_price) as category_revenue
        from joined
        group by order_id, category_name
    ) as by_category
    qualify row_number() over (
        partition by order_id
        order by category_revenue desc, category_name asc
    ) = 1

)

select
    a.*,
    d.primary_category
from aggregated as a
left join dominant_category as d
    on a.order_id = d.order_id
