{{
    config(
        materialized='table',
        unique_key='product_key',
        cluster_by=cluster_config(['category_name'])
    )
}}

-- Grain: one row per product, with the sales rollup a merchandising dashboard
-- would otherwise recompute on every refresh.

with products as (

    select * from {{ ref('stg_olist__products') }}

),

item_sales as (

    select
        product_id,
        count(*)                                as units_sold,
        count(distinct order_id)                as orders_containing_product,
        count(distinct seller_id)               as seller_count,
        sum(item_price)                         as gross_revenue,
        avg(item_price)                         as avg_selling_price
    from {{ ref('stg_olist__order_items') }}
    group by product_id

)

select
    p.product_id                                as product_key,
    p.category_name,
    p.category_name_pt,

    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,

    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_volume_cm3,

    -- Shipping-cost bands. Boundaries are business rules, so they live in the
    -- model rather than in a BI tool's calculated field.
    case
        when p.product_weight_g is null      then 'unknown'
        when p.product_weight_g < 500        then 'light'
        when p.product_weight_g < 2000       then 'medium'
        when p.product_weight_g < 10000      then 'heavy'
        else 'oversized'
    end                                         as weight_class,

    coalesce(s.units_sold, 0)                   as units_sold,
    coalesce(s.orders_containing_product, 0)    as orders_containing_product,
    coalesce(s.seller_count, 0)                 as seller_count,
    round(cast(coalesce(s.gross_revenue, 0) as {{ dbt.type_numeric() }}), 2)
                                                as gross_revenue,
    round(cast(s.avg_selling_price as {{ dbt.type_numeric() }}), 2)
                                                as avg_selling_price,

    (p.category_name = 'unknown')               as is_uncategorised,
    (s.product_id is null)                      as is_never_sold

from products as p
left join item_sales as s
    on p.product_id = s.product_id
