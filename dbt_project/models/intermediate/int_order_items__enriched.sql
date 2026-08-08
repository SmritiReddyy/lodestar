{{
    config(
        materialized='view'
    )
}}

-- Line items with the product, seller and order context each row needs, so
-- `fct_order_items` is a projection rather than a five-way join.

with items as (

    select * from {{ ref('stg_olist__order_items') }}

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

products as (

    select * from {{ ref('stg_olist__products') }}

),

sellers as (

    select * from {{ ref('stg_olist__sellers') }}

),

customers as (

    select
        customer_id,
        customer_unique_id,
        customer_state,
        customer_region,
        zip_code_prefix as customer_zip_code_prefix
    from {{ ref('stg_olist__customers') }}

)

select
    i.order_item_key,
    i.order_id,
    i.line_number,

    -- Foreign keys out to the dimensions.
    i.product_id,
    i.seller_id,
    o.customer_id,
    c.customer_unique_id,

    -- Dates the fact is grained and partitioned on.
    o.purchased_at,
    o.purchased_date,
    i.shipping_limit_at,

    -- Degenerate attributes kept on the fact for cheap filtering.
    o.order_status,
    p.category_name,
    p.category_name_pt,
    s.seller_state,
    s.seller_region,
    c.customer_state,
    c.customer_region,

    -- Same-state shipments are the fast, cheap ones; the flag makes the
    -- freight-cost story sliceable without another join.
    (s.seller_state = c.customer_state)     as is_intrastate_shipment,

    p.product_weight_g,
    p.product_volume_cm3,

    i.item_price,
    i.freight_value,
    i.item_total,

    i.ingest_date

from items as i
inner join orders as o
    on i.order_id = o.order_id
left join products as p
    on i.product_id = p.product_id
left join sellers as s
    on i.seller_id = s.seller_id
left join customers as c
    on o.customer_id = c.customer_id
