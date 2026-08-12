{{ config(severity='error') }}

/*
    Cross-grain consistency: summing the line-item fact must reproduce the
    order fact's totals exactly.

    This is the test that catches a fan-out bug — the classic failure where a
    join in `fct_orders` silently multiplies revenue. Row counts alone would
    not catch it; the totals do.
*/

with item_rollup as (

    select
        order_key,
        count(*)                as item_count,
        sum(item_price)         as gross_item_revenue,
        sum(freight_value)      as freight_revenue,
        sum(item_total)         as order_total
    from {{ ref('fct_order_items') }}
    group by order_key

),

compared as (

    select
        o.order_key,
        o.item_count            as order_item_count,
        i.item_count            as rolled_item_count,
        o.gross_item_revenue    as order_revenue,
        i.gross_item_revenue    as rolled_revenue,
        o.order_total           as order_total,
        i.order_total           as rolled_total
    from {{ ref('fct_orders') }} as o
    inner join item_rollup as i
        on o.order_key = i.order_key

)

select *
from compared
where order_item_count != rolled_item_count
   or abs(order_revenue - rolled_revenue) > 0.01
   or abs(order_total - rolled_total) > 0.01
