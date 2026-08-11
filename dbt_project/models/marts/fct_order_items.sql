{{
    config(
        materialized='incremental',
        unique_key='order_item_key',
        on_schema_change='append_new_columns',
        partition_by=partition_config('purchased_date', 'day'),
        cluster_by=cluster_config(['category_name', 'seller_state'])
    )
}}

/*
    Grain: one row per order line (order_id + line_number).

    The lowest-grain fact in the warehouse and the one every revenue number
    ultimately rolls up from. Partitioned on purchase date and clustered on the
    two columns the dashboards filter hardest, so a "last 30 days, one category"
    query touches a fraction of the table.
*/

{% set lookback_days = 7 %}

with items as (

    select * from {{ ref('int_order_items__enriched') }}

    {% if is_incremental() %}
        where purchased_date >= (
            select {{ dbt.dateadd('day', -1 * lookback_days, 'coalesce(max(purchased_date), cast(\'1900-01-01\' as date))') }}
            from {{ this }}
        )
    {% endif %}

),

delivery as (

    select
        order_id,
        delivery_status,
        is_delivered,
        is_late,
        days_to_delivery
    from {{ ref('int_orders__delivery_timeline') }}

)

select
    -- Keys
    i.order_item_key,
    i.order_id                                          as order_key,
    i.line_number,
    i.product_id                                        as product_key,
    i.seller_id                                         as seller_key,
    i.customer_id                                       as customer_key,
    i.customer_unique_id,
    i.purchased_date                                    as purchased_date_key,

    -- Degenerate dimensions, kept on the fact so the common filters need no join
    i.order_status,
    d.delivery_status,
    i.category_name,
    i.seller_state,
    i.seller_region,
    i.customer_state,
    i.customer_region,
    i.is_intrastate_shipment,

    -- Timestamps
    i.purchased_at,
    i.shipping_limit_at,

    -- Additive measures
    i.item_price,
    i.freight_value,
    i.item_total,

    -- Freight as a share of item price: the headline unit-economics metric.
    -- Guarded against divide-by-zero on free items.
    case
        when i.item_price > 0
        then round(cast(i.freight_value as {{ dbt.type_numeric() }}) / i.item_price, 4)
    end                                                 as freight_ratio,

    i.product_weight_g,
    i.product_volume_cm3,

    -- Delivery outcome, denormalised from the order for line-level analysis
    d.is_delivered,
    d.is_late,
    d.days_to_delivery,

    i.purchased_date

from items as i
left join delivery as d
    on i.order_id = d.order_id
