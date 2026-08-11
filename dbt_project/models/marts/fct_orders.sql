{{
    config(
        materialized='incremental',
        unique_key='order_key',
        on_schema_change='append_new_columns',
        partition_by=partition_config('purchased_date', 'day'),
        cluster_by=cluster_config(['customer_state', 'order_status'])
    )
}}

/*
    Grain: one row per order.

    Incremental by purchase date. A daily run rebuilds only the recent window
    instead of the full history; `dbt run --full-refresh` rebuilds everything.
    The lookback exists because late-arriving payments and reviews can change
    an order's row days after it was placed, so re-reading only "since the last
    max date" would freeze stale values.
*/

{% set lookback_days = 7 %}

with orders as (

    select * from {{ ref('int_orders__delivery_timeline') }}

    {% if is_incremental() %}
        -- Re-process the trailing window so late-arriving facts land.
        where purchased_date >= (
            select {{ dbt.dateadd('day', -1 * lookback_days, 'coalesce(max(purchased_date), cast(\'1900-01-01\' as date))') }}
            from {{ this }}
        )
    {% endif %}

),

customers as (

    select
        customer_id,
        customer_unique_id,
        customer_state,
        customer_region,
        zip_code_prefix as customer_zip_code_prefix
    from {{ ref('stg_olist__customers') }}

),

items      as ( select * from {{ ref('int_orders__item_summary') }} ),
payments   as ( select * from {{ ref('int_orders__payment_summary') }} ),
reviews    as ( select * from {{ ref('int_orders__review_summary') }} )

select
    -- Keys
    o.order_id                                          as order_key,
    o.customer_id                                       as customer_key,
    c.customer_unique_id,
    o.purchased_date                                    as purchased_date_key,
    o.estimated_delivery_date                           as estimated_delivery_date_key,
    o.delivered_date                                    as delivered_date_key,

    -- Degenerate dimensions
    o.order_status,
    o.delivery_status,
    c.customer_state,
    c.customer_region,
    c.customer_zip_code_prefix,
    i.primary_category,
    p.primary_payment_type,

    -- Timestamps
    o.purchased_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_at,

    -- Additive measures
    coalesce(i.item_count, 0)                           as item_count,
    coalesce(i.distinct_product_count, 0)               as distinct_product_count,
    coalesce(i.distinct_seller_count, 0)                as distinct_seller_count,
    round(cast(coalesce(i.gross_item_revenue, 0) as {{ dbt.type_numeric() }}), 2)
                                                        as gross_item_revenue,
    round(cast(coalesce(i.freight_revenue, 0) as {{ dbt.type_numeric() }}), 2)
                                                        as freight_revenue,
    round(cast(coalesce(i.order_item_total, 0) as {{ dbt.type_numeric() }}), 2)
                                                        as order_total,
    round(cast(coalesce(p.payment_total, 0) as {{ dbt.type_numeric() }}), 2)
                                                        as payment_total,

    -- Semi-additive / non-additive measures
    coalesce(p.payment_count, 0)                        as payment_count,
    p.max_installments,
    coalesce(r.review_count, 0)                         as review_count,
    round(cast(r.avg_review_score as {{ dbt.type_numeric() }}), 3)
                                                        as avg_review_score,
    r.first_review_score,
    r.first_review_sentiment,

    -- Delivery performance
    o.hours_to_approval,
    o.days_to_delivery,
    o.days_in_transit,
    o.promised_lead_days,
    o.delivery_vs_promise_days,
    o.is_delivered,
    o.is_canceled,
    o.is_late,

    -- Reconciliation: payments should settle the order total. A non-trivial
    -- gap means a split payment went missing or a voucher was double counted,
    -- which `assert_payments_reconcile_to_orders` tests on.
    round(cast(coalesce(p.payment_total, 0)
               - coalesce(i.order_item_total, 0) as {{ dbt.type_numeric() }}), 2)
                                                        as payment_variance,

    -- Data-quality flags carried onto the fact so dashboards can exclude them
    -- explicitly instead of silently.
    o.is_missing_delivery_timestamp,
    (p.order_id is null)                                as is_missing_payment,
    (i.order_id is null)                                as is_missing_items,

    o.purchased_date

from orders as o
left join customers as c on o.customer_id = c.customer_id
left join items    as i on o.order_id = i.order_id
left join payments as p on o.order_id = p.order_id
left join reviews  as r on o.order_id = r.order_id
