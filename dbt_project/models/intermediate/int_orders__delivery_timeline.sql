{{
    config(
        materialized='view'
    )
}}

-- Turns five raw timestamps into the delivery measures the SLA dashboard
-- needs. All the fiddly null-handling lives here, once, rather than being
-- re-derived in every downstream query.

with orders as (

    select * from {{ ref('stg_olist__orders') }}

),

timings as (

    select
        order_id,
        customer_id,
        order_status,
        purchased_at,
        approved_at,
        shipped_at,
        delivered_at,
        estimated_delivery_at,
        purchased_date,
        estimated_delivery_date,
        delivered_date,

        {{ dbt.datediff('purchased_at', 'approved_at', 'hour') }}    as hours_to_approval,
        {{ dbt.datediff('approved_at', 'shipped_at', 'hour') }}      as hours_to_carrier,
        {{ dbt.datediff('purchased_at', 'delivered_at', 'day') }}    as days_to_delivery,
        {{ dbt.datediff('shipped_at', 'delivered_at', 'day') }}      as days_in_transit,

        -- Positive = delivered after the promise date, i.e. an SLA breach.
        {{ dbt.datediff('estimated_delivery_date', 'delivered_date', 'day') }}
                                                                     as delivery_vs_promise_days,
        {{ dbt.datediff('purchased_date', 'estimated_delivery_date', 'day') }}
                                                                     as promised_lead_days

    from orders

)

select
    order_id,
    customer_id,
    order_status,
    purchased_at,
    approved_at,
    shipped_at,
    delivered_at,
    estimated_delivery_at,
    purchased_date,
    estimated_delivery_date,
    delivered_date,

    hours_to_approval,
    hours_to_carrier,
    days_to_delivery,
    days_in_transit,
    delivery_vs_promise_days,
    promised_lead_days,

    (delivered_at is not null)                              as is_delivered,
    (order_status = 'canceled')                             as is_canceled,

    -- A late flag is only meaningful once the order has actually arrived;
    -- leaving it null for in-flight orders stops `avg(is_late)` from quietly
    -- counting undelivered orders as on time.
    case
        when delivered_at is null then null
        else delivery_vs_promise_days > {{ var('delivery_sla_grace_days') }}
    end                                                     as is_late,

    case
        when order_status = 'canceled'          then 'canceled'
        when delivered_at is not null
             and delivery_vs_promise_days > {{ var('delivery_sla_grace_days') }}
                                                then 'delivered_late'
        when delivered_at is not null           then 'delivered_on_time'
        when shipped_at is not null             then 'in_transit'
        when approved_at is not null            then 'approved'
        else 'pending'
    end                                                     as delivery_status,

    -- Data-quality flag, not a business state: upstream marks a handful of
    -- orders `delivered` with no delivery timestamp. Surfaced rather than
    -- hidden so the singular test can count it and the dashboard can exclude it.
    (order_status = 'delivered' and delivered_at is null)   as is_missing_delivery_timestamp

from timings
