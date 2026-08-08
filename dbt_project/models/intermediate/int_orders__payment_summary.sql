{{
    config(
        materialized='view'
    )
}}

-- Collapses the one-to-many payment split down to one row per order, so
-- `fct_orders` can stay at order grain without fanning out.

with payments as (

    select * from {{ ref('stg_olist__order_payments') }}

),

aggregated as (

    select
        order_id,
        count(*)                                as payment_count,
        count(distinct payment_type)            as payment_type_count,
        sum(payment_value)                      as payment_total,
        max(payment_installments)               as max_installments,
        max(case when payment_type = 'credit_card' then 1 else 0 end) = 1
                                                as used_credit_card,
        max(case when payment_type = 'voucher'     then 1 else 0 end) = 1
                                                as used_voucher
    from payments
    group by order_id

),

primary_method as (

    -- "Primary" = the single largest payment line. Ties break on the earliest
    -- sequence number so the result is deterministic across runs.
    select
        order_id,
        payment_type                            as primary_payment_type,
        payment_installments                    as primary_payment_installments
    from payments
    qualify row_number() over (
        partition by order_id
        order by payment_value desc, payment_sequential asc
    ) = 1

)

select
    a.order_id,
    a.payment_count,
    a.payment_type_count,
    a.payment_total,
    a.max_installments,
    a.used_credit_card,
    a.used_voucher,
    p.primary_payment_type,
    p.primary_payment_installments
from aggregated as a
left join primary_method as p
    on a.order_id = p.order_id
