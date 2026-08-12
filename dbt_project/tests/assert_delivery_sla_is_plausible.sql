{{
    config(
        severity='error'
    )
}}

/*
    Guards the SLA metric itself, not the rows feeding it.

    `late_delivery_rate` is the headline number on the operations dashboard. If
    a join or a null-handling change pushed it to 0% or 100%, every individual
    row test would still pass while the metric became nonsense. This asserts
    the aggregate lands in a range that is merely bad, not impossible.

    Deliberately wide: it is a smoke alarm for broken logic, not a business
    target. Tighten it only if the underlying rate becomes genuinely stable.
*/

with sla as (

    select
        count(*)                                            as delivered_orders,
        sum(case when is_late then 1 else 0 end)            as late_orders,
        1.0 * sum(case when is_late then 1 else 0 end) / nullif(count(*), 0)
                                                            as late_rate
    from {{ ref('fct_orders') }}
    where is_delivered

)

select
    delivered_orders,
    late_orders,
    late_rate
from sla
where delivered_orders = 0
   or late_rate <= 0.0
   or late_rate >= 0.60
