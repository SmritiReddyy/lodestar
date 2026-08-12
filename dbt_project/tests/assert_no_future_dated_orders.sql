{{ config(severity='error') }}

/*
    No order may be purchased in the future, and no order may be delivered
    before it was purchased.

    Both are impossible in the real world, so either one means a timezone bug
    or a bad parse — the kind of defect that quietly skews every time-series
    chart until someone notices the trend line ends next March.
*/

select
    order_key,
    purchased_at,
    delivered_at,
    'purchased in the future' as violation
from {{ ref('fct_orders') }}
where purchased_at > {{ dbt.current_timestamp() }}

union all

select
    order_key,
    purchased_at,
    delivered_at,
    'delivered before purchase' as violation
from {{ ref('fct_orders') }}
where delivered_at is not null
  and delivered_at < purchased_at

union all

select
    order_key,
    purchased_at,
    delivered_at,
    'purchased before the business existed' as violation
from {{ ref('fct_orders') }}
where purchased_at < timestamp '{{ var("earliest_valid_order_date") }} 00:00:00'
