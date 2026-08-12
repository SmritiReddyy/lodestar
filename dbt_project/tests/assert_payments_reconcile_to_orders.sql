{{ config(severity='error') }}

/*
    Money must balance: what a customer paid should equal what the order cost.

    Scoped to orders that have both payment rows and item rows — orders missing
    one side are a different defect, caught by
    `assert_order_completeness_within_tolerance`.

    One cent of tolerance absorbs the rounding applied when the item and payment
    amounts were each cast to a fixed scale in staging.
*/

select
    order_key,
    order_total,
    payment_total,
    payment_variance
from {{ ref('fct_orders') }}
where not is_missing_payment
  and not is_missing_items
  and abs(payment_variance) > 0.01
