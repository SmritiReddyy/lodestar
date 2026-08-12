{{
    config(
        severity='warn',
        warn_if='> 0',
        error_if='> 1500'
    )
}}

/*
    A *threshold* test rather than a binary one.

    Some orders genuinely arrive without payment rows, and a handful are marked
    `delivered` with no delivery timestamp. Both quirks exist in the real Olist
    export. Asserting zero would mean a permanently red pipeline that everyone
    learns to ignore; asserting nothing would mean a silent regression the day
    the loader starts dropping payments.

    So: warn on any occurrence (visible in every run's summary), fail the build
    only when the count crosses a level that cannot be explained by known
    upstream messiness. Raise the ceiling deliberately, with a commit message,
    if the source genuinely changes.
*/

select
    order_key,
    order_status,
    delivery_status,
    is_missing_payment,
    is_missing_items,
    is_missing_delivery_timestamp
from {{ ref('fct_orders') }}
where is_missing_payment
   or is_missing_items
   or is_missing_delivery_timestamp
