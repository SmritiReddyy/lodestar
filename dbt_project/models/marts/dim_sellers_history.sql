{{
    config(
        materialized='table',
        unique_key='seller_version_key'
    )
}}

/*
    **Grain: one row per seller per period their attributes were unchanged.**

    The Type 2 companion to `dim_sellers`. `dim_sellers` answers "where is this
    seller now?"; this answers "where were they when that order shipped?" — the
    difference that stops a relocation from silently rewriting last year's
    regional revenue split.

    Join a fact to the version that was current at the time:

        from fct_order_items i
        join dim_sellers_history h
          on i.seller_key = h.seller_key
         and i.purchased_at >= h.valid_from
         and (i.purchased_at < h.valid_to or h.valid_to is null)
*/

with snapshotted as (

    select * from {{ ref('snap_sellers') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['seller_id', 'dbt_valid_from']) }}
                                                as seller_version_key,
    seller_id                                   as seller_key,

    seller_city,
    seller_state,
    seller_region,
    zip_code_prefix,

    dbt_valid_from                              as valid_from,
    dbt_valid_to                                as valid_to,

    -- Exactly one row per seller has a null `valid_to`; that is the live one.
    (dbt_valid_to is null)                      as is_current,

    row_number() over (
        partition by seller_id
        order by dbt_valid_from
    )                                           as version_number

from snapshotted
