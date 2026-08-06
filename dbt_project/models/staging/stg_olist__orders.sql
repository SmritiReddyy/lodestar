with source as (

    select * from {{ source('olist', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        lower(trim(order_status))                       as order_status,

        order_purchase_timestamp                        as purchased_at,
        order_approved_at                               as approved_at,
        order_delivered_carrier_date                    as shipped_at,
        order_delivered_customer_date                   as delivered_at,
        order_estimated_delivery_date                   as estimated_delivery_at,

        cast(order_purchase_timestamp as date)          as purchased_date,
        cast(order_estimated_delivery_date as date)     as estimated_delivery_date,
        cast(order_delivered_customer_date as date)     as delivered_date,

        ingest_date,
        _ingested_at

    from source
    where order_purchase_timestamp >= timestamp '{{ var("earliest_valid_order_date") }} 00:00:00'

),

deduplicated as (

    -- A partition can legitimately be re-landed (backfill, replay). Keep the
    -- most recently ingested copy of each order so a replay is a no-op rather
    -- than a duplicate.
    select *
    from renamed
    qualify row_number() over (
        partition by order_id
        order by _ingested_at desc
    ) = 1

)

select * from deduplicated
