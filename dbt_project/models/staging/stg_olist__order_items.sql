with source as (

    select * from {{ source('olist', 'order_items') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }}
                                            as order_item_key,
        order_id,
        order_item_id                       as line_number,
        product_id,
        seller_id,

        shipping_limit_date                 as shipping_limit_at,

        -- Money is rounded once, here, so every downstream sum agrees.
        round(cast(price as {{ dbt.type_numeric() }}), 2)         as item_price,
        round(cast(freight_value as {{ dbt.type_numeric() }}), 2) as freight_value,
        round(cast(price as {{ dbt.type_numeric() }})
              + cast(freight_value as {{ dbt.type_numeric() }}), 2) as item_total,

        ingest_date,
        _ingested_at

    from source
    where price >= 0
      and freight_value >= 0

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by order_id, line_number
        order by _ingested_at desc
    ) = 1

)

select * from deduplicated
