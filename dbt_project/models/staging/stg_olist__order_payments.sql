with source as (

    select * from {{ source('olist', 'order_payments') }}

),

renamed as (

    select
        order_id,
        payment_sequential,
        lower(trim(payment_type))                       as payment_type,

        -- A one-off purchase is recorded upstream as 0 instalments; normalise
        -- it to 1 so `sum(instalments)` is not silently wrong.
        case
            when payment_installments is null or payment_installments < 1 then 1
            else payment_installments
        end                                             as payment_installments,

        round(cast(payment_value as {{ dbt.type_numeric() }}), 2) as payment_value,

        ingest_date,
        _ingested_at

    from source
    where payment_value >= 0

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by order_id, payment_sequential
        order by _ingested_at desc
    ) = 1

)

select * from deduplicated
