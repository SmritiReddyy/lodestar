with source as (

    select * from {{ source('olist', 'sellers') }}

),

renamed as (

    select
        seller_id,
        lpad(trim(seller_zip_code_prefix), 5, '0')      as zip_code_prefix,
        {{ title_case('seller_city') }}                      as seller_city,
        upper(trim(seller_state))                       as seller_state,
        {{ brazil_region('seller_state') }}             as seller_region,

        ingest_date,
        _ingested_at

    from source

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by seller_id
        order by _ingested_at desc
    ) = 1

)

select * from deduplicated
