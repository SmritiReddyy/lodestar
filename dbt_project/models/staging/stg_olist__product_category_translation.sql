with source as (

    select * from {{ source('olist', 'product_category_translation') }}

),

renamed as (

    select
        trim(product_category_name)                     as category_name_pt,
        trim(product_category_name_english)             as category_name_en,
        ingest_date,
        _ingested_at
    from source
    where nullif(trim(product_category_name), '') is not null

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by category_name_pt
        order by _ingested_at desc
    ) = 1

)

select * from deduplicated
