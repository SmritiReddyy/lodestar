with source as (

    select * from {{ source('olist', 'customers') }}

),

renamed as (

    select
        -- Note the two-key design in this dataset: `customer_id` is minted per
        -- order, `customer_unique_id` is the stable person. Repeat-purchase
        -- analysis must use the latter.
        customer_id,
        customer_unique_id,

        lpad(trim(customer_zip_code_prefix), 5, '0')    as zip_code_prefix,
        {{ title_case('customer_city') }}                    as customer_city,
        upper(trim(customer_state))                     as customer_state,
        {{ brazil_region('customer_state') }}           as customer_region,

        ingest_date,
        _ingested_at

    from source

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by customer_id
        order by _ingested_at desc
    ) = 1

)

select * from deduplicated
