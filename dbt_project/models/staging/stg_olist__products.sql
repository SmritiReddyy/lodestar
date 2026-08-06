with source as (

    select * from {{ source('olist', 'products') }}

),

translation as (

    select * from {{ ref('stg_olist__product_category_translation') }}

),

renamed as (

    select
        p.product_id,

        coalesce(nullif(trim(p.product_category_name), ''), 'unknown')
                                                        as category_name_pt,
        coalesce(t.category_name_en, 'unknown')         as category_name,

        -- Upstream spells these `_lenght`; fixed at the boundary so the typo
        -- never reaches a mart or a dashboard.
        p.product_name_lenght                           as product_name_length,
        p.product_description_lenght                    as product_description_length,
        p.product_photos_qty                            as product_photos_qty,

        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,

        round(cast(
            p.product_length_cm * p.product_height_cm * p.product_width_cm
        as {{ dbt.type_numeric() }}), 2)                as product_volume_cm3,

        p.ingest_date,
        p._ingested_at

    from source as p
    left join translation as t
        on nullif(trim(p.product_category_name), '') = t.category_name_pt

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by product_id
        order by _ingested_at desc
    ) = 1

)

select * from deduplicated
