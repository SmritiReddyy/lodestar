{{
    config(
        materialized='table',
        unique_key='zip_code_prefix'
    )
}}

-- Collapses the many geolocation samples per zip prefix into one row. This is
-- where the "many observations" shape from staging becomes a real dimension.

with geolocation as (

    select * from {{ ref('stg_olist__geolocation') }}

),

centroids as (

    select
        zip_code_prefix,
        avg(latitude)                       as latitude,
        avg(longitude)                      as longitude,
        count(*)                            as observation_count
    from geolocation
    group by zip_code_prefix

),

modal_place as (

    -- Pick the most frequently observed city/state for the prefix; ties break
    -- alphabetically so the dimension is stable between runs.
    select
        zip_code_prefix,
        city,
        state
    from (
        select
            zip_code_prefix,
            city,
            state,
            count(*) as observations
        from geolocation
        group by zip_code_prefix, city, state
    ) as ranked
    qualify row_number() over (
        partition by zip_code_prefix
        order by observations desc, city asc, state asc
    ) = 1

)

select
    c.zip_code_prefix,
    m.city,
    m.state,
    {{ brazil_region('m.state') }}          as region,
    round(cast(c.latitude as {{ dbt.type_numeric() }}), 6)  as latitude,
    round(cast(c.longitude as {{ dbt.type_numeric() }}), 6) as longitude,
    c.observation_count
from centroids as c
inner join modal_place as m
    on c.zip_code_prefix = m.zip_code_prefix
