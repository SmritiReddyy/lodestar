with source as (

    select * from {{ source('olist', 'geolocation') }}

),

cleaned as (

    select
        lpad(trim(geolocation_zip_code_prefix), 5, '0') as zip_code_prefix,
        cast(geolocation_lat as double)                 as latitude,
        cast(geolocation_lng as double)                 as longitude,
        {{ title_case('geolocation_city') }}                 as city,
        upper(trim(geolocation_state))                  as state,
        ingest_date,
        _ingested_at

    from source
    -- Quarantine impossible coordinates instead of letting them drag a map
    -- centroid into the Atlantic.
    where {{ in_brazil_bounds('geolocation_lat', 'geolocation_lng') }}

)

-- Deliberately *not* deduplicated to one row per zip: this table is a sample
-- of observations, and `dim_geography` is where it collapses to one row per
-- prefix. Keeping the samples here preserves the ability to change that rule.
select * from cleaned
