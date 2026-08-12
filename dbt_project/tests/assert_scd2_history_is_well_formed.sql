{{ config(severity='error') }}

/*
    Structural invariants of the Type 2 dimension.

    A Type 2 table that loses these guarantees is worse than useless — a
    point-in-time join silently returns two rows, doubling whatever it touched.
    Three things must hold:

      1. exactly one open (`valid_to is null`) version per seller;
      2. no version starts before it ends;
      3. no two versions of the same seller overlap in time.

    The overlap check is the one that matters: it is what makes the
    `purchased_at between valid_from and valid_to` join safe.
*/

with open_versions as (

    select
        seller_key,
        'expected exactly one open version' as violation
    from {{ ref('dim_sellers_history') }}
    where is_current
    group by seller_key
    having count(*) <> 1

),

reversed_bounds as (

    select
        seller_key,
        'valid_to precedes valid_from' as violation
    from {{ ref('dim_sellers_history') }}
    where valid_to is not null
      and valid_to <= valid_from

),

overlapping as (

    select
        a.seller_key,
        'versions overlap in time' as violation
    from {{ ref('dim_sellers_history') }} as a
    inner join {{ ref('dim_sellers_history') }} as b
        on a.seller_key = b.seller_key
        and a.seller_version_key <> b.seller_version_key
        and a.valid_from < coalesce(b.valid_to, timestamp '9999-12-31 00:00:00')
        and coalesce(a.valid_to, timestamp '9999-12-31 00:00:00') > b.valid_from

)

select * from open_versions
union all
select * from reversed_bounds
union all
select * from overlapping
