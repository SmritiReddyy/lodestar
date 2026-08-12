{#
    Type 2 history for seller attributes.

    A note on why this tracks *sellers* rather than customers: in the Olist
    schema `customer_id` is minted per order, so a customer row is immutable by
    construction and a Type 2 dimension over it would record changes that can
    never happen. Sellers have a genuine 1:1 natural key whose city, state and
    zip really can change — a seller relocates and every order after that ships
    from somewhere new. That makes it the honest place to demonstrate the
    pattern. `dim_customers` instead uses the person-level `customer_unique_id`
    rollup to answer the repeat-purchase questions Type 2 would otherwise serve.

    Strategy is `check` rather than `timestamp` because the source carries no
    reliable updated-at column; dbt compares the listed columns and opens a new
    version only when one of them actually differs.

    Demo the behaviour end to end with:
        lodestar-ingest seed --drift        # relocate a sample of sellers
        lodestar-ingest run --ingest-date <today>
        dbt snapshot
#}

{% snapshot snap_sellers %}

{{
    config(
        unique_key='seller_id',
        strategy='check',
        check_cols=['seller_city', 'seller_state', 'zip_code_prefix'],
        invalidate_hard_deletes=True
    )
}}

select
    seller_id,
    seller_city,
    seller_state,
    seller_region,
    zip_code_prefix
from {{ ref('stg_olist__sellers') }}

{% endsnapshot %}
