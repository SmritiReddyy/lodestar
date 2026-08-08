{{
    config(
        materialized='view'
    )
}}

-- An order can attract more than one review. Reduce to order grain, keeping
-- both the average (for trend charts) and the first response (for SLA work).

with reviews as (

    select * from {{ ref('stg_olist__order_reviews') }}

),

aggregated as (

    select
        order_id,
        count(*)                                    as review_count,
        avg(cast(review_score as double))           as avg_review_score,
        min(review_score)                           as min_review_score,
        max(case when has_comment then 1 else 0 end) = 1
                                                    as has_any_comment
    from reviews
    group by order_id

),

first_review as (

    select
        order_id,
        review_score                                as first_review_score,
        review_sentiment                            as first_review_sentiment,
        review_created_at                           as first_review_created_at,
        {{ dbt.datediff('review_created_at', 'review_answered_at', 'hour') }}
                                                    as review_response_hours
    from reviews
    qualify row_number() over (
        partition by order_id
        order by review_created_at asc, review_id asc
    ) = 1

)

select
    a.order_id,
    a.review_count,
    a.avg_review_score,
    a.min_review_score,
    a.has_any_comment,
    f.first_review_score,
    f.first_review_sentiment,
    f.first_review_created_at,
    f.review_response_hours
from aggregated as a
left join first_review as f
    on a.order_id = f.order_id
