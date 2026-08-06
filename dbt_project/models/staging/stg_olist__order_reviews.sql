with source as (

    select * from {{ source('olist', 'order_reviews') }}

),

renamed as (

    select
        review_id,
        order_id,
        review_score,

        nullif(trim(review_comment_title), '')          as review_title,
        nullif(trim(review_comment_message), '')        as review_message,

        review_creation_date                            as review_created_at,
        review_answer_timestamp                         as review_answered_at,

        case when nullif(trim(review_comment_message), '') is not null
             then true else false end                   as has_comment,

        -- Standard CSAT buckets, defined once here rather than in the BI tool.
        case
            when review_score >= 4 then 'promoter'
            when review_score = 3  then 'passive'
            when review_score <= 2 then 'detractor'
        end                                             as review_sentiment,

        ingest_date,
        _ingested_at

    from source
    where review_score between 1 and 5

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by review_id, order_id
        order by _ingested_at desc
    ) = 1

)

select * from deduplicated
