
{{
      config(
        materialized = 'incremental',
        on_schema_change= 'fail'
        )
}}


with src_reviews as (
    select 
        listing_id, 
        review_date, 
        NVL(reviewer_name, 'Anonymous'),
        review_text,
        review_sentiment 
    from {{ref('src_reviews')}}
)

select * from src_reviews
where review_text is not null
{%if is_incremental() %}
  and review_date >=( select max(review_date) from {{ this }})
{% endif %}