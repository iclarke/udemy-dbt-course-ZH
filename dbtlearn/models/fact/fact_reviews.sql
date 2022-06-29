
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
        NVL(reviewer_name, 'Anonymous') as reviewer_name,
        review_text,
        review_sentiment 
    from {{ref('src_reviews')}}
)

select 
{{dbt_utils.surrogate_key(['listing_id','review_date','reviewer_name', 'review_text'])}} as review_id
,* from src_reviews
where review_text is not null
{%if is_incremental() %}
  and review_date >=( select max(review_date) from {{ this }})
{% endif %}