with mart_fullmoon_reviews as (
    select * from {{ ref('mart_fullmoon_reviews') }}
)

select
    is_full_moon,
    review_sentiment, 
    count(*) as reviews
from mart_fullmoon_reviews
group by 1,2
order by 1,2