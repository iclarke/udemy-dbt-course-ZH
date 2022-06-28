-- Checks that there is no review date that is submitted before its listing was created
-- Make sure that every review date in fct_reviews is more recent than the associated created_at in dim_listings_cleansed

select * from {{ ref('fct_reviews') }} fr 
    left join {{ ref('dim_listings_cleansed') }} dl
    on fr.listing_id = dl.listing_id
    where fr.created_at > dl.created_at

