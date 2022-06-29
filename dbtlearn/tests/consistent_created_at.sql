-- Checks that there is no review date that is submitted before its listing was created
-- Make sure that every review date in fct_reviews is more recent than the associated created_at in dim_listings_cleansed

SELECT * FROM {{ ref('dim_listings_cleansed') }} l
INNER JOIN {{ ref('fact_reviews') }} r
USING (listing_id)
WHERE l.created_at >= r.review_date

