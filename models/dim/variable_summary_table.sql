{{ config(
    materialized='table'  
) }}

WITH base AS (
    SELECT 
        offers.ASIN,
        offers.SELLERID,
        offers.NOTIFICATIONID,
        offers.ISBUYBOXWINNER,
        offers.ISOFFERPRIME,
        offers.SHIPSDOMESTICALLY,
        offers.PUBLISHTIME,
        offers.LISTINGPRICEAMOUNT,
        offers.SELLERFEEDBACKCOUNT,
        offers.SELLERPOSITIVEFEEDBACKRATING,
        offers.SHIPPINGAMOUNT,
        offers.SHIPPINGMINHOURS,
        offers.SHIPPINGMAXHOURS,
        summary.BUYBOXLISTINGPRICEAMOUNT,

        -- Difference between seller's listing price and mean listing price at publish time.
        AVG(offers.ListingPriceAmount) OVER (PARTITION BY offers.PublishTime) AS MeanListingPrice,
        ABS(offers.ListingPriceAmount - AVG(offers.ListingPriceAmount) OVER (PARTITION BY offers.PublishTime)) AS diff_listing_price_mean,

        -- Difference between seller's listing price and lowest listing price at publish time.
        MIN(offers.ListingPriceAmount) OVER (PARTITION BY offers.PublishTime) AS MinListingPrice,
        ABS(offers.ListingPriceAmount - MIN(offers.ListingPriceAmount) OVER (PARTITION BY offers.PublishTime)) AS diff_listing_price_lowest,

        -- Difference between seller's average shipping hours and mean average hours at publish time.
        AVG((offers.ShippingMinHours + offers.ShippingMaxHours) / 2) OVER (PARTITION BY offers.PublishTime) AS MeanShippingHours,
        ABS(((offers.ShippingMinHours + offers.ShippingMaxHours) / 2) - AVG((offers.ShippingMinHours + offers.ShippingMaxHours) / 2) OVER (PARTITION BY offers.PublishTime)) AS diff_average_hours_mean,

        -- Difference between seller's average shipping hours and lowest average hours at publish time.
        (offers.ShippingMinHours + offers.ShippingMaxHours) / 2 AS AvgShippingHours,
        MIN((offers.ShippingMinHours + offers.ShippingMaxHours) / 2) OVER (PARTITION BY offers.PublishTime) AS LowestAvgShippingHours,
        ABS(((offers.ShippingMinHours + offers.ShippingMaxHours) / 2) - MIN((offers.ShippingMinHours + offers.ShippingMaxHours) / 2) OVER (PARTITION BY offers.PublishTime)) AS diff_average_hours_lowest,

        -- Difference between seller's feedback count and mean feedback count at publish time.
        AVG(offers.SELLERFEEDBACKCOUNT) OVER (PARTITION BY offers.PublishTime) AS MeanSELLERFEEDBACKCOUNT,
        ABS(offers.SELLERFEEDBACKCOUNT - AVG(offers.SELLERFEEDBACKCOUNT) OVER (PARTITION BY offers.PublishTime)) AS diff_fpt_cnt_mean,

        -- Difference between seller's feedback count and highest feedback count at publish time.
        MAX(offers.SELLERFEEDBACKCOUNT) OVER (PARTITION BY offers.PublishTime) AS MAXSELLERFEEDBACKCOUNT,
        ABS(offers.SELLERFEEDBACKCOUNT - MAX(offers.SELLERFEEDBACKCOUNT) OVER (PARTITION BY offers.PublishTime)) AS diff_fpt_cnt_highest,

        -- Difference between seller's feedback rating and mean feedback rating at publish time.
        AVG(offers.SellerPositiveFeedbackRating) OVER (PARTITION BY offers.PublishTime) AS MeanSellerPositiveFeedbackRating,
        ABS(offers.SellerPositiveFeedbackRating - AVG(offers.SellerPositiveFeedbackRating) OVER (PARTITION BY offers.PublishTime)) AS diff_fpt_rate_mean,

        -- Difference between seller's feedback rating and highest feedback rating at publish time.
        MAX(offers.SellerPositiveFeedbackRating) OVER (PARTITION BY offers.PublishTime) AS MAXSellerPositiveFeedbackRating,
        ABS(offers.SellerPositiveFeedbackRating - MAX(offers.SellerPositiveFeedbackRating) OVER (PARTITION BY offers.PublishTime)) AS diff_fpt_rate_highest,

        -- Indicates if seller has the lowest listing price at publish time (1=True, 0=False).
        CASE WHEN offers.ListingPriceAmount = MIN(offers.ListingPriceAmount) OVER (PARTITION BY offers.ASIN, offers.PublishTime) THEN 1 ELSE 0 END AS is_lowest,

        -- Ratio of seller's rating to their listing price (rating per dollar).
        SAFE_DIVIDE(offers.SellerPositiveFeedbackRating, offers.ListingPriceAmount) AS ratio_rating_per_dollar,

        -- Ratio of seller's feedback count to their listing price (feedback per dollar).
        SAFE_DIVIDE(offers.SellerFeedbackCount, offers.ListingPriceAmount) AS ratio_count_per_dollar,

        -- Ratio between seller's current listing price and their BuyBox listing price.
        SAFE_DIVIDE(offers.ListingPriceAmount, summary.BUYBOXLISTINGPRICEAMOUNT) AS ratio_current_previous_price,

        -- Total number of assessments or reviews the product has received from customers.
        COUNT(*) OVER (PARTITION BY offers.PublishTime) AS number_of_assessments,

        -- Indicates if the seller is domestic (1=True, 0=False).
        CASE WHEN offers.SHIPSDOMESTICALLY THEN 1 ELSE 0 END AS is_domestic,

        -- Cost of shipping the product offered by the seller.
        offers.ShippingAmount AS shipping_price,

        -- Estimated shipping time (in hours).
        offers.ShippingMaxHours AS shipping_time_in_hours

    FROM 
    de-coe.buybox_dataset.dim_all_columns
    --{{ source("de-coe", "all_columns") }}
     AS offers

JOIN 
    dbt-437212.wellbefore.dim_summary_values AS summary
    ON 
        offers.NotificationId = summary.NotificationId
)

SELECT * FROM base
ORDER BY ASIN
