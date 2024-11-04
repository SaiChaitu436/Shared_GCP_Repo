WITH source_data AS (
    SELECT 
        message_id,
        PARSE_JSON(message_body) AS raw_data
    FROM 
        {{ source("de-coe", "BB_Raw_data") }}
),
flatten_payload AS (
    SELECT
        message_id,
        STRING(raw_data.EventTime) AS EventTime,
        JSON_VALUE(raw_data, '$.NotificationMetadata.NotificationId') AS NotificationId,
        JSON_VALUE(raw_data, '$.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN') AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, '$.Payload.AnyOfferChangedNotification.Offers') AS offers
    FROM 
        source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() AS surrogatekey,
        message_id,
        NotificationId,
        EventTime, 
        ASIN, 
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY NotificationId DESC) AS OfferId,
        JSON_VALUE(offer, '$.SellerId') AS SellerId,
        JSON_VALUE(offer, '$.IsBuyBoxWinner') AS IsBuyBoxWinner,
        JSON_VALUE(offer, '$.IsFeaturedMerchant') AS IsFeaturedMerchant,
        SAFE_CAST(JSON_VALUE(offer, '$.ListingPrice.Amount') AS FLOAT64) AS ListingPriceAmount,
        JSON_VALUE(offer, '$.ListingPrice.CurrencyCode') AS ListingPriceCurrencyCode,
        JSON_VALUE(offer, '$.PrimeInformation.IsOfferNationalPrime') AS IsOfferNationalPrime,
        JSON_VALUE(offer, '$.PrimeInformation.IsOfferPrime') AS PrimeInformation,
        JSON_VALUE(offer, '$.IsFulfilledByAmazon') AS IsFulfilledByAmazon,
        SAFE_CAST(JSON_VALUE(offer, '$.SellerFeedbackRating.FeedbackCount') AS INT64) AS FeedbackCount,
        SAFE_CAST(JSON_VALUE(offer, '$.SellerFeedbackRating.SellerPositiveFeedbackRating') AS FLOAT64) AS SellerPositiveFeedbackRating
    FROM 
        flatten_payload, UNNEST(offers) AS offer
),
ranked_offers AS (
    SELECT
        OfferId,
        EventTime,
        ASIN,
        message_id,
        NotificationId,
        SellerId,
        IsBuyBoxWinner,
        IsFulfilledByAmazon,
        IsFeaturedMerchant,
        FeedbackCount,
        SellerPositiveFeedbackRating,
        ListingPriceAmount,
        ListingPriceCurrencyCode,
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY 
            IsBuyBoxWinner DESC, 
            IsFulfilledByAmazon DESC, 
            IsFeaturedMerchant ASC, 
            SellerPositiveFeedbackRating DESC,
            ListingPriceAmount ASC) AS ranked
    FROM 
        flatten_offers
)
SELECT 
    ranked,
    OfferId,
    EventTime,
    ASIN,
    message_id,
    NotificationId,
    SellerId,
    IsBuyBoxWinner,
    IsFulfilledByAmazon,
    IsFeaturedMerchant,
    FeedbackCount,
    SellerPositiveFeedbackRating,
    ListingPriceAmount,
    ListingPriceCurrencyCode
FROM 
    ranked_offers 
ORDER BY 
    EventTime ASC,
    IsBuyBoxWinner DESC,
    IsFulfilledByAmazon DESC,
    IsFeaturedMerchant ASC,
    SellerPositiveFeedbackRating DESC,
    ListingPriceAmount ASC
