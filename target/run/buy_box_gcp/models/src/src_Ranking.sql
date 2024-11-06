
  
    

    create or replace table `de-coe`.`buybox_dataset`.`src_Ranking`
      
    
    

    OPTIONS()
    as (
      WITH source_data AS (
    SELECT 
        message_id,
        PARSE_JSON(message_body) AS raw_data
    FROM 
        `de-coe`.`buybox_dataset`.`merge_seed_final_table`
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
    CASE 
        WHEN SellerId = 'A3F2UBJ6MNDDM5' THEN 'MedicalSupplyMI'
        WHEN SellerId = 'A13NYAASDR0XYP' THEN 'IRONMED'
        WHEN SellerId = 'A2V74LV9L3ASTD' THEN 'Health & Prime'

        WHEN SellerId = 'A1AKLLB03VCSY5' THEN 'UrthShop'
        WHEN SellerId = 'A32YGV37EPHIKJ' THEN 'Boondocks Medical'
        WHEN SellerId = 'A147ASZ83GESTI' THEN 'Stateside Medical Supply'

        WHEN SellerId = 'A3MT75038F86CX' THEN 'Johnson Distributors'
        WHEN SellerId = 'A1G2IX65IQJHUO' THEN 'EXPRESSMED'
        WHEN SellerId = 'ABOPLAY6RS86X' THEN 'global-wholesale'

        WHEN SellerId = 'A29OWEYSFJVSZC' THEN 'Healing Easier'
        WHEN SellerId = 'AFF8XSNGT0QQC' THEN 'Honest Medical'
        WHEN SellerId = 'A2I0HOF5WGMLJC' THEN 'Social Medical Supply'

        WHEN SellerId = 'APSAI9VUG3A9O' THEN 'Katy Med Solutions'

        ELSE 'Unknown Seller'
    END AS SellerName,
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
    );
  