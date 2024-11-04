WITH source_data AS (
    SELECT 
        JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.Summary') AS raw_data,
        PARSE_JSON(data.message_body) AS parse_data
    FROM 
        {{ source("de-coe", "BB_Raw_data") }} AS data
),
flatten_payload AS (
    SELECT
        JSON_VALUE(parse_data, '$.NotificationMetadata.NotificationId') AS NotificationId,
        JSON_EXTRACT_ARRAY(raw_data, '$.LowestPrices') AS offers
    FROM 
        source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() AS SurrogateKey,
        NotificationId,
        JSON_VALUE(offer_element, '$.Condition') AS Condition,
        JSON_VALUE(offer_element, '$.FulfillmentChannel') AS FulfillmentChannel,
        SAFE_CAST(JSON_VALUE(offer_element, '$.LandedPrice.Amount') AS FLOAT64) AS Amount,
        JSON_VALUE(offer_element, '$.LandedPrice.CurrencyCode') AS CurrencyCode,
        SAFE_CAST(JSON_VALUE(offer_element, '$.ListingPrice.Amount') AS FLOAT64) AS Listing_Amount,
        SAFE_CAST(JSON_VALUE(offer_element, '$.Shipping.Amount') AS FLOAT64) AS Shipping_Amount
    FROM 
        flatten_payload, 
        UNNEST(offers) AS offer_element
)
SELECT * 
FROM flatten_offers
