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
        JSON_EXTRACT_ARRAY(raw_data, '$.BuyBoxPrices') AS offers,
        SAFE_CAST(JSON_VALUE(raw_data, '$.ListPrice.Amount') AS FLOAT64) AS ListPrice
    FROM 
        source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() AS SurrogateKey,
        NotificationId,
        SAFE_CAST(JSON_VALUE(offer, '$.LandedPrice.Amount') AS FLOAT64) AS BB_LandingPrice,
        SAFE_CAST(JSON_VALUE(offer, '$.ListingPrice.Amount') AS FLOAT64) AS BB_ListingPrice,
        SAFE_CAST(JSON_VALUE(offer, '$.Shipping.Amount') AS FLOAT64) AS BB_LShippingPrice,
        JSON_VALUE(offer, '$.Shipping.CurrencyCode') AS BB_ShippingCurrencyCode,
        ListPrice
    FROM 
        flatten_payload, 
        UNNEST(offers) AS offer
)
SELECT * 
FROM flatten_offers
