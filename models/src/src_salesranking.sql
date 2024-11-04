WITH source_data AS (
    SELECT 
        JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.Summary') AS raw_data,
        PARSE_JSON(data.message_body) AS parse_data
    FROM 
        {{ source("de-coe", "BB_Raw_data") }} AS data
),
flatten_payload AS (
    SELECT 
        JSON_VALUE(parse_data.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN) AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, '$.SalesRankings') AS SalesRankings
    FROM 
        source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() AS SurrogateKey,
        ASIN,
        JSON_VALUE(offer, '$.ProductCategoryId') AS ProductCategoryId,
        SAFE_CAST(JSON_VALUE(offer, '$.Rank') AS INT64) AS Rank
    FROM 
        flatten_payload, UNNEST(SalesRankings) AS offer 
)
SELECT * 
FROM flatten_offers
