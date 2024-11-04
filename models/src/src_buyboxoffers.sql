WITH source_data AS (
    SELECT 
        JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.Summary') AS raw_data,
        PARSE_JSON(data.message_body) AS parse_data
    FROM 
        {{ source("de-coe", "BB_Raw_data") }} AS data
),
flatten_payload AS (
    SELECT 
        JSON_EXTRACT_ARRAY(raw_data, '$.NumberOfBuyBoxEligibleOffers') AS offers,
        JSON_EXTRACT_ARRAY(raw_data, '$.NumberOfOffers') AS offer
    FROM 
        source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() AS SurrogateKey,
        JSON_EXTRACT(offer_element, '$.Condition') AS Condition,
        JSON_EXTRACT(offer_element, '$.FulfillmentChannel') AS FulfillmentChannel,
        SAFE_CAST(JSON_VALUE(offer_element, '$.OfferCount') AS INT64) AS OfferCount,
        JSON_EXTRACT(number_of_offers_element, '$.Condition') AS NumberOfOffers_Condition,
        JSON_EXTRACT(number_of_offers_element, '$.FulfillmentChannel') AS NumberOfOffers_FulfillmentChannel,
        SAFE_CAST(JSON_VALUE(number_of_offers_element, '$.OfferCount') AS INT64) AS NumberOfOffers_OfferCount
    FROM 
        flatten_payload,
        UNNEST(offers) AS offer_element,
        UNNEST(offer) AS number_of_offers_element
)
SELECT  *
FROM 
    flatten_offers
