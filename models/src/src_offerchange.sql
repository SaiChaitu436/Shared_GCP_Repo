WITH source_data AS (
    SELECT 
        PARSE_JSON(data.message_body) AS raw_data,
        PARSE_JSON(data.message_body).Payload.AnyOfferChangedNotification.OfferChangeTrigger AS offer
    FROM 
        {{ source("de-coe", "BB_Raw_data") }} AS data
),
flatten_payload AS (
    SELECT 
        GENERATE_UUID() AS SurrogateKey,
        SAFE_CAST(JSON_VALUE(raw_data.NotificationMetadata.PublishTime) AS TIMESTAMP) AS PublishTime,
        JSON_VALUE(offer.ASIN) AS ASIN,
        JSON_VALUE(offer.ItemCondition) AS ItemCondition
    FROM 
        source_data
)
SELECT * 
FROM flatten_payload
