WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.OfferChangeTrigger') AS raw_data
    FROM {{source("de-coe","BB_Raw_data")}} as data
),
flatten_payload AS (
    SELECT  
        GENERATE_UUID() as SurrogateKey, 
        JSON_VALUE(raw_data, '$.ASIN') AS ASIN,
        JSON_VALUE(raw_data, '$.MarketplaceId') AS MarketplaceId
    FROM source_data
)
SELECT * FROM flatten_payload

