{{ 
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        full_refresh = true,
        merge_update_columns=['IsFeaturedMerchant', 'IsFulfilledByAmazon']
    ) 
}}

WITH source_data AS (
    SELECT 
        PARSE_JSON(message_body) AS raw_data
    FROM {{source("de-coe","BB_Raw_data")}}
),
flatten_payload AS (
    SELECT 
        raw_data.EventTime AS EventTime,
        JSON_EXTRACT_ARRAY(raw_data.Payload.AnyOfferChangedNotification, '$.Offers') AS offers
    FROM source_data
)SELECT 
    GENERATE_UUID() as surrogate_key,
    ROW_NUMBER() OVER (ORDER BY NULL) AS id,
    TIMESTAMP(JSON_VALUE(EventTime)) AS EventTime,
    JSON_VALUE(offer, '$.SellerId') AS SellerId,
    JSON_VALUE(offer, '$.IsFeaturedMerchant') AS IsFeaturedMerchant,
    JSON_VALUE(offer, '$.IsFulfilledByAmazon') AS IsFulfilledByAmazon,
    CURRENT_TIMESTAMP() AS updated_at
FROM flatten_payload, UNNEST(offers) AS offer