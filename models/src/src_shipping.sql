WITH source_data AS (
    SELECT 
        JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification') AS raw_data,
        PARSE_JSON(data.message_body) AS parse_data
    FROM {{ source("de-coe", "BB_Raw_data") }} AS data
),
flatten_payload AS (
    SELECT 
        JSON_VALUE(parse_data.NotificationMetadata.NotificationId) AS NotificationId,
        JSON_EXTRACT_ARRAY(raw_data, '$.Offers') AS offers
    FROM source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() AS SurrogateKey,
        NotificationId,
        SAFE_CAST(JSON_VALUE(offer, '$.Shipping.Amount') AS FLOAT64) AS ShippingAmount,
        JSON_VALUE(offer, '$.Shipping.CurrencyCode') AS ShippingCurrencyCode,
        JSON_VALUE(offer, '$.ShippingTime.AvailabilityType') AS AvailabilityType,
        SAFE_CAST(JSON_VALUE(offer, '$.ShippingTime.AvailableDate') AS DATE) AS AvailableDate,
        SAFE_CAST(JSON_VALUE(offer, '$.ShippingTime.MaximumHours') AS INT64) AS MaximumHours,
        SAFE_CAST(JSON_VALUE(offer, '$.ShippingTime.MinimumHours') AS INT64) AS MinimumHours,
        JSON_VALUE(offer, '$.ShipsDomestically') AS ShipsDomestically
    FROM flatten_payload, UNNEST(offers) AS offer 
)
SELECT * 
FROM flatten_offers
