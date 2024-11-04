-- WITH source_data AS (
--     SELECT 
--         PARSE_JSON(data.message_body) AS raw_data,
--     FROM 
--         dbt-437212.wellbefore.BuyBox_raw_ds_first20_entries as data
-- ),
-- flatten_data AS (
--     SELECT
--         JSON_VALUE(raw_data.NotificationMetadata.NotificationId) AS NotificationId,
--         JSON_EXTRACT_ARRAY(raw_data.Payload.AnyOfferChangedNotification.Offers) AS offers
--     FROM source_data
-- )
-- SELECT
--     NotificationId,
--     JSON_VALUE(offers.Shipping.Amount) AS ShippingAmount,
--     JSON_VALUE(offers.Shipping.CurrencyCode) AS ShippingCurrencyCode,
--     JSON_VALUE(offers.ShippingTime.AvailabilityType) AS ShippingAvailabilityType,
--     JSON_VALUE(offers.ShippingTime.MaximumHours) AS ShippingMaximumHours,
--     JSON_VALUE(offers.ShippingTime.MinimumHours) AS ShippingMinimumHours,
--     JSON_VALUE(offers.ShipsDomestically) AS ShipsDomestically,
-- FROM
--     flatten_data, UNNEST(offers) AS offers

WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data
    FROM dbtproject-439612.wellbefore.BuyBox_raw_ds_first20_entries as data
),
flatten_payload AS (
    SELECT 
        JSON_VALUE(parse_data.NotificationMetadata.NotificationId) AS NotificationId,
        JSON_EXTRACT_ARRAY(raw_data, '$.Offers') AS offers
    FROM source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() as SurrogateKey,
        NotificationId,
        JSON_EXTRACT(offer, '$.Shipping.Amount') AS ShippingAmount,
        JSON_EXTRACT(offer, '$.Shipping.CurrencyCode') AS ShippingCurrencyCode,
        JSON_EXTRACT(offer, '$.ShippingTime.AvailabilityType') AS AvailabilityType,
        JSON_EXTRACT(offer, '$.ShippingTime.AvailableDate') AS AvailableDate,
        JSON_EXTRACT(offer, '$.ShippingTime.MaximumHours') AS MaximumHours,
        JSON_EXTRACT(offer, '$.ShippingTime.MinimumHours') AS MinimumHours,
        JSON_EXTRACT(offer, '$.ShipsDomestically') AS ShipsDomestically

    FROM flatten_payload, UNNEST(offers) AS offer 
)
SELECT * FROM flatten_offers