

  create or replace view `dbtproject-439612`.`wellbefore`.`src_sellers`
  OPTIONS()
  as -- WITH source_data AS (
--     SELECT 
--         PARSE_JSON(data.message_body) AS raw_data,
--     FROM 
--         dbt-437212.wellbefore.BuyBox_raw_ds_first20_entries as data
-- ),
-- flatten_data AS (
--     SELECT
--         JSON_VALUE(raw_data.EventTime) AS EventTime,
--         JSON_EXTRACT_ARRAY(raw_data.Payload.AnyOfferChangedNotification.Offers) AS offers
--     FROM source_data
-- )
-- SELECT
--     EventTime,
--     JSON_VALUE(offers.SellerId) AS SellerId,
--     JSON_VALUE(offers.IsFeaturedMerchant) AS IsFeaturedMerchant,
--     JSON_VALUE(offers.SellerId) AS IsFulfilledByAmazon
-- FROM
--     flatten_data, UNNEST(offers) AS offers

WITH source_data AS (
    SELECT 
        PARSE_JSON(data.message_body) AS raw_data
    FROM 
        dbtproject-439612.wellbefore.BuyBox_raw_ds_first20_entries AS data
),
flatten_data AS (
    SELECT
        JSON_VALUE(raw_data, "$.EventTime") AS EventTime,
        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Offers") AS offers
    FROM source_data
),
flattened_offers AS (
    SELECT 
        EventTime,
        JSON_VALUE(offer, "$.SellerId") AS SellerId,
        JSON_VALUE(offer, "$.IsFeaturedMerchant") AS IsFeaturedMerchant,
        JSON_VALUE(offer, "$.IsFulfilledByAmazon") AS IsFulfilledByAmazon
    FROM 
        flatten_data,
        UNNEST(offers) AS offer
)
SELECT * 
FROM flattened_offers;

