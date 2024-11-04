

  create or replace view `dbtproject-439612`.`wellbefore`.`src_notification_lowest_prices`
  OPTIONS()
  as WITH source_data AS (
    SELECT 
        PARSE_JSON(data.message_body) AS raw_data,
    FROM 
        dbtproject-439612.wellbefore.BuyBox_raw_ds_first20_entries as data
),
flatten_data AS (
    SELECT
        JSON_VALUE(raw_data.NotificationMetadata.NotificationId) AS NotificationId,
        JSON_EXTRACT_ARRAY(raw_data.Payload.AnyOfferChangedNotification.Summary.LowestPrices) AS lowestPrices
    FROM source_data
),
flatten_data_1 AS (
    SELECT
        NotificationId,
        JSON_VALUE(lp.Condition)AS Condition,
        JSON_VALUE(lp.FulfillmentChannel)AS FulFillmentChannel,

        JSON_VALUE(lp.LandedPrice.Amount)AS LandedPriceAmount,
        JSON_VALUE(lp.LandedPrice.CurrencyCode)AS LandedPriceCurrencyCode,

        JSON_VALUE(lp.ListingPrice.Amount)AS ListingPriceAmount,
        JSON_VALUE(lp.ListingPrice.CurrencyCode)AS ListingPriceCurrencyCode,

        JSON_VALUE(lp.Shipping.Amount)AS ShippingCost,
    FROM flatten_data,
        UNNEST(lowestPrices) AS lp
)

SELECT * FROM flatten_data_1;

