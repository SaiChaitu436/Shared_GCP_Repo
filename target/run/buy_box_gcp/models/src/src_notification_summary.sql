

  create or replace view `dbtproject-439612`.`wellbefore`.`src_notification_summary`
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
        JSON_VALUE(raw_data.Payload.AnyOfferChangedNotification.Summary.ListPrice.Amount) AS ListPriceAmount,
        JSON_EXTRACT_ARRAY(raw_data.Payload.AnyOfferChangedNotification.Summary.BuyBoxPrices) AS buyboxprice

    FROM source_data
),
flatten_data_1 AS (
    SELECT
        NotificationId,
        JSON_VALUE(bp.LandedPrice.Amount)AS LandedPriceAmount,
        JSON_VALUE(bp.LandedPrice.CurrencyCode)AS LandedPriceCurrencyCode,

        JSON_VALUE(bp.ListingPrice.Amount)AS ListingPriceAmount,
        JSON_VALUE(bp.ListingPrice.CurrencyCode)AS ListingPriceCurrencyCode,
        
        JSON_VALUE(bp.Shipping.Amount)AS ShippingCost,
        JSON_VALUE(bp.Shipping.CurrencyCode)AS ShippingCurrencyCode,

        ListPriceAmount
        
    FROM flatten_data,
        UNNEST(buyboxprice) AS bp
)

SELECT * FROM flatten_data_1;

