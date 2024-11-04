

  create or replace view `dbtproject-439612`.`wellbefore`.`src_offers`
  OPTIONS()
  as WITH source_data AS (
    SELECT 
        PARSE_JSON(data.message_body) AS raw_data
    FROM 
        dbtproject-439612.wellbefore.BuyBox_raw_ds_first20_entries AS data
),
flatten_data AS (
    SELECT
        JSON_VALUE(raw_data, "$.NotificationMetadata.NotificationId") AS NotificationId,
        JSON_VALUE(raw_data, "$.AnyOfferChangedNotification.OfferChangeTrigger.ASIN") AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Offers") AS offer
    FROM source_data
),
flatten_data_1 AS (
    SELECT
        NotificationId,
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY NotificationId DESC) AS OfferID,
        ASIN,
        JSON_VALUE(offer, "$.SellerId") AS SellerId,
        JSON_VALUE(offer, "$.IsBuyBoxWinner") AS IsBuyBoxWinner,
        JSON_VALUE(offer, "$.ListingPrice.Amount") AS ListingPriceAmount,
        JSON_VALUE(offer, "$.ListingPrice.CurrencyCode") AS ListingPriceCurrencyCode,
        JSON_VALUE(offer, "$.PrimeInformation.IsOfferNationalPrime") AS IsOfferNationalPrime,
        JSON_VALUE(offer, "$.PrimeInformation.IsOfferPrime") AS PrimeInformation
    FROM flatten_data,
    UNNEST(offer) AS offer
)

SELECT * 
FROM flatten_data_1;

