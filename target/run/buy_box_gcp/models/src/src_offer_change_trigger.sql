

  create or replace view `dbtproject-439612`.`wellbefore`.`src_offer_change_trigger`
  OPTIONS()
  as WITH source_data AS (
    SELECT
        PARSE_JSON(data.message_body) AS raw_data,
        PARSE_JSON(data.message_body).Payload AS offer
    FROM dbtproject-439612.wellbefore.BuyBox_raw_ds_first20_entries as data
),
flatten_data AS (
    SELECT
        JSON_VALUE(offer.AnyOfferChangedNotification.OfferChangeTrigger.ASIN)AS ASIN,
        JSON_VALUE(offer.AnyOfferChangedNotification.OfferChangeTrigger.ItemCondition)AS ItemCondition,
        JSON_VALUE(raw_data.NotificationMetadata.PublishTime) AS PublishTime,
    FROM source_data
)
 
SELECT * FROM flatten_data;

