WITH source_data AS (
    SELECT
        PARSE_JSON(data.message_body) AS raw_data,
        PARSE_JSON(data.message_body).Payload AS offer
    FROM dbtproject-439612.wellbefore.BuyBox_raw_ds_first20_entries as data
),
flatten_data AS (
    SELECT
        JSON_VALUE(raw_data.NotificationMetadata.NotificationId) AS NotificationId,
        JSON_VALUE(raw_data.NotificationMetadata.PublishTime) AS PublishTime,
        JSON_VALUE(raw_data.NotificationMetadata.SubscriptionId) AS SubscriptionId,
        JSON_VALUE(raw_data.NotificationMetadata.ApplicationId) AS ApplicationId,
        JSON_VALUE(raw_data.NotificationType) AS NotificationType,
        JSON_VALUE(raw_data.NotificationVersion) AS NotificationVersion,
        JSON_VALUE(offer.AnyOfferChangedNotification.OfferChangeTrigger.ASIN)AS ASIN
    FROM source_data
)
 
SELECT * FROM flatten_data