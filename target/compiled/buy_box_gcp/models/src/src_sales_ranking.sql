WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.Summary') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data
    FROM dbtproject-439612.wellbefore.BuyBox_raw_ds_first20_entries as data
),
flatten_payload AS (
    SELECT 
        JSON_VALUE(parse_data.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN) AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, '$.SalesRankings') AS SalesRankings
    FROM source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() as SurrogateKey,
        ASIN,
        JSON_EXTRACT(sr, '$.ProductCategoryId') AS ProductCategoryId,
        JSON_EXTRACT(sr, '$.Rank') AS Ranking
    FROM flatten_payload, UNNEST(SalesRankings) AS sr 
)
SELECT * FROM flatten_offers