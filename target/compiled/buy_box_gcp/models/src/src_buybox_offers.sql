WITH source_data AS (
    SELECT 
        PARSE_JSON(data.message_body) AS raw_data,
    FROM 
        dbtproject-439612.wellbefore.BuyBox_raw_ds_first20_entries as data
),
flatten_data AS (
    SELECT
        JSON_EXTRACT_ARRAY(raw_data.Payload.AnyOfferChangedNotification.Summary.NumberOfOffers) AS numberOfOffers,
        JSON_EXTRACT_ARRAY(raw_data.Payload.AnyOfferChangedNotification.Summary.NumberOfBuyBoxEligibleOffers) AS buyboxprices
    FROM source_data
),
flatten_data_1 AS (
    SELECT
        JSON_VALUE(bop.Condition)AS EligibleOfferCondition,
        JSON_VALUE(bop.FulfillmentChannel)AS EligibleFulFillmentChannel,
        JSON_VALUE(bop.OfferCount)AS EligibleOfferCount,

        JSON_VALUE(noo.Condition)AS NumberOfferCondition,
        JSON_VALUE(noo.FulfillmentChannel)AS NumberFulFillmentChannel,
        JSON_VALUE(noo.OfferCount)AS NumberOfferCount
    FROM flatten_data,
        UNNEST(buyboxprices) AS bop,
        UNNEST(numberOfOffers) AS noo
)

SELECT * FROM flatten_data_1