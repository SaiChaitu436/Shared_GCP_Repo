WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data,
      data.event_time AS EventTime
    FROM {{source("de-coe","BB_Raw_data")}} as data
),
flatten_payload AS (
    SELECT 
        EventTime,
        JSON_VALUE(parse_data.NotificationMetadata.NotificationId) AS NotificationId,  
        JSON_VALUE(raw_data, '$.OfferChangeTrigger.ASIN') AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, '$.Offers') AS offers
    FROM source_data
),
flatten_offers AS (
    SELECT
        GENERATE_UUID() as SurrogateKey,
        EventTime,
        NotificationId, 
        ASIN,
        JSON_VALUE(offer, '$.SellerId') AS SellerId,
        JSON_VALUE(offer, '$.IsBuyBoxWinner') AS IsBuyBoxWinner,
        JSON_VALUE(offer, '$.ListingPrice.Amount') AS Amount,
        JSON_VALUE(offer, '$.ListingPrice.CurrencyCode') AS CurrencyCode,
        JSON_VALUE(offer, '$.PrimeInformation.IsOfferNationalPrime') AS IsOfferNationalPrime,
        JSON_VALUE(offer, '$.PrimeInformation.IsOfferPrime') AS IsOfferPrime,
        JSON_VALUE(offer, '$.SubCondition') AS SubCondition

    FROM flatten_payload, UNNEST(offers) AS offer 
)
SELECT 
    EventTime,
    ASIN,
    NotificationId,
    SellerId,
    IsBuyBoxWinner,
    SAFE_CAST(Amount AS INT64) AS ListingPriceAmount,
    CurrencyCode as ListingPriceCurrencyCode,
    IsOfferNationalPrime as PrimeInformation_IsOfferNationalPrime,
    IsOfferPrime as PrimeInformation_IsOfferPrime,
    Subcondition
FROM 
    flatten_offers
GROUP BY 
    ASIN,
    NotificationId,
    SellerId,
    ListingPriceAmount,
    ListingPriceCurrencyCode,
    PrimeInformation_IsOfferNationalPrime,
    PrimeInformation_IsOfferPrime,
    Subcondition,
    IsBuyBoxWinner,
    EventTime

