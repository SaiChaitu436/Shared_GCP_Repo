WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data,
      data.event_time AS EventTime
    FROM `de-coe`.`buybox_dataset`.`merge_seed_final_table` as data
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
    CASE 
        WHEN SellerId = 'A3F2UBJ6MNDDM5' THEN 'MedicalSupplyMI'
        WHEN SellerId = 'A13NYAASDR0XYP' THEN 'IRONMED'
        WHEN SellerId = 'A2V74LV9L3ASTD' THEN 'Health & Prime'

        WHEN SellerId = 'A1AKLLB03VCSY5' THEN 'UrthShop'
        WHEN SellerId = 'A32YGV37EPHIKJ' THEN 'Boondocks Medical'
        WHEN SellerId = 'A147ASZ83GESTI' THEN 'Stateside Medical Supply'

        WHEN SellerId = 'A3MT75038F86CX' THEN 'Johnson Distributors'
        WHEN SellerId = 'A1G2IX65IQJHUO' THEN 'EXPRESSMED'
        WHEN SellerId = 'ABOPLAY6RS86X' THEN 'global-wholesale'

        WHEN SellerId = 'A29OWEYSFJVSZC' THEN 'Healing Easier'
        WHEN SellerId = 'AFF8XSNGT0QQC' THEN 'Honest Medical'
        WHEN SellerId = 'A2I0HOF5WGMLJC' THEN 'Social Medical Supply'

        WHEN SellerId = 'APSAI9VUG3A9O' THEN 'Katy Med Solutions'

        ELSE 'Unknown Seller'
    END AS SellerName,
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