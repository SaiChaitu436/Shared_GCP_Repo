

-- WITH source_data AS (
--     SELECT 
--         PARSE_JSON(message_body) AS raw_data
--     FROM `de-coe`.`buybox_dataset`.`merge_seed_final_table`
-- ),
-- flatten_payload AS (
--     SELECT 
--         raw_data.EventTime AS EventTime,
--         JSON_EXTRACT_ARRAY(raw_data.Payload.AnyOfferChangedNotification, '$.Offers') AS offers
--     FROM source_data
-- )SELECT 
--     GENERATE_UUID() as surrogate_key,
--     ROW_NUMBER() OVER (ORDER BY NULL) AS id,
--     TIMESTAMP(JSON_VALUE(EventTime)) AS EventTime,
--     JSON_VALUE(offer, '$.SellerId') AS SellerId,
--     JSON_VALUE(offer, '$.IsFeaturedMerchant') AS IsFeaturedMerchant,
--     JSON_VALUE(offer, '$.IsFulfilledByAmazon') AS IsFulfilledByAmazon,
--     CURRENT_TIMESTAMP() AS updated_at
-- FROM flatten_payload, UNNEST(offers) AS offer

WITH source_data AS (
    SELECT 
        PARSE_JSON(message_body) AS raw_data
    FROM `de-coe`.`buybox_dataset`.`merge_seed_final_table`
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

SELECT 
    EventTime,
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
    IsFeaturedMerchant,
    IsFulfilledByAmazon
FROM 
    flattened_offers
ORDER BY 
    SellerId