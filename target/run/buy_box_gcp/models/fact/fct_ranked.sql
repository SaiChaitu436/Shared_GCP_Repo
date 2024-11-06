

  create or replace view `de-coe`.`buybox_dataset`.`fct_ranked`
  OPTIONS()
  as WITH ranked_offers AS (
    SELECT 
        EventTime,
        ASIN,
        NotificationId,
        SellerId,
        IsBuyBoxWinner,
        ListingPriceAmount,
        ListingPriceCurrencyCode,
        PrimeInformation_IsOfferNationalPrime,
        PrimeInformation_IsOfferPrime,
        Subcondition,
        ROW_NUMBER() OVER (
            PARTITION BY ASIN 
            ORDER BY 
                IsBuyBoxWinner DESC, 
                ListingPriceAmount ASC,
                PrimeInformation_IsOfferNationalPrime DESC,
                PrimeInformation_IsOfferPrime DESC,
                NotificationId DESC    
        ) AS OfferId
    FROM 
        `de-coe`.`buybox_dataset`.`src_offers`
)

SELECT 
    EventTime,
    OfferId,
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
    ListingPriceAmount,
    ListingPriceCurrencyCode,
    PrimeInformation_IsOfferNationalPrime,
    PrimeInformation_IsOfferPrime,
    Subcondition
FROM 
    ranked_offers
ORDER BY 
    ASIN,    
    OfferId;

