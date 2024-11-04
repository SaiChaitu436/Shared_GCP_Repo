WITH ranked_offers AS (
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
        {{ ref('src_offers') }}
)

SELECT 
    EventTime,
    OfferId,
    ASIN,
    NotificationId,
    SellerId,
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
    OfferId
