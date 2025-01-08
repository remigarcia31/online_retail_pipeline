SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['ProductCategory', 'PricePerUnit']) }} as product_id,
    ProductCategory AS product_category,
    PricePerUnit AS price
FROM {{ source('retail', 'raw_transactions') }}
WHERE ProductCategory IS NOT NULL
AND PricePerUnit > 0