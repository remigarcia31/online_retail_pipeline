-- Create the fact table by joining the relevant keys from dimension table
WITH fct_invoices_cte AS (
    SELECT
        TransactionID AS invoice_id,
        TransactionDate AS datetime_id,
        {{ dbt_utils.generate_surrogate_key(['ProductCategory', 'PricePerUnit']) }} as product_id,
        {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} as customer_id,
        Quantity AS quantity,
        TotalAmount AS total
    FROM {{ source('retail', 'raw_transactions') }}
    WHERE Quantity > 0
)
SELECT
    invoice_id,
    dt.datetime_id,
    dp.product_id,
    dc.customer_id,
    quantity,
    total
FROM fct_invoices_cte fi
INNER JOIN {{ ref('dim_datetime') }} dt ON fi.datetime_id = dt.datetime_id
INNER JOIN {{ ref('dim_product') }} dp ON fi.product_id = dp.product_id
INNER JOIN {{ ref('dim_customers') }} dc ON fi.customer_id = dc.customer_id