SELECT
  p.product_id,
  p.product_category,
  SUM(fi.quantity) AS total_quantity_sold
FROM {{ ref('fact_invoices') }} fi
JOIN {{ ref('dim_product') }} p ON fi.product_id = p.product_id
GROUP BY p.product_id, p.product_category
ORDER BY total_quantity_sold DESC
LIMIT 10