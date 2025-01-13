-- Create a CTE to extract date and time components
WITH datetime_cte AS (  
  SELECT DISTINCT
    TransactionDate AS datetime_id,
    CASE
      WHEN LENGTH(TransactionDate) = 10 THEN
        -- Date format: "YYYY/MM/DD"
        PARSE_DATETIME('%Y-%m-%d', TransactionDate)
      WHEN LENGTH(TransactionDate) <= 8 THEN
        -- Date format: "YY/MM/DD"
        PARSE_DATETIME('%Y-%m-%d', TransactionDate)
      ELSE
        NULL
    END AS date_part,
  FROM {{ source('retail', 'raw_transactions') }}
  WHERE TransactionDate IS NOT NULL
)
SELECT
  datetime_id,
  date_part as datetime,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  EXTRACT(DAY FROM date_part) AS day,
  EXTRACT(DAYOFWEEK FROM date_part) AS weekday
FROM datetime_cte