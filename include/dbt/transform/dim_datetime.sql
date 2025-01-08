-- Create a CTE to extract date and time components
WITH datetime_cte AS (  
  SELECT DISTINCT
    TransactionDate AS datetime_id,
    CASE
      WHEN LENGTH(TransactionDate) = 10 THEN
        -- Date format: "DD/MM/YYYY"
        PARSE_DATETIME('%m/%d/%Y', TransactionDate)
      WHEN LENGTH(TransactionDate) <= 8 THEN
        -- Date format: "MM/DD/YY"
        PARSE_DATETIME('%m/%d/%y', TransactionDate)
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
  EXTRACT(HOUR FROM date_part) AS hour,
  EXTRACT(MINUTE FROM date_part) AS minute,
  EXTRACT(DAYOFWEEK FROM date_part) AS weekday
FROM datetime_cte