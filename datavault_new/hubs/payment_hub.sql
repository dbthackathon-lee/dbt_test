{{ config(materialized='table') }}
WITH src AS (
  SELECT 'AI_PAYMENT' AS record_source, UPPER(TRIM(CAST(PaymentID AS STRING))) AS bk_value
  FROM {{ source('ai_staging', 'AI_PAYMENT') }}
  UNION ALL
  SELECT 'SJ_PAYMENT' AS record_source, UPPER(TRIM(CAST(PaymentID AS STRING))) AS bk_value
  FROM {{ source('sj_staging', 'SJ_PAYMENT') }}
),
dedup AS (
  SELECT DISTINCT bk_value, record_source FROM src
)
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(bk_value AS STRING))))) AS payment_hk,
  bk_value AS paymentid,
  CURRENT_TIMESTAMP() AS load_dts,
  record_source
FROM dedup
