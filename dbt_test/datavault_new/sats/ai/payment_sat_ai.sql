{{ config(materialized='table') }}
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(PaymentID AS STRING))))) AS payment_hk,
  TO_HEX(SHA256(COALESCE(CAST(PaymentMethod AS STRING),'') || '|' || COALESCE(CAST(Amount AS STRING),'') || '|' || COALESCE(CAST(TransactionDate AS STRING),'') || '|' || COALESCE(CAST(Status AS STRING),''))) AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'AI_PAYMENT' AS record_source,
  PaymentMethod, Amount, TransactionDate, Status
FROM {{ source('ai_staging', 'AI_PAYMENT') }}
