{{ config(materialized='table') }}
WITH unioned AS (

  SELECT 'AI_PAYMENT' AS record_source,
         TO_HEX(SHA256(UPPER(TRIM(CAST(PaymentID AS STRING))))) AS payment_hk,
         TO_HEX(SHA256(COALESCE(CAST(PaymentMethod AS STRING),'') || '|' || COALESCE(CAST(Amount AS STRING),'') || '|' || COALESCE(CAST(TransactionDate AS STRING),'') || '|' || COALESCE(CAST(Status AS STRING),''))) AS hashdiff,
         CURRENT_TIMESTAMP() AS load_dts,
         PaymentMethod, Amount, TransactionDate, Status
  FROM {{ source('staging', 'AI_PAYMENT') }}

UNION ALL

  SELECT 'SJ_PAYMENT' AS record_source,
         TO_HEX(SHA256(UPPER(TRIM(CAST(PaymentID AS STRING))))) AS payment_hk,
         TO_HEX(SHA256(COALESCE(CAST(PaymentMethod AS STRING),'') || '|' || COALESCE(CAST(Amount AS STRING),'') || '|' || COALESCE(CAST(TransactionDate AS STRING),'') || '|' || COALESCE(CAST(Status AS STRING),''))) AS hashdiff,
         CURRENT_TIMESTAMP() AS load_dts,
         PaymentMethod, Amount, TransactionDate, Status
  FROM {{ source('staging', 'SJ_PAYMENT') }}

)
SELECT * FROM unioned
