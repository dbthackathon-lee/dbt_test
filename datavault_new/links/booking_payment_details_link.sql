{{ config(materialized='table') }}
WITH unioned AS (

  SELECT 'AI_BOOKING_DETAILS' AS record_source,
         TO_HEX(SHA256(COALESCE(CAST(UPPER(TRIM(CAST(BookingID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(PaymentID AS STRING))) AS STRING),''))) AS booking_payment_details_lk_hk,
         TO_HEX(SHA256(UPPER(TRIM(CAST(BookingID AS STRING))))) AS bookingid_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(PaymentID AS STRING))))) AS paymentid_hk
  FROM {{ source('ai_staging', 'AI_BOOKING_DETAILS') }}

UNION ALL

  SELECT 'SJ_BOOKING_DETAILS' AS record_source,
         TO_HEX(SHA256(COALESCE(CAST(UPPER(TRIM(CAST(BookingID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(PaymentID AS STRING))) AS STRING),''))) AS booking_payment_details_lk_hk,
         TO_HEX(SHA256(UPPER(TRIM(CAST(BookingID AS STRING))))) AS bookingid_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(PaymentID AS STRING))))) AS paymentid_hk
  FROM {{ source('sj_staging', 'SJ_BOOKING_DETAILS') }}

),
dedup AS (
  SELECT DISTINCT booking_payment_details_lk_hk, bookingid_hk, paymentid_hk, record_source
  FROM unioned
)
SELECT
  booking_payment_details_lk_hk,
  bookingid_hk, paymentid_hk,
  CURRENT_TIMESTAMP() AS load_dts,
  record_source
FROM dedup
