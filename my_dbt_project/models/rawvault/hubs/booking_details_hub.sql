{{ config(materialized='table') }}
WITH src AS (
  SELECT 'AI_BOOKING_DETAILS' AS record_source, UPPER(TRIM(CAST(BookingID AS STRING))) AS bk_value
  FROM {{ source('ai_staging', 'AI_BOOKING_DETAILS') }}
  UNION ALL
  SELECT 'SJ_BOOKING_DETAILS' AS record_source, UPPER(TRIM(CAST(BookingID AS STRING))) AS bk_value
  FROM {{ source('sj_staging', 'SJ_BOOKING_DETAILS') }}
),
dedup AS (
  SELECT DISTINCT bk_value, record_source FROM src
)
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(bk_value AS STRING))))) AS booking_details_hk,
  bk_value AS bookingid,
  CURRENT_TIMESTAMP() AS load_dts,
  record_source
FROM dedup
