{{ config(materialized='table') }}
WITH src AS (
  SELECT 'AI_PASSENGER_DETAILS' AS record_source, UPPER(TRIM(CAST(PassengerID AS STRING))) AS bk_value
  FROM {{ source('staging', 'AI_PASSENGER_DETAILS') }}
  UNION ALL
  SELECT 'SJ_PASSENGER_DETAILS' AS record_source, UPPER(TRIM(CAST(PassengerID AS STRING))) AS bk_value
  FROM {{ source('staging', 'SJ_PASSENGER_DETAILS') }}
),
dedup AS (
  SELECT DISTINCT bk_value, record_source FROM src
)
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(bk_value AS STRING))))) AS passenger_details_hk,
  bk_value AS passengerid,
  CURRENT_TIMESTAMP() AS load_dts,
  record_source
FROM dedup
