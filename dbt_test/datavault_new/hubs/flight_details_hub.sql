{{ config(materialized='table') }}
WITH src AS (
  SELECT 'AI_FLIGHT_DETAILS' AS record_source, UPPER(TRIM(CAST(FlightID AS STRING))) AS bk_value
  FROM {{ source('ai_staging', 'AI_FLIGHT_DETAILS') }}
  UNION ALL
  SELECT 'SJ_FLIGHT_DETAILS' AS record_source, UPPER(TRIM(CAST(FlightID AS STRING))) AS bk_value
  FROM {{ source('sj_staging', 'SJ_FLIGHT_DETAILS') }}
),
dedup AS (
  SELECT DISTINCT bk_value, record_source FROM src
)
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(bk_value AS STRING))))) AS flight_details_hk,
  bk_value AS flightid,
  CURRENT_TIMESTAMP() AS load_dts,
  record_source
FROM dedup
