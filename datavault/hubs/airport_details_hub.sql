{{ config(materialized='table') }}
WITH src AS (
  SELECT 'AI_AIRPORT_DETAILS' AS record_source, UPPER(TRIM(CAST(AirportCode AS STRING))) AS bk_value
  FROM {{ source('staging', 'AI_AIRPORT_DETAILS') }}
  UNION ALL
  SELECT 'SJ_AIRPORT_DETAILS' AS record_source, UPPER(TRIM(CAST(AirportCode AS STRING))) AS bk_value
  FROM {{ source('staging', 'SJ_AIRPORT_DETAILS') }}
),
dedup AS (
  SELECT DISTINCT bk_value, record_source FROM src
)
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(bk_value AS STRING))))) AS airport_details_hk,
  bk_value AS airportcode,
  CURRENT_TIMESTAMP() AS load_dts,
  record_source
FROM dedup
