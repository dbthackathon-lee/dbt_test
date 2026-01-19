{{ config(materialized='table') }}
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(AirportCode AS STRING))))) AS airport_details_hk,
  TO_HEX(SHA256(COALESCE(CAST(AirportName AS STRING),''))) AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'SJ_AIRPORT_DETAILS' AS record_source,
  AirportName
FROM {{ source('sj_staging', 'SJ_AIRPORT_DETAILS') }}
