{{ config(materialized='table') }}
WITH unioned AS (

  SELECT 'AI_AIRPORT_DETAILS' AS record_source,
         TO_HEX(SHA256(UPPER(TRIM(CAST(AirportCode AS STRING))))) AS airport_details_hk,
         TO_HEX(SHA256(COALESCE(CAST(AirportName AS STRING),''))) AS hashdiff,
         CURRENT_TIMESTAMP() AS load_dts,
         AirportName
  FROM {{ source('staging', 'AI_AIRPORT_DETAILS') }}

UNION ALL

  SELECT 'SJ_AIRPORT_DETAILS' AS record_source,
         TO_HEX(SHA256(UPPER(TRIM(CAST(AirportCode AS STRING))))) AS airport_details_hk,
         TO_HEX(SHA256(COALESCE(CAST(AirportName AS STRING),''))) AS hashdiff,
         CURRENT_TIMESTAMP() AS load_dts,
         AirportName
  FROM {{ source('staging', 'SJ_AIRPORT_DETAILS') }}

)
SELECT * FROM unioned
