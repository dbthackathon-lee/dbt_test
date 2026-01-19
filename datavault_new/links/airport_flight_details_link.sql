{{ config(materialized='table') }}
WITH unioned AS (

  SELECT 'AI_FLIGHT_DETAILS' AS record_source,
         TO_HEX(SHA256(COALESCE(CAST(UPPER(TRIM(CAST(FlightID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(OriginAirportCode AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(DestinationAirportCode AS STRING))) AS STRING),''))) AS airport_flight_details_lk_hk,
         TO_HEX(SHA256(UPPER(TRIM(CAST(FlightID AS STRING))))) AS flightid_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(OriginAirportCode AS STRING))))) AS originairportcode_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(DestinationAirportCode AS STRING))))) AS destinationairportcode_hk
  FROM {{ source('ai_staging', 'AI_FLIGHT_DETAILS') }}

UNION ALL

  SELECT 'SJ_FLIGHT_DETAILS' AS record_source,
         TO_HEX(SHA256(COALESCE(CAST(UPPER(TRIM(CAST(FlightID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(OriginAirportCode AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(DestinationAirportCode AS STRING))) AS STRING),''))) AS airport_flight_details_lk_hk,
         TO_HEX(SHA256(UPPER(TRIM(CAST(FlightID AS STRING))))) AS flightid_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(OriginAirportCode AS STRING))))) AS originairportcode_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(DestinationAirportCode AS STRING))))) AS destinationairportcode_hk
  FROM {{ source('sj_staging', 'SJ_FLIGHT_DETAILS') }}

),
dedup AS (
  SELECT DISTINCT airport_flight_details_lk_hk, flightid_hk, originairportcode_hk, destinationairportcode_hk, record_source
  FROM unioned
)
SELECT
  airport_flight_details_lk_hk,
  flightid_hk, originairportcode_hk, destinationairportcode_hk,
  CURRENT_TIMESTAMP() AS load_dts,
  record_source
FROM dedup
