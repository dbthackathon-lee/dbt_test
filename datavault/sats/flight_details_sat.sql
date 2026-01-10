{{ config(materialized='table') }}
WITH unioned AS (

  SELECT 'AI_FLIGHT_DETAILS' AS record_source,
         TO_HEX(SHA256(UPPER(TRIM(CAST(FlightID AS STRING))))) AS flight_details_hk,
         TO_HEX(SHA256(COALESCE(CAST(FlightNumber AS STRING),'') || '|' || COALESCE(CAST(ArrivalDateTime AS STRING),'') || '|' || COALESCE(CAST(ScheduledDepartureDateTime AS STRING),'') || '|' || COALESCE(CAST(ActualDepartureDateTime AS STRING),'') || '|' || COALESCE(CAST(OriginAirportCode AS STRING),'') || '|' || COALESCE(CAST(DestinationAirportCode AS STRING),'') || '|' || COALESCE(CAST(SeatCapacity AS STRING),'') || '|' || COALESCE(CAST(AvailableSeats AS STRING),''))) AS hashdiff,
         CURRENT_TIMESTAMP() AS load_dts,
         FlightNumber, ArrivalDateTime, ScheduledDepartureDateTime, ActualDepartureDateTime, OriginAirportCode, DestinationAirportCode, SeatCapacity, AvailableSeats
  FROM {{ source('staging', 'AI_FLIGHT_DETAILS') }}

UNION ALL

  SELECT 'SJ_FLIGHT_DETAILS' AS record_source,
         TO_HEX(SHA256(UPPER(TRIM(CAST(FlightID AS STRING))))) AS flight_details_hk,
         TO_HEX(SHA256(COALESCE(CAST(FlightNumber AS STRING),'') || '|' || COALESCE(CAST(ArrivalDateTime AS STRING),'') || '|' || COALESCE(CAST(ScheduledDepartureDateTime AS STRING),'') || '|' || COALESCE(CAST(ActualDepartureDateTime AS STRING),'') || '|' || COALESCE(CAST(OriginAirportCode AS STRING),'') || '|' || COALESCE(CAST(DestinationAirportCode AS STRING),'') || '|' || COALESCE(CAST(SeatCapacity AS STRING),'') || '|' || COALESCE(CAST(AvailableSeats AS STRING),''))) AS hashdiff,
         CURRENT_TIMESTAMP() AS load_dts,
         FlightNumber, ArrivalDateTime, ScheduledDepartureDateTime, ActualDepartureDateTime, OriginAirportCode, DestinationAirportCode, SeatCapacity, AvailableSeats
  FROM {{ source('staging', 'SJ_FLIGHT_DETAILS') }}

)
SELECT * FROM unioned
