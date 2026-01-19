{{ config(materialized='table') }}
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(FlightID AS STRING))))) AS flight_details_hk,
  TO_HEX(SHA256(COALESCE(CAST(FlightNumber AS STRING),'') || '|' || COALESCE(CAST(ArrivalDateTime AS STRING),'') || '|' || COALESCE(CAST(ScheduledDepartureDateTime AS STRING),'') || '|' || COALESCE(CAST(ActualDepartureDateTime AS STRING),'') || '|' || COALESCE(CAST(OriginAirportCode AS STRING),'') || '|' || COALESCE(CAST(DestinationAirportCode AS STRING),'') || '|' || COALESCE(CAST(SeatCapacity AS STRING),'') || '|' || COALESCE(CAST(AvailableSeats AS STRING),''))) AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'SJ_FLIGHT_DETAILS' AS record_source,
  FlightNumber, ArrivalDateTime, ScheduledDepartureDateTime, ActualDepartureDateTime, OriginAirportCode, DestinationAirportCode, SeatCapacity, AvailableSeats
FROM {{ source('sj_staging', 'SJ_FLIGHT_DETAILS') }}
