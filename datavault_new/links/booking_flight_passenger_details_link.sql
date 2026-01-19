{{ config(materialized='table') }}
WITH unioned AS (

  SELECT 'AI_BOOKING_DETAILS' AS record_source,
         TO_HEX(SHA256(COALESCE(CAST(UPPER(TRIM(CAST(BookingID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(FlightID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(PassengerID AS STRING))) AS STRING),''))) AS booking_flight_passenger_details_lk_hk,
         TO_HEX(SHA256(UPPER(TRIM(CAST(BookingID AS STRING))))) AS bookingid_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(FlightID AS STRING))))) AS flightid_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(PassengerID AS STRING))))) AS passengerid_hk
  FROM {{ source('ai_staging', 'AI_BOOKING_DETAILS') }}

UNION ALL

  SELECT 'SJ_BOOKING_DETAILS' AS record_source,
         TO_HEX(SHA256(COALESCE(CAST(UPPER(TRIM(CAST(BookingID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(FlightID AS STRING))) AS STRING),'') || '|' || COALESCE(CAST(UPPER(TRIM(CAST(PassengerID AS STRING))) AS STRING),''))) AS booking_flight_passenger_details_lk_hk,
         TO_HEX(SHA256(UPPER(TRIM(CAST(BookingID AS STRING))))) AS bookingid_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(FlightID AS STRING))))) AS flightid_hk, TO_HEX(SHA256(UPPER(TRIM(CAST(PassengerID AS STRING))))) AS passengerid_hk
  FROM {{ source('sj_staging', 'SJ_BOOKING_DETAILS') }}

),
dedup AS (
  SELECT DISTINCT booking_flight_passenger_details_lk_hk, bookingid_hk, flightid_hk, passengerid_hk, record_source
  FROM unioned
)
SELECT
  booking_flight_passenger_details_lk_hk,
  bookingid_hk, flightid_hk, passengerid_hk,
  CURRENT_TIMESTAMP() AS load_dts,
  record_source
FROM dedup
