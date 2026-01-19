{{ config(materialized='table') }}
SELECT
  TO_HEX(SHA256(UPPER(TRIM(CAST(BookingID AS STRING))))) AS booking_details_hk,
  TO_HEX(SHA256(COALESCE(CAST(FlightID AS STRING),'') || '|' || COALESCE(CAST(PassengerID AS STRING),'') || '|' || COALESCE(CAST(Status AS STRING),'') || '|' || COALESCE(CAST(BookingDate AS STRING),'') || '|' || COALESCE(CAST(SeatNumber AS STRING),'') || '|' || COALESCE(CAST(SeatClass AS STRING),'') || '|' || COALESCE(CAST(PaymentID AS STRING),'') || '|' || COALESCE(CAST(CreatedDateTime AS STRING),'') || '|' || COALESCE(CAST(LastupdateDateTime AS STRING),''))) AS hashdiff,
  CURRENT_TIMESTAMP() AS load_dts,
  'AI_BOOKING_DETAILS' AS record_source,
  FlightID, PassengerID, Status, BookingDate, SeatNumber, SeatClass, PaymentID, CreatedDateTime, LastupdateDateTime
FROM {{ source('ai_staging', 'AI_BOOKING_DETAILS') }}
