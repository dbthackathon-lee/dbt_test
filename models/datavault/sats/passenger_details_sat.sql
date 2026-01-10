{{ config(materialized='table') }}
WITH unioned AS (

  SELECT 'AI_PASSENGER_DETAILS' AS record_source,
         TO_HEX(SHA256(UPPER(TRIM(CAST(PassengerID AS STRING))))) AS passenger_details_hk,
         TO_HEX(SHA256(COALESCE(CAST(FirstName AS STRING),'') || '|' || COALESCE(CAST(LastName AS STRING),'') || '|' || COALESCE(CAST(DOB AS STRING),'') || '|' || COALESCE(CAST(Email AS STRING),'') || '|' || COALESCE(CAST(CreatedDateTime AS STRING),'') || '|' || COALESCE(CAST(LastupdateDateTime AS STRING),''))) AS hashdiff,
         CURRENT_TIMESTAMP() AS load_dts,
         FirstName, LastName, DOB, Email, CreatedDateTime, LastupdateDateTime
  FROM {{ source('staging', 'AI_PASSENGER_DETAILS') }}

UNION ALL

  SELECT 'SJ_PASSENGER_DETAILS' AS record_source,
         TO_HEX(SHA256(UPPER(TRIM(CAST(PassengerID AS STRING))))) AS passenger_details_hk,
         TO_HEX(SHA256(COALESCE(CAST(FirstName AS STRING),'') || '|' || COALESCE(CAST(LastName AS STRING),'') || '|' || COALESCE(CAST(DOB AS STRING),'') || '|' || COALESCE(CAST(Email AS STRING),'') || '|' || COALESCE(CAST(MobileNumber AS STRING),'') || '|' || COALESCE(CAST(CreatedDateTime AS STRING),'') || '|' || COALESCE(CAST(LastupdateDateTime AS STRING),''))) AS hashdiff,
         CURRENT_TIMESTAMP() AS load_dts,
         FirstName, LastName, DOB, Email, MobileNumber, CreatedDateTime, LastupdateDateTime
  FROM {{ source('staging', 'SJ_PASSENGER_DETAILS') }}

)
SELECT * FROM unioned
