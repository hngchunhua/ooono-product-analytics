WITH drives_flatten AS (
SELECT
json_extract_string(raw_payload, '$.session_id') AS session_id
, json_extract_string(raw_payload, '$.user_id') AS user_id
, json_extract_string(raw_payload, '$.platform') AS platform
, json_extract_string(raw_payload, '$.app_version') AS app_version
, json_extract_string(raw_payload, '$.os_version') AS os_version
, json_extract_string(raw_payload, '$.device') AS device
, json_extract_string(raw_payload, '$.country') AS country

, TRY_CAST(json_extract_string(raw_payload, '$.started_at') AS TIMESTAMP) AS started_at
, TRY_CAST(json_extract_string(raw_payload, '$.ended_at') AS TIMESTAMP) AS ended_at
, TRY_CAST(json_extract_string(raw_payload, '$.duration_s') AS INTEGER) AS duration_s
, TRY_CAST(json_extract_string(raw_payload, '$.connection_count') AS INTEGER) AS connection_count
, TRY_CAST(received_at AS TIMESTAMPTZ) AS received_at
, ingested_at

FROM {{ source('bronze', 'drives') }}
)

, drives_deduplicate AS (
SELECT
*
, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY received_at DESC) AS row_num

FROM drives_flatten
)

, drives_timezone AS (
SELECT
*
, CASE
    WHEN country IN ('DE', 'DK', 'ES', 'FR', 'IT') THEN 'Europe/Berlin'
    WHEN country = 'GB' THEN 'Europe/London'
  END AS assumed_timezone

FROM drives_deduplicate
WHERE row_num = 1
)

SELECT
session_id
, user_id
, platform
, app_version
, os_version
, device
, country
, assumed_timezone

, started_at
, ended_at
, CASE
    WHEN assumed_timezone IS NOT NULL THEN timezone('UTC', timezone(assumed_timezone, started_at))
  END AS started_at_utc
, CASE
    WHEN assumed_timezone IS NOT NULL THEN timezone('UTC', timezone(assumed_timezone, ended_at))
  END AS ended_at_utc

, duration_s
, connection_count
, timezone('UTC', received_at) AS received_at_utc
, ingested_at

FROM drives_timezone
WHERE ended_at_utc >= TIMESTAMP '2026-09-28 00:00:00'