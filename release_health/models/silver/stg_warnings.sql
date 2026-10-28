WITH warnings_flatten AS (
SELECT
json_extract_string(raw_payload, '$.warning_id') AS warning_id
, json_extract_string(raw_payload, '$.session_id') AS session_id
, json_extract_string(raw_payload, '$.user_id') AS user_id
, json_extract_string(raw_payload, '$.pin_type') AS pin_type
, json_extract_string(raw_payload, '$.user_action') AS user_action

, TRY_CAST(json_extract_string(raw_payload, '$.device_time') AS TIMESTAMP) AS device_time
, TRY_CAST(received_at AS TIMESTAMPTZ) AS received_at
, ingested_at

FROM {{ source('bronze', 'warnings') }}
)

SELECT
warning_id
, session_id
, user_id
, pin_type
, user_action
, device_time

, CASE
    WHEN device_time = TIMESTAMP '2038-01-19 03:14:07'
    THEN TRUE
    ELSE FALSE
  END AS is_invalid_device_time

, received_at
, ingested_at

FROM warnings_flatten