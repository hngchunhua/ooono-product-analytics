SELECT
json_extract_string(raw_payload, '$.user_id') AS user_id
, TRY_CAST(json_extract_string(raw_payload, '$.created_at')AS TIMESTAMPTZ) AS created_at
, json_extract_string(raw_payload, '$.country') AS country
, json_extract_string(raw_payload, '$.platform') AS platform
, json_extract_string(raw_payload, '$.plan') AS plan
, TRY_CAST(json_extract_string(raw_payload, '$.monthly_price_eur')AS DECIMAL(10,2)) AS monthly_price_eur
, ingested_at

FROM {{ source('bronze', 'users') }}