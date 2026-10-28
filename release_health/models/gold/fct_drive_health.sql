WITH warnings_agg AS (
SELECT
    session_id
    , COUNT(*) AS warning_count

FROM {{ ref('stg_warnings') }}
GROUP BY session_id
)

SELECT
d.session_id
, d.user_id
, d.platform
, d.app_version
, d.os_version
, d.device
, d.country
, d.ended_at_utc
, CAST(d.ended_at_utc AS DATE) AS ended_at_utc_date_only
, DATE_TRUNC('hour', d.ended_at_utc) AS ended_at_utc_datehour_only
, d.received_at_utc
, d.duration_s

, d.connection_count
, CASE
    WHEN d.connection_count >= 1 THEN d.connection_count - 1
  END AS reconnection_count

, (d.connection_count > 1) AS is_interrupted_drive
, (d.connection_count > 2) AS is_repeatedly_interrupted_drive

, COALESCE(w.warning_count, 0) AS warning_count

FROM {{ ref('stg_drives') }} d
LEFT JOIN warnings_agg w ON d.session_id = w.session_id