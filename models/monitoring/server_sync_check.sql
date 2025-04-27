{{
  config(
    materialized='table',
    schema='monitoring'
  )
}}

WITH server_max_times AS (
  SELECT 
    server_source,
    MAX(timemsc) as max_time
  FROM {{ ref('inter_deals') }}
  GROUP BY server_source
),

distinct_times_count AS (
  SELECT COUNT(DISTINCT max_time) as distinct_count
  FROM server_max_times
)

SELECT 
  CASE 
    WHEN distinct_count = 1 THEN 'IN SYNC'
    ELSE 'OUT OF SYNC'
  END as sync_status,
  distinct_count as distinct_timestamps_count,
  CURRENT_TIMESTAMP() as checked_at
FROM distinct_times_count 