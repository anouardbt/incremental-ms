-- Test to verify all servers have exactly the same max timestamp
-- This test fails if any server's max timestamp is different from the others

WITH server_max_times AS (
  SELECT 
    server_source,
    MAX(timemsc) as max_time
  FROM {{ ref('inter_deals') }}
  GROUP BY server_source
),

distinct_times AS (
  SELECT DISTINCT max_time
  FROM server_max_times
)

-- If this returns any rows, it means servers are out of sync
SELECT
  'Servers are out of sync. Found ' || COUNT(*) || ' different max timestamps.' as failure_reason
FROM distinct_times
HAVING COUNT(*) > 1 