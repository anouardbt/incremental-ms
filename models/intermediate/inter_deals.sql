{{
  config(
    materialized='incremental',
    unique_key='deal_id',
    incremental_strategy='merge',
    partition_by={
      "field": "timemsc",
      "data_type": "datetime",
      "granularity": "day"
    },
    cluster_by=["server_source"]
  )
}}

WITH combined_data AS (
  -- p1 deals
  SELECT * FROM {{ ref('stg_p1_deals') }}
  {{ selective_server_filter('timemsc', 'p1') }}
  
  UNION ALL
  
  -- p2 deals
  SELECT * FROM {{ ref('stg_p2_deals') }}
  {{ selective_server_filter('timemsc', 'p2') }}
  
  UNION ALL
  
  -- p3 deals
  SELECT * FROM {{ ref('stg_p3_deals') }}
  {{ selective_server_filter('timemsc', 'p3') }}
)

SELECT
  deal_id,
  timemsc,
  server_source,
  created_at,
  updated_at,
  _loaded_at
FROM combined_data 