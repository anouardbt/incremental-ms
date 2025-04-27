{{
  config(
    materialized='incremental',
    unique_key='deal_id',
    incremental_strategy='delete+insert',
    partition_by={
      "field": "timemsc",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH source_data AS (
  SELECT
    *,
    'p2' as server_source
  FROM {{ source('raw_data', 'p2_deals') }}
  {{ server_incremental_filter('timemsc', 'p2', var('backfill', false)) }}
)

SELECT
  deal_id,
  timemsc,
  server_source,
  -- add all other fields
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as _loaded_at
FROM source_data 