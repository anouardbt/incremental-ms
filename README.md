# dbt Incremental Loading Strategy for Multi-Server Data

## Overview

This project demonstrates a simple approach to incrementally load data from multiple servers (p1, p2, p3) into a unified table, while handling servers that temporarily stop producing data.

## Architecture

```
p1_deals ──→ stg_p1_deals ───┐
p2_deals ──→ stg_p2_deals ───┼──→ inter_deals
p3_deals ──→ stg_p3_deals ───┘
```

## Key Features

1. **Incremental Loading**: Only processes new data since last run
2. **Server-Aware Incremental Logic**: Tracks each server's data independently
3. **Dormant Server Handling**: If a server stops producing data, other servers continue to update
4. **Selective Backfill Support**: Can backfill data for a specific server
5. **BigQuery Optimizations**: Partitioning and clustering for performance

## Macros

This project includes two key macros in `macros/server_deal_helpers.sql`:

### server_incremental_filter

Used in staging models for incremental filtering:

```sql
{% macro server_incremental_filter(column_name, server_id, is_backfill=false) %}
  {% if is_incremental() and not is_backfill %}
    WHERE {{ column_name }} > (
      SELECT MAX({{ column_name }})
      FROM {{ this }}
      WHERE server_source = '{{ server_id }}'
    )
  {% elif is_backfill %}
    WHERE {{ column_name }} >= '{{ var('start_date') }}'
      AND {{ column_name }} < '{{ var('end_date') }}'
  {% endif %}
{% endmacro %}
```

### selective_server_filter

Used for server-specific backfills in the intermediate model:

```sql
{% macro selective_server_filter(column_name, server_id) %}
  {% set backfill_server = var('backfill_server', none) %}
  
  {% if is_incremental() %}
    {% if backfill_server == server_id and var('backfill', false) %}
      -- Apply backfill filter only to the specified server
      WHERE {{ column_name }} >= '{{ var('start_date') }}'
        AND {{ column_name }} < '{{ var('end_date') }}'
    {% else %}
      -- Apply regular incremental filter for this server
      WHERE {{ column_name }} > (
        SELECT MAX({{ column_name }}) 
        FROM {{ this }} 
        WHERE server_source = '{{ server_id }}'
      )
    {% endif %}
  {% endif %}
{% endmacro %}
```

## Implementation Approaches

### Approach 1: Inline Logic (inter_deals.sql)

This approach implements the selective backfill logic directly in the model:

```sql
-- p1 deals excerpt
SELECT * FROM {{ ref('stg_p1_deals') }}
{% if is_incremental() %}
  {% if backfill_server == 'p1' and var('backfill', false) %}
    WHERE timemsc >= '{{ var('start_date') }}'
      AND timemsc < '{{ var('end_date') }}'
  {% else %}
    WHERE timemsc > (
      SELECT MAX(timemsc) 
      FROM {{ this }} 
      WHERE server_source = 'p1'
    )
  {% endif %}
{% endif %}
```

### Approach 2: Macro-based (inter_deals_m2.sql)

This approach centralizes the logic in a macro for cleaner model code:

```sql
-- p1 deals excerpt
SELECT * FROM {{ ref('stg_p1_deals') }}
{{ selective_server_filter('timemsc', 'p1') }}
```

Both approaches achieve the same result, but Approach 2 makes the model code cleaner and the logic more reusable.

## Example Commands

Here are some common commands for different scenarios:

### Normal Incremental Run

Run an incremental update for all servers:

```bash
dbt run --models stg_*_deals inter_deals
```

### Server-Specific Backfill

Backfill data for only server p1:

```bash
dbt run --models stg_p1_deals inter_deals --vars '{"backfill": true, "backfill_server": "p1", "start_date": "2023-01-01", "end_date": "2023-01-31"}'
```

### Full Backfill for All Servers

Backfill data for all servers:

```bash
dbt run --models stg_*_deals inter_deals --vars '{"backfill": true, "start_date": "2023-01-01", "end_date": "2023-01-31"}'
```

### Running for a Subset of Servers

Process only specific servers:

```bash
dbt run --models stg_p1_deals stg_p2_deals inter_deals
```

## What Happens During a Server-Specific Backfill

When running the server-specific backfill command, here's exactly what happens for each server:

**For p1:**
1. The `stg_p1_deals` model runs in backfill mode
   - It reloads all p1 data between Jan 1-31, 2023
   - Uses delete+insert strategy, so existing data in this range is replaced

2. In the `inter_deals` model, p1 uses the backfill filter:
   - `WHERE timemsc >= '2023-01-01' AND timemsc < '2023-01-31'`
   - Only p1 data from the specified date range is processed

**For p2 and p3:**
1. The staging models for p2 and p3 don't run at all (not included in command)

2. In the `inter_deals` model, p2 and p3 use their normal incremental filters:
   - `WHERE timemsc > (SELECT MAX(timemsc) FROM inter_deals WHERE server_source = 'p2')`
   - `WHERE timemsc > (SELECT MAX(timemsc) FROM inter_deals WHERE server_source = 'p3')`
   - They only include data newer than what's already in the target table

This ensures that only p1 data is backfilled while other servers continue with their normal incremental behavior.

## Handling Server Outages

Each server's incremental logic is tracked independently, so if a server stops producing data temporarily, other servers continue to update correctly.

## Future Improvements

This implementation uses a simple approach with the server_incremental_filter macro. For a larger number of servers, consider these improvements:

1. **Server Configuration in YAML**: Define servers in a central config file
2. **Templated Models**: Create a reusable template for staging models
3. **Dynamic SQL Generation**: Use macros to generate the UNION ALL logic
4. **Health Monitoring**: Add alerting for servers that haven't produced data 