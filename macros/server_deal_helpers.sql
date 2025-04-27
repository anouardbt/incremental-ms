-- This file is kept for reference but isn't actively used
-- in the current simplified implementation

{% macro server_incremental_filter(column_name, server_id, is_backfill=false) %}
  {% if is_incremental() and not is_backfill %}
    -- Get data newer than the latest timestamp for this specific server
    WHERE {{ column_name }} > (
      SELECT MAX({{ column_name }})
      FROM {{ this }}
      WHERE server_source = '{{ server_id }}'
    )
  {% elif is_backfill %}
    -- For backfills, use the provided date range
    WHERE {{ column_name }} >= '{{ var('start_date') }}'
      AND {{ column_name }} < '{{ var('end_date') }}'
  {% endif %}
{% endmacro %}

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

{% macro backfill_filter(column_name) %}
  WHERE {{ column_name }} >= '{{ var('start_date') }}'
    AND {{ column_name }} < '{{ var('end_date') }}'
{% endmacro %} 