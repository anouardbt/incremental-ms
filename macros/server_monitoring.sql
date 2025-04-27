{% macro get_server_sync_status(threshold_hours=24) %}
  
  {% set query %}
    WITH server_last_updates AS (
      SELECT 
        server_source,
        MAX(timemsc) as last_update_time
      FROM {{ ref('inter_deals') }}
      GROUP BY server_source
    ),
    
    max_overall_time AS (
      SELECT MAX(last_update_time) as max_time
      FROM server_last_updates
    ),
    
    server_status AS (
      SELECT
        s.server_source,
        s.last_update_time,
        m.max_time,
        TIMESTAMP_DIFF(m.max_time, s.last_update_time, HOUR) as hours_behind,
        CASE
          WHEN TIMESTAMP_DIFF(m.max_time, s.last_update_time, HOUR) > {{ threshold_hours }} 
          THEN false
          ELSE true
        END as is_up_to_date
      FROM server_last_updates s
      CROSS JOIN max_overall_time m
    )
    
    SELECT * FROM server_status
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set server_statuses = {} %}
    {% for row in results %}
      {% do server_statuses.update({
        row.server_source: {
          'last_update': row.last_update_time,
          'hours_behind': row.hours_behind,
          'is_up_to_date': row.is_up_to_date
        }
      }) %}
    {% endfor %}
    {{ return(server_statuses) }}
  {% else %}
    {{ return({}) }}
  {% endif %}
{% endmacro %}

{% macro check_all_servers_in_sync(threshold_hours=24) %}
  {% set server_statuses = get_server_sync_status(threshold_hours) %}
  
  {% set all_in_sync = true %}
  {% set lagging_servers = [] %}
  
  {% for server, status in server_statuses.items() %}
    {% if not status.is_up_to_date %}
      {% set all_in_sync = false %}
      {% do lagging_servers.append(server) %}
    {% endif %}
  {% endfor %}
  
  {% if all_in_sync %}
    {{ log("All servers are in sync within " ~ threshold_hours ~ " hours", info=true) }}
    {{ return(true) }}
  {% else %}
    {{ log("Warning: The following servers are lagging: " ~ lagging_servers | join(", "), info=true) }}
    {{ return(false) }}
  {% endif %}
{% endmacro %}

{% macro check_servers_exact_sync() %}
  {% set query %}
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
    
    SELECT distinct_count
    FROM distinct_times_count
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set distinct_count = results.columns[0].values()[0] %}
    
    {% if distinct_count == 1 %}
      {{ log("All servers have the exact same maximum timestamp", info=true) }}
      {{ return(true) }}
    {% else %}
      {{ log("ALERT: Servers are out of sync. Found " ~ distinct_count ~ " different max timestamps.", info=true) }}
      {{ return(false) }}
    {% endif %}
  {% else %}
    {{ return(none) }}
  {% endif %}
{% endmacro %} 