{% macro flatten_json_columns(table, column) %}
  {% set keys_query %}
    select distinct jsonb_object_keys({{ column }}) as key
    from {{ table }}
    where {{ column }} is not null
  {% endset %}

  {% set query_result = run_query(keys_query) %}
  
  {% if query_result and query_result.columns[0].values() %}
    {% set results = query_result.columns[0].values() %}
    {% set used_keys = [] %}
    {% for key in results %}
      {% if key not in ['_id', '_submission_time', 'end', 'endtime'] %}
        {% set safe_key = key.replace('/', '_').replace(' ', '_').replace('-', '_').replace('.', '_').replace('(', '').replace(')', '').replace('?', '').replace('!', '').replace('&', 'and').lower() %}
        
        {# Use original key + loop index to ensure uniqueness #}
        {% set unique_key = safe_key + '_' + loop.index|string %}

        {{ column }} ->> '{{ key }}' as {{ unique_key }}{% if not loop.last %},{% endif %}
      {% endif %}
    {% endfor %}
  {% else %}
    -- Fallback: extract common fields if dynamic extraction fails
    {{ column }} ->> 'name' as name,
    {{ column }} ->> 'age' as age,
    {{ column }} ->> 'gender' as gender,
    {{ column }} ->> 'location' as location,
    {{ column }} ->> 'project_name' as project_name
  {% endif %}
{% endmacro %}
