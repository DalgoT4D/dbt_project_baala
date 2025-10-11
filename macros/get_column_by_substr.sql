{% macro get_column_by_substring_postgres(relation, substring) %}
    {# relation must be a ref() or source() #}
    {% if relation is none %}
        {{ exceptions.warn("⚠️ get_column_by_substring_postgres: relation is None. Returning 'N/A'.") }}
        {{ return('N/A') }}
    {% endif %}

    {% set database = relation.database or target.database %}
    {% set schema = relation.schema %}
    {% set table = relation.identifier %}

    {% set sql %}
        select column_name
        from {{ database }}.information_schema.columns
        where table_schema = '{{ schema }}'
          and table_name = '{{ table }}'
          and lower(column_name) like '%{{ substring | lower }}%'
        limit 1
    {% endset %}

    {% if execute %}
        {% set result = run_query(sql) %}
        {% if result is none or result.rows | length == 0 %}
            {{ log("ℹ️ No column matching substring '" ~ substring ~ "' found in " ~ schema ~ "." ~ table ~ ". Returning 'N/A'.", info=True) }}
            {{ return('N/A') }}
        {% else %}
            {% set colname = result.columns[0].values()[0] %}
            {{ return(colname) }}
        {% endif %}
    {% else %}
        {{ return('N/A') }}
    {% endif %}
{% endmacro %}
