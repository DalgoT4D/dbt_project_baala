{% macro extract_all_jsonb_fields(jsonb_column, exclude_fields=None) %}
    {% set exclude_fields = exclude_fields or [] %}
    
    -- This macro dynamically extracts all fields from JSONB data
    -- It creates a generic structure that can handle any survey format
    
    -- Common fields that are typically present
    {% set common_fields = [
        'name', 'age', 'gender', 'location', 'village', 'district', 'state',
        'education', 'occupation', 'income', 'family_size', 'children_count',
        'survey_date', 'respondent_id', 'household_id', 'community_id',
        'project_name', 'intervention_type', 'baseline_date', 'endline_date'
    ] %}
    
    -- Extract common fields first
    {% for field in common_fields %}
        {% if field not in exclude_fields %}
            {{ extract_jsonb_value(jsonb_column, field) }} as {{ field }},
        {% endif %}
    {% endfor %}
    
    -- Extract all other fields dynamically using jsonb_each
    -- This will capture any additional fields not in the common list
    (select string_agg(key || ':' || value, '; ') 
     from jsonb_each({{ jsonb_column }}) 
     where key not in ({% for field in common_fields %}'{{ field }}'{% if not loop.last %}, {% endif %}{% endfor %})
    ) as additional_fields_json
{% endmacro %}

{% macro extract_jsonb_keys(jsonb_column) %}
    -- Extract all keys from JSONB for dynamic field discovery
    (select array_agg(key) from jsonb_each({{ jsonb_column }}))
{% endmacro %}

{% macro extract_jsonb_field_if_exists(jsonb_column, field_name, default_value='') %}
    -- Safely extract a field if it exists, otherwise return default
    case 
        when {{ jsonb_column }} ? '{{ field_name }}' 
        then {{ extract_jsonb_value(jsonb_column, field_name) }}
        else '{{ default_value }}'
    end
{% endmacro %}

{% macro extract_conditional_fields(jsonb_column, field_mappings) %}
    -- Extract fields based on conditional logic
    {% for field_name, config in field_mappings.items() %}
        {% set alias = config.get('alias', field_name) %}
        {% set default = config.get('default', '') %}
        {% set condition = config.get('condition', '') %}
        
        {% if condition %}
            case when {{ condition }} then {{ extract_jsonb_value(jsonb_column, field_name) }} else '{{ default }}' end as {{ alias }},
        {% else %}
            {{ extract_jsonb_value(jsonb_column, field_name) }} as {{ alias }},
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro create_dynamic_schema(jsonb_column) %}
    -- Create a dynamic schema based on the actual JSONB structure
    -- This is useful for creating flexible staging models
    
    -- Extract all keys and create a dynamic structure
    with jsonb_structure as (
        select 
            key,
            jsonb_type(value) as value_type,
            case 
                when jsonb_type(value) = 'string' then 'text'
                when jsonb_type(value) = 'number' then 'numeric'
                when jsonb_type(value) = 'boolean' then 'boolean'
                when jsonb_type(value) = 'array' then 'jsonb'
                when jsonb_type(value) = 'object' then 'jsonb'
                else 'text'
            end as suggested_type
        from jsonb_each({{ jsonb_column }})
    )
    select 
        key as field_name,
        suggested_type as data_type,
        'Extracted from JSONB' as description
    from jsonb_structure
{% endmacro %}
