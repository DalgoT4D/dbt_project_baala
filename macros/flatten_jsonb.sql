{% macro flatten_jsonb_column(column_name, prefix='') %}
    {% set flattened_columns = [] %}
    
    -- Common survey fields that are typically present in JSONB data
    {% set common_fields = [
        'name', 'age', 'gender', 'location', 'village', 'district', 'state',
        'education', 'occupation', 'income', 'family_size', 'children_count',
        'survey_date', 'respondent_id', 'household_id', 'community_id',
        'baseline_date', 'endline_date', 'project_name', 'intervention_type',
        'satisfaction_score', 'knowledge_score', 'attitude_score', 'practice_score',
        'health_status', 'nutrition_status', 'water_access', 'sanitation_access',
        'electricity_access', 'road_access', 'school_access', 'healthcare_access',
        'employment_status', 'skill_level', 'training_received', 'benefits_received',
        'challenges_faced', 'recommendations', 'feedback', 'notes'
    ] %}
    
    {% for field in common_fields %}
        {% set column_alias = prefix ~ field if prefix else field %}
        {% set flattened_columns = flattened_columns + [
            "COALESCE(data->>'" ~ field ~ "', '') as " ~ column_alias
        ] %}
    {% endfor %}
    
    {{ return(flattened_columns | join(',\n    ')) }}
{% endmacro %}

{% macro extract_jsonb_value(jsonb_column, key, default_value='') %}
    COALESCE({{ jsonb_column }}->>'{{ key }}', '{{ default_value }}')
{% endmacro %}

{% macro extract_jsonb_array(jsonb_column, key) %}
    {{ jsonb_column }}->'{{ key }}'
{% endmacro %}

{% macro extract_jsonb_object(jsonb_column, key) %}
    {{ jsonb_column }}->'{{ key }}'
{% endmacro %}
