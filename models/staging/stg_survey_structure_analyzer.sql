{{
    config(
        materialized='table',
        tags=['staging', 'survey', 'utility', 'structure_analysis']
    )
}}

-- Survey Structure Analyzer
-- This model analyzes the JSONB structure of all surveys to understand field availability
-- Useful for discovering what fields are available across different surveys

with all_surveys as (
    -- Baseline Questionnaire Bareilly
    select 
        'baseline_questionnaire_bareilly' as survey_name,
        'Baseline Questionnaire Bareilly' as survey_description,
        data,
        _airbyte_extracted_at
    from {{ source('survey_raw_data', 'baseline_questionnaire_bareilly') }}
    
    union all
    
    -- Bir Community Baseline 2025
    select 
        'bir_community_baseline2025' as survey_name,
        'Bir Community Baseline 2025' as survey_description,
        data,
        _airbyte_extracted_at
    from {{ source('survey_raw_data', 'bir_community_baseline2025') }}
    
    union all
    
    -- CAF Ajmer 2024
    select 
        'caf_ajmer_2024_baselineendline_survey' as survey_name,
        'CAF Ajmer 2024 Baseline/Endline' as survey_description,
        data,
        _airbyte_extracted_at
    from {{ source('survey_raw_data', 'caf_ajmer_2024_baselineendline_survey') }}
    
    -- Add more surveys as needed...
    -- You can add all 41 surveys here for complete analysis
),

field_analysis as (
    select 
        survey_name,
        survey_description,
        _airbyte_extracted_at,
        
        -- Extract all unique field names from JSONB
        (select array_agg(distinct key) 
         from jsonb_each(data)) as all_field_names,
        
        -- Count total fields
        jsonb_object_keys(data) as field_count,
        
        -- Extract field types
        (select jsonb_object_agg(key, jsonb_typeof(value))
         from jsonb_each(data)) as field_types,
        
        -- Sample values for each field (first 3 unique values)
        (select jsonb_object_agg(key, 
            case 
                when jsonb_typeof(value) = 'string' then 
                    (select string_agg(distinct value::text, '|') 
                     from (select value::text from jsonb_each(data) where key = k.key limit 3) t)
                when jsonb_typeof(value) = 'number' then 
                    (select string_agg(distinct value::text, '|') 
                     from (select value::text from jsonb_each(data) where key = k.key limit 3) t)
                else 'complex_type'
            end)
         from jsonb_each(data) k) as sample_values
        
    from all_surveys
),

field_categorization as (
    select 
        *,
        
        -- Categorize fields by type
        case 
            when 'name' = any(all_field_names) then true 
            else false 
        end as has_demographic_fields,
        
        case 
            when 'age' = any(all_field_names) or 'gender' = any(all_field_names) then true 
            else false 
        end as has_personal_fields,
        
        case 
            when 'village' = any(all_field_names) or 'district' = any(all_field_names) or 'state' = any(all_field_names) then true 
            else false 
        end as has_location_fields,
        
        case 
            when 'education' = any(all_field_names) or 'occupation' = any(all_field_names) or 'income' = any(all_field_names) then true 
            else false 
        end as has_socioeconomic_fields,
        
        case 
            when 'baseline_date' = any(all_field_names) or 'endline_date' = any(all_field_names) then true 
            else false 
        end as has_temporal_fields,
        
        case 
            when 'school_name' = any(all_field_names) or 'class' = any(all_field_names) then true 
            else false 
        end as has_education_fields,
        
        case 
            when 'health_status' = any(all_field_names) or 'nutrition_status' = any(all_field_names) then true 
            else false 
        end as has_health_fields
        
    from field_analysis
)

select * from field_categorization
order by survey_name
