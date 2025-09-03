{{
    config(
        materialized='view',
        tags=['staging', 'survey', 'dynamic', 'template']
    )
}}

-- Dynamic staging model template for any survey
-- This model automatically adapts to the JSONB structure of the source data

with dynamic_survey_data as (
    select
        -- Standard metadata fields (always present)
        _id,
        end,
        data,
        endtime,
        _submission_time,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        
        -- Standardized metadata fields
        case 
            when _submission_time is not null then 
                try_cast(_submission_time as timestamp)
            else null 
        end as submission_timestamp,
        
        case 
            when endtime is not null then 
                try_cast(endtime as timestamp)
            else null 
        end as end_timestamp,
        
        -- Dynamic field extraction using the new macro
        {{ extract_all_jsonb_fields('data') }}
        
        -- Data quality indicators
        case when data is not null then true else false end as has_json_data,
        case when _submission_time is not null then true else false end as has_submission_time,
        case when endtime is not null then true else false end as has_end_time,
        
        -- Timestamps for analysis
        _airbyte_extracted_at as data_extracted_at,
        current_timestamp as model_created_at
        
    from {{ source('survey_raw_data', 'your_survey_name_here') }}
),

-- Dynamic field discovery and categorization
field_analysis as (
    select 
        *,
        -- Analyze the structure of the JSONB data
        {{ extract_jsonb_keys('data') }} as all_field_names,
        
        -- Categorize fields based on their names
        case 
            when 'name' = any({{ extract_jsonb_keys('data') }}) then true 
            else false 
        end as has_name_field,
        
        case 
            when 'age' = any({{ extract_jsonb_keys('data') }}) then true 
            else false 
        end as has_age_field,
        
        case 
            when 'gender' = any({{ extract_jsonb_keys('data') }}) then true 
            else false 
        end as has_gender_field,
        
        case 
            when 'location' = any({{ extract_jsonb_keys('data') }}) or 
                 'village' = any({{ extract_jsonb_keys('data') }}) or
                 'district' = any({{ extract_jsonb_keys('data') }}) or
                 'state' = any({{ extract_jsonb_keys('data') }}) then true 
            else false 
        end as has_location_fields,
        
        case 
            when 'education' = any({{ extract_jsonb_keys('data') }}) or
                 'occupation' = any({{ extract_jsonb_keys('data') }}) or
                 'income' = any({{ extract_jsonb_keys('data') }}) then true 
        else false 
        end as has_socioeconomic_fields
        
    from dynamic_survey_data
)

select * from field_analysis
