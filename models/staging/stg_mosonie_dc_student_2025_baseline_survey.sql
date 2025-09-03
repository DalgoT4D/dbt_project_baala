{
    config(
        materialized='view',
        tags=['staging', 'survey', 'baseline']
    )
}

-- Staging model for Mosonie DC Student 2025 baseline survey data
-- This model flattens the JSONB data and standardizes the structure

with mosonie_dc_student_2025_baseline_survey_data as (
    select
        -- Standard fields
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
        
        -- Extract common JSONB fields
        {{ extract_jsonb_value('data', 'name') }} as respondent_name,
        {{ extract_jsonb_value('data', 'age') }} as respondent_age,
        {{ extract_jsonb_value('data', 'gender') }} as respondent_gender,
        {{ extract_jsonb_value('data', 'location') }} as location,
        {{ extract_jsonb_value('data', 'village') }} as village,
        {{ extract_jsonb_value('data', 'district') }} as district,
        {{ extract_jsonb_value('data', 'state') }} as state,
        {{ extract_jsonb_value('data', 'education') }} as education_level,
        {{ extract_jsonb_value('data', 'occupation') }} as occupation,
        {{ extract_jsonb_value('data', 'income') }} as income_level,
        {{ extract_jsonb_value('data', 'family_size') }} as family_size,
        {{ extract_jsonb_value('data', 'children_count') }} as children_count,
        {{ extract_jsonb_value('data', 'survey_date') }} as survey_date,
        {{ extract_jsonb_value('data', 'respondent_id') }} as respondent_id,
        {{ extract_jsonb_value('data', 'household_id') }} as household_id,
        {{ extract_jsonb_value('data', 'community_id') }} as community_id,
        {{ extract_jsonb_value('data', 'project_name') }} as project_name,
        {{ extract_jsonb_value('data', 'intervention_type') }} as intervention_type,
        
        -- Baseline specific fields
        {{ extract_jsonb_value('data', 'initial_assessment') }} as initial_assessment,
        {{ extract_jsonb_value('data', 'pre_intervention') }} as pre_intervention,
        {{ extract_jsonb_value('data', 'baseline_date') }} as baseline_date,
        
        -- Data quality indicators
        case when data is not null then true else false end as has_json_data,
        case when _submission_time is not null then true else false end as has_submission_time,
        case when endtime is not null then true else false end as has_end_time,
        
        -- Timestamps for analysis
        _airbyte_extracted_at as data_extracted_at,
        current_timestamp as model_created_at
        
    from {{ source('survey_raw_data', 'mosonie_dc_student_2025_baseline_survey') }}
)

select * from mosonie_dc_student_2025_baseline_survey_data
