{{
    config(
        materialized='view',
        tags=['staging', 'survey', 'fbc', 'school', 'boys', 'baseline']
    )
}}

-- Staging model for FBC School Boys 2024 baseline survey data
-- This model flattens the JSONB data and standardizes the structure

with fbc_school_boys_data as (
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
        
        -- FBC School Boys specific fields
        {{ extract_jsonb_value('data', 'school_name') }} as school_name,
        {{ extract_jsonb_value('data', 'class') }} as class_level,
        {{ extract_jsonb_value('data', 'section') }} as section,
        {{ extract_jsonb_value('data', 'roll_number') }} as roll_number,
        {{ extract_jsonb_value('data', 'teacher_name') }} as teacher_name,
        {{ extract_jsonb_value('data', 'parent_name') }} as parent_name,
        {{ extract_jsonb_value('data', 'parent_contact') }} as parent_contact,
        {{ extract_jsonb_value('data', 'address') }} as address,
        
        -- Academic performance fields
        {{ extract_jsonb_value('data', 'previous_year_percentage') }} as previous_year_percentage,
        {{ extract_jsonb_value('data', 'current_year_performance') }} as current_year_performance,
        {{ extract_jsonb_value('data', 'favorite_subject') }} as favorite_subject,
        {{ extract_jsonb_value('data', 'difficult_subject') }} as difficult_subject,
        {{ extract_jsonb_value('data', 'study_hours_daily') }} as study_hours_daily,
        {{ extract_jsonb_value('data', 'homework_completion') }} as homework_completion,
        {{ extract_jsonb_value('data', 'library_usage') }} as library_usage,
        
        -- Extracurricular activities
        {{ extract_jsonb_value('data', 'sports_participation') }} as sports_participation,
        {{ extract_jsonb_value('data', 'cultural_activities') }} as cultural_activities,
        {{ extract_jsonb_value('data', 'clubs_joined') }} as clubs_joined,
        {{ extract_jsonb_value('data', 'competitions_participated') }} as competitions_participated,
        
        -- Health and nutrition
        {{ extract_jsonb_value('data', 'height_cm') }} as height_cm,
        {{ extract_jsonb_value('data', 'weight_kg') }} as weight_kg,
        {{ extract_jsonb_value('data', 'bmi') }} as bmi,
        {{ extract_jsonb_value('data', 'health_status') }} as health_status,
        {{ extract_jsonb_value('data', 'nutrition_status') }} as nutrition_status,
        {{ extract_jsonb_value('data', 'breakfast_regularity') }} as breakfast_regularity,
        {{ extract_jsonb_value('data', 'mid_day_meal') }} as mid_day_meal,
        
        -- Social and emotional development
        {{ extract_jsonb_value('data', 'friends_count') }} as friends_count,
        {{ extract_jsonb_value('data', 'bullying_experience') }} as bullying_experience,
        {{ extract_jsonb_value('data', 'teacher_support') }} as teacher_support,
        {{ extract_jsonb_value('data', 'parent_support') }} as parent_support,
        {{ extract_jsonb_value('data', 'confidence_level') }} as confidence_level,
        {{ extract_jsonb_value('data', 'stress_level') }} as stress_level,
        
        -- Future aspirations
        {{ extract_jsonb_value('data', 'career_interest') }} as career_interest,
        {{ extract_jsonb_value('data', 'higher_education_plan') }} as higher_education_plan,
        {{ extract_jsonb_value('data', 'role_model') }} as role_model,
        {{ extract_jsonb_value('data', 'dream_job') }} as dream_job,
        
        -- Data quality indicators
        case when data is not null then true else false end as has_json_data,
        case when _submission_time is not null then true else false end as has_submission_time,
        case when endtime is not null then true else false end as has_end_time,
        
        -- Timestamps for analysis
        _airbyte_extracted_at as data_extracted_at,
        current_timestamp as model_created_at
        
    from {{ source('survey_raw_data', 'fbc_school_boys_2024_baseline_survey') }}
)

select * from fbc_school_boys_data
