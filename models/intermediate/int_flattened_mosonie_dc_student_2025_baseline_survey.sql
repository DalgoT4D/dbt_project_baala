{{
    config(
        materialized='table',
        tags=['intermediate', 'flattened', 'survey', 'baseline', 'school']
    )
}}

-- Flattened intermediate model for Mosonie Dc Student 2025 Baseline Survey
-- This model flattens the JSONB data from the source table into individual columns

with flattened as (
    select
        -- Standard metadata fields
        _id::integer as survey_id,
        
        -- Basic timestamps
        case 
            when _submission_time is not null then 
                CAST(_submission_time as timestamp)
            else null 
        end as submission_timestamp,
        
        case 
            when "end" is not null then 
                CAST("end" as timestamp)
            else null 
        end as end_timestamp,
        
        -- Data quality indicators
        case when data is not null then true else false end as has_json_data,
        case when _submission_time is not null then true else false end as has_submission_time,
        case when "end" is not null then true else false end as has_end_time,
        
        -- Dynamic JSONB flattening using the macro (directly from source)
        {{ flatten_json_columns(source('survey_raw_data', 'mosonie_dc_student_2025_baseline_survey'), "data") }},
        
        -- Additional metadata
        current_timestamp as flattened_at,
        _airbyte_extracted_at as data_extracted_at,
        'mosonie_dc_student_2025_baseline_survey' as source_table

    from {{ source('survey_raw_data', 'mosonie_dc_student_2025_baseline_survey') }}
    where data is not null
)

select * from flattened
