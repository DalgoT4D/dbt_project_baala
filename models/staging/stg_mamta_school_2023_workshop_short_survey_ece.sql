{{
    config(
        materialized='table',
        tags=['staging', 'survey', 'school', 'workshop']
    )
}}

-- Staging model for Mamta School 2023 Workshop Short Survey Ece with flattened JSONB data
-- This model flattens the JSONB data and standardizes the structure

with stg_mamta_school_2023_workshop_short_survey_ece_data as (
    select
        -- Standard metadata fields
        _id::integer as survey_id,
        
        -- Basic timestamps
        case 
            when _submission_time is not null
                then 
                    _submission_time::timestamp 
        end as submission_timestamp,
        
        case 
            when "end" is not null
                then 
                    "end"::timestamp 
        end as end_timestamp,
        
        -- Raw fields (for reference)
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        
        -- Data quality indicators
        coalesce(data is not null, false) as has_json_data,
        coalesce(_submission_time is not null, false) as has_submission_time,
        coalesce("end" is not null, false) as has_end_time,
        
        -- Dynamic JSONB flattening using the macro
        {{ flatten_json_columns(source('survey_raw_data', 'mamta_school_2023_workshop_short_survey_ece'), "data") }},
        
        -- Additional metadata
        _airbyte_extracted_at as data_extracted_at,
        current_timestamp as model_created_at,
        'mamta_school_2023_workshop_short_survey_ece' as source_table
        
    from {{ source('survey_raw_data', 'mamta_school_2023_workshop_short_survey_ece') }}
    where data is not null
)

select * from stg_mamta_school_2023_workshop_short_survey_ece_data
