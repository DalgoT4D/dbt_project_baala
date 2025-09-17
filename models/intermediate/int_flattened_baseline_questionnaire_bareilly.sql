{{
    config(
        materialized='table',
        tags=['intermediate', 'flattened', 'survey', 'baseline']
    )
}}

-- Flattened intermediate model for Baseline Questionnaire Bareilly
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
        {{ flatten_json_columns(source('survey_raw_data', 'baseline_questionnaire_bareilly'), "data") }},
        
        -- Additional metadata
        current_timestamp as flattened_at,
        _airbyte_extracted_at as data_extracted_at,
        'baseline_questionnaire_bareilly' as source_table

    from {{ source('survey_raw_data', 'baseline_questionnaire_bareilly') }}
    where data is not null
)

select * from flattened
