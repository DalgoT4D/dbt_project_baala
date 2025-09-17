{{
    config(
        materialized='table',
        tags=['staging', 'survey', 'endline']
    )
}}

-- Staging model for Mamta 2025 Endline Survey with flattened JSONB data
-- This model flattens the JSONB data and standardizes the structure

with stg_mamta_2025_endline_survey_data as (
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
        
        -- Raw fields (for reference)
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        
        -- Data quality indicators
        case when data is not null then true else false end as has_json_data,
        case when _submission_time is not null then true else false end as has_submission_time,
        case when "end" is not null then true else false end as has_end_time,
        
        -- Dynamic JSONB flattening using the macro
        {{ flatten_json_columns(source('survey_raw_data', 'mamta_2025_endline_survey'), "data") }},
        
        -- Additional metadata
        _airbyte_extracted_at as data_extracted_at,
        current_timestamp as model_created_at,
        'mamta_2025_endline_survey' as source_table
        
    from {{ source('survey_raw_data', 'mamta_2025_endline_survey') }}
    where data is not null
)

select * from stg_mamta_2025_endline_survey_data
