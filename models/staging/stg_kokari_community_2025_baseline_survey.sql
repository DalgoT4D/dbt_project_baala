{{
    config(
        materialized='table',
        tags=['staging', 'survey', 'baseline', 'community']
    )
}}

-- Staging model for Kokari Community 2025 Baseline Survey with flattened JSONB data
-- This model flattens the JSONB data and standardizes the structure

with stg_kokari_community_2025_baseline_survey_data as (
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
        {{ flatten_json_columns(source('survey_raw_data', 'kokari_community_2025_baseline_survey'), "data") }},
        
        -- Additional metadata
        _airbyte_extracted_at as data_extracted_at,
        current_timestamp as model_created_at,
        'kokari_community_2025_baseline_survey' as source_table
        
    from {{ source('survey_raw_data', 'kokari_community_2025_baseline_survey') }}
    where data is not null
)

select * from stg_kokari_community_2025_baseline_survey_data
