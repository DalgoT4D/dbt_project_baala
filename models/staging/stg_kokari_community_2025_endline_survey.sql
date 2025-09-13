{{
    config(
        materialized='table',
        tags=['staging', 'survey', 'baseline']
    )
}}

-- Staging model for kokari_community_2025_endline_survey
-- This model flattens the JSONB data and standardizes the structure

with kokari_community_2025_endline_survey_data as (
    select
        -- Standard fields
        _id,
        "end",
        data,
        endtime,
        _submission_time,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        
        -- Standardized metadata fields
        case 
            when _submission_time is not null then 
                CAST(_submission_time as timestamp)
            else null 
        end as submission_timestamp,
        
        case 
            when endtime is not null then 
                CAST(endtime as timestamp)
            else null 
        end as end_timestamp,
        
        -- Dynamic field extraction using the new macro
        {{ extract_all_jsonb_fields('data') }},
        
        -- Survey-specific fields can be added here if needed
        
        -- Data quality indicators
        case when data is not null then true else false end as has_json_data,
        case when _submission_time is not null then true else false end as has_submission_time,
        case when endtime is not null then true else false end as has_end_time,
        
        -- Timestamps for analysis
        _airbyte_extracted_at as data_extracted_at,
        current_timestamp as model_created_at
        
    from {{ source('survey_raw_data', 'kokari_community_2025_endline_survey') }}
)

select * from kokari_community_2025_endline_survey_data
