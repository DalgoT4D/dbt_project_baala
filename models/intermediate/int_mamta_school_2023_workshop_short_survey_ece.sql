{{
    config(
        materialized='table',
        tags=['intermediate', 'survey', 'school', 'workshop']
    )
}}

-- Intermediate model for Mamta School 2023 Workshop Short Survey Ece with business logic and transformations
-- This model applies business logic and transformations on top of the staging layer

with transformed as (
    select
        -- All fields from staging (includes flattened JSONB data)
        *
        
    from {{ ref('stg_mamta_school_2023_workshop_short_survey_ece') }}
    
    -- Add any business logic, filtering, or transformations here
    -- For example:
    -- where survey_id is not null
    -- and has_json_data = true
)

select 
    -- Apply any final transformations or column selections
    * 
from transformed
