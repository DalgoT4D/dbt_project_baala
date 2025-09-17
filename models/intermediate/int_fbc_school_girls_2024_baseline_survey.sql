{{
    config(
        materialized='table',
        tags=['intermediate', 'survey', 'baseline', 'school', 'women']
    )
}}

-- Intermediate model for Fbc School Girls 2024 Baseline Survey with business logic and transformations
-- This model applies business logic and transformations on top of the staging layer

with transformed as (
    select
        -- All fields from staging (includes flattened JSONB data)
        *
        
    from {{ ref('stg_fbc_school_girls_2024_baseline_survey') }}
    
    -- Add any business logic, filtering, or transformations here
    -- For example:
    -- where survey_id is not null
    -- and has_json_data = true
)

select 
    -- Apply any final transformations or column selections
    * 
from transformed
