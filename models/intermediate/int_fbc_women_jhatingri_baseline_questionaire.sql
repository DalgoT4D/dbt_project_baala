{{
    config(
        materialized='table',
        tags=['intermediate', 'survey', 'baseline', 'women']
    )
}}

-- Intermediate model for Fbc Women Jhatingri Baseline Questionaire with business logic and transformations
-- This model applies business logic and transformations on top of the staging layer

with transformed as (
    select
        -- All fields from staging (includes flattened JSONB data)
        *
        
    from {{ ref('stg_fbc_women_jhatingri_baseline_questionaire') }}
    
    -- Add any business logic, filtering, or transformations here
    -- For example:
    -- where survey_id is not null
    -- and has_json_data = true
)

select 
    -- Apply any final transformations or column selections
    * 
from transformed
