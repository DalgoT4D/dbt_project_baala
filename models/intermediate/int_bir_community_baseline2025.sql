{{
    config(
        materialized='table',
        tags=['intermediate', 'survey', 'baseline', 'community']
    )
}}

-- Intermediate model for Bir Community Baseline2025 with business logic and transformations
-- This model applies business logic and transformations on top of the staging layer

with transformed as (
    select
        -- All fields except Airbyte and data quality columns
        {{ 
            dbt_utils.star(
                from=ref('stg_bir_community_baseline2025'), 
                except=[
                    "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta",
                    "has_json_data", "has_submission_time", "has_end_time"
                ]
            ) 
        }}
        
    from {{ ref('stg_bir_community_baseline2025') }}
    
    -- Add any business logic, filtering, or transformations here
    -- For example:
    -- where survey_id is not null
)

select 
    -- Apply any final transformations or column selections
    * 
from transformed
