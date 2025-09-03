{{
    config(
        materialized='table',
        tags=['intermediate', 'survey', 'caf', 'combined']
    )
}}

-- Intermediate model combining all CAF (Child Aid Foundation) surveys
-- This model provides a unified view of CAF project data across different locations and time periods

with caf_surveys as (
    -- CAF Ajmer 2024 baseline and endline
    select 
        *,
        'CAF_Ajmer_2024' as project_identifier,
        'Ajmer' as location,
        2024 as survey_year,
        case 
            when survey_type ilike '%baseline%' then 'baseline'
            when survey_type ilike '%endline%' then 'endline'
            else 'unknown'
        end as survey_phase
    from {{ ref('stg_caf_ajmer_2024_baselineendline_survey') }}
    
    union all
    
    -- CAF Ajmer students 2025 baseline
    select 
        *,
        'CAF_Ajmer_Students_2025' as project_identifier,
        'Ajmer' as location,
        2025 as survey_year,
        'baseline' as survey_phase
    from {{ ref('stg_caf_ajmer_students_2025_baseline_survey') }}
    
    union all
    
    -- CAF Delhi School 2024 baseline
    select 
        *,
        'CAF_Delhi_School_2024' as project_identifier,
        'Delhi' as location,
        2024 as survey_year,
        'baseline' as survey_phase
    from {{ ref('stg_caf_delhi_school_2024_baseline_survey') }}
    
    union all
    
    -- CAF Delhi School 2025 endline
    select 
        *,
        'CAF_Delhi_School_2025' as project_identifier,
        'Delhi' as location,
        2025 as survey_year,
        'endline' as survey_phase
    from {{ ref('stg_caf_delhi_school_2025_endline_survey') }}
),

enriched_caf_data as (
    select 
        *,
        -- Create standardized project categories
        case 
            when location = 'Ajmer' then 'Rural'
            when location = 'Delhi' then 'Urban'
            else 'Unknown'
        end as project_type,
        
        -- Create standardized respondent categories
        case 
            when project_identifier like '%Students%' then 'Students'
            when project_identifier like '%School%' then 'School_Community'
            else 'General_Community'
        end as respondent_category,
        
        -- Create standardized intervention categories
        case 
            when survey_phase = 'baseline' then 'Pre_Intervention'
            when survey_phase = 'endline' then 'Post_Intervention'
            else 'Unknown'
        end as intervention_stage
        
    from caf_surveys
)

select * from enriched_caf_data
