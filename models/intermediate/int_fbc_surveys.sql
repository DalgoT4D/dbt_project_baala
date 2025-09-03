{{
    config(
        materialized='table',
        tags=['intermediate', 'survey', 'fbc', 'combined']
    )
}}

-- Intermediate model combining all FBC (Foundation for Better Childhood) surveys
-- This model provides a unified view of FBC project data across different target groups and time periods

with fbc_surveys as (
    -- FBC Community 2024 baseline
    select 
        *,
        'FBC_Community_2024' as project_identifier,
        'Community' as target_group,
        2024 as survey_year,
        'baseline' as survey_phase
    from {{ ref('stg_fbc_community_2024_baseline_survey') }}
    
    union all
    
    -- FBC School Boys 2024 baseline
    select 
        *,
        'FBC_School_Boys_2024' as project_identifier,
        'School_Boys' as target_group,
        2024 as survey_year,
        'baseline' as survey_phase
    from {{ ref('stg_fbc_school_boys_2024_baseline_survey') }}
    
    union all
    
    -- FBC School Boys 2024 endline
    select 
        *,
        'FBC_School_Boys_2024' as project_identifier,
        'School_Boys' as target_group,
        2024 as survey_year,
        'endline' as survey_phase
    from {{ ref('stg_fbc_school_boys_2024_endline_survey') }}
    
    union all
    
    -- FBC School Girls 2024 baseline
    select 
        *,
        'FBC_School_Girls_2024' as project_identifier,
        'School_Girls' as target_group,
        2024 as survey_year,
        'baseline' as survey_phase
    from {{ ref('stg_fbc_school_girls_2024_baseline_survey') }}
    
    union all
    
    -- FBC School Girls 2024 endline
    select 
        *,
        'FBC_School_Girls_2024' as project_identifier,
        'School_Girls' as target_group,
        2024 as survey_year,
        'endline' as survey_phase
    from {{ ref('stg_fbc_school_girls_2024_endline_survey') }}
    
    union all
    
    -- FBC Women Jhatingri baseline
    select 
        *,
        'FBC_Women_Jhatingri' as project_identifier,
        'Women' as target_group,
        null as survey_year,
        'baseline' as survey_phase
    from {{ ref('stg_fbc_women_jhatingri_baseline_questionaire') }}
),

enriched_fbc_data as (
    select 
        *,
        -- Create standardized project categories
        case 
            when target_group = 'Community' then 'Community_Development'
            when target_group like '%School%' then 'Education'
            when target_group = 'Women' then 'Women_Empowerment'
            else 'Other'
        end as project_category,
        
        -- Create standardized respondent categories
        case 
            when target_group like '%Boys%' then 'Male_Students'
            when target_group like '%Girls%' then 'Female_Students'
            when target_group = 'Women' then 'Adult_Women'
            when target_group = 'Community' then 'Community_Members'
            else 'Other'
        end as respondent_category,
        
        -- Create standardized intervention categories
        case 
            when survey_phase = 'baseline' then 'Pre_Intervention'
            when survey_phase = 'endline' then 'Post_Intervention'
            else 'Unknown'
        end as intervention_stage,
        
        -- Create standardized location categories
        case 
            when target_group = 'Women' then 'Jhatingri'
            else 'Multiple_Locations'
        end as specific_location
        
    from fbc_surveys
)

select * from enriched_fbc_data
