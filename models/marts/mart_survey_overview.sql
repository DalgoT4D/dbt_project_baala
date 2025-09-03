{{
    config(
        materialized='table',
        tags=['marts', 'survey', 'overview', 'reporting']
    )
}}

-- Mart model providing comprehensive overview of all survey data
-- This model serves as the primary data source for survey analysis and reporting

with all_surveys as (
    -- CAF Surveys
    select 
        *,
        'CAF' as organization,
        'Child Aid Foundation' as organization_full_name
    from {{ ref('int_caf_surveys') }}
    
    union all
    
    -- FBC Surveys
    select 
        *,
        'FBC' as organization,
        'Foundation for Better Childhood' as organization_full_name
    from {{ ref('int_fbc_surveys') }}
    
    union all
    
    -- Individual survey models (for surveys not yet in intermediate models)
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_baseline_questionnaire_bareilly') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_bir_community_baseline2025') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_elephanta_community_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_ffec_community_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_jakson_cc_trainers_2025_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_jakson_community_2024_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_jakson_community_2025_endline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_jk_fenner_hyd_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_kokari_2024_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_kokari_community_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_kokari_community_2025_endline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_majuli_needs_assessment') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_mamta_2025_endline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_mamta_school_2023_workshop_long_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_mamta_school_2023_workshop_short_survey_ece') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_mamta_school_2024_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_mamta_school_2025_endline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_mosonie_dc_project_teachers_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_mosonie_dc_student_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_mosonie_irctc_community_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_msi_cs_2024_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_msi_cs_2024_endline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_msi_cs_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_msi_cs_2025_endline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_msi_trainers_2025_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_ntpc_community_2024_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_ntpc_community_2025_endline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_observation_checklist') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_tfi_school_2024_baseline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_tfi_school_2024_endline_survey') }}
    
    union all
    
    select 
        *,
        'Individual' as organization,
        'Individual Project' as organization_full_name,
        'Unknown' as project_identifier,
        'Unknown' as target_group,
        null as survey_year,
        'Unknown' as survey_phase,
        'Unknown' as project_category,
        'Unknown' as respondent_category,
        'Unknown' as intervention_stage,
        'Unknown' as specific_location
    from {{ ref('stg_yflo_community_2025_baseline_survey') }}
),

final_survey_data as (
    select 
        *,
        -- Create standardized survey categories
        case 
            when organization = 'CAF' then 'Education_Development'
            when organization = 'FBC' then 'Childhood_Development'
            else 'Other_Development'
        end as organization_category,
        
        -- Create standardized location categories
        case 
            when location in ('Ajmer', 'Delhi') then location
            when specific_location != 'Unknown' then specific_location
            when village is not null then village
            when district is not null then district
            when state is not null then state
            else 'Unknown_Location'
        end as standardized_location,
        
        -- Create standardized respondent age groups
        case 
            when try_cast(respondent_age as integer) < 18 then 'Under_18'
            when try_cast(respondent_age as integer) between 18 and 25 then '18_25'
            when try_cast(respondent_age as integer) between 26 and 35 then '26_35'
            when try_cast(respondent_age as integer) between 36 and 50 then '36_50'
            when try_cast(respondent_age as integer) > 50 then 'Over_50'
            else 'Unknown_Age'
        end as age_group,
        
        -- Create standardized education categories
        case 
            when education_level ilike '%primary%' then 'Primary'
            when education_level ilike '%secondary%' then 'Secondary'
            when education_level ilike '%higher%' or education_level ilike '%college%' then 'Higher'
            when education_level ilike '%university%' then 'University'
            when education_level ilike '%none%' or education_level ilike '%illiterate%' then 'None'
            else 'Other'
        end as standardized_education,
        
        -- Create standardized income categories
        case 
            when income_level ilike '%low%' or income_level ilike '%below%' then 'Low'
            when income_level ilike '%medium%' or income_level ilike '%middle%' then 'Medium'
            when income_level ilike '%high%' or income_level ilike '%above%' then 'High'
            else 'Unknown'
        end as standardized_income
        
    from all_surveys
)

select * from final_survey_data
