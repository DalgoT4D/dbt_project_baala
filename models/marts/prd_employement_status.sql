{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- calculate employment status percentage composition of respondents

with cleaned_source as (
    select
        state_name,
        district_name,
        block_name,
        town_village_name,
        unit_type,
        beneficiary_type,
        place,
        place_type,
        enrollment_type,
        age_group,
        age_int,
        education,
        marital_status, 
        occupation
    from {{ ref('forms_mapping') }}
    where
        occupation is not null 
        and occupation <> 'N/A'
),

women_occupation_category as (
    select
        state_name,
        district_name,
        block_name,
        town_village_name,
        unit_type,
        beneficiary_type,
        place,
        place_type,
        enrollment_type,
        age_group,
        education,
        marital_status,
        case 
            when occupation = 'student__currently_studying' or occupation = 'student' then 'Student'
            when occupation = 'unskilled_manual__janitors__factory_work' then 'Unskilled Manual'
            when occupation = 'skilled_manual__electricians__plumbers__' then 'Skilled Manual'
            when occupation = 'agriculture__farmers__agricultural_worke' then 'Agriculture'
            when occupation = 'sales_and_services__retail_salespersons_' then 'Sales and Services'
            when occupation = 'home_makers' then 'Home Maker'
            when occupation = 'domestic_service__housekeepers__nannies_' then 'Domestic Service'
            when occupation = 'government_workers___asha___nr' or occupation = 'govermental_jobs___ashas' then 'Asha Worker'
            when occupation = 'others' or occupation = 'option_2' or occupation = 'option_1' then 'Others'
        end as occupation_group
    from cleaned_source
)

select *
from women_occupation_category
where occupation_group is not null
