{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- calculate education status percentage composition of respondents

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
        education as women_education
    from {{ ref('forms_mapping') }}
    where
        education is not null 
        and education <> 'N/A'
),

women_education_category as (
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
            when women_education = 'not_working' then 'Not Working'
            when women_education = 'graduate' then 'Graduate'
            when women_education = 'higher_secondary' then 'Higher Secondary'
            when women_education = 'post_graduate' then 'Post Graduate'
        end as education_group
    from cleaned_source
)

select *
from women_education_category
where education_group is not null
