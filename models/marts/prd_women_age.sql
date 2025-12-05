{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- calculate age group percentage composition of respondents

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
        cast(age as integer) as women_age
    from {{ ref('forms_mapping') }}
    where
        age is not null 
        and age <> 'N/A'
        and age ~ '^[0-9]+$'
),

women_age_category as (
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
        marital_status
    from cleaned_source
)

select *
from women_age_category
where age_group is not null
