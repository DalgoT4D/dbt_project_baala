{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- calculate caste percentage composition of respondents

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
        caste
    from {{ ref('forms_mapping') }}
    where
        caste is not null
        and caste <> 'N/A'
),

caste_category as (
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
            when caste in ('general__open','general','ur') then 'Open/General'
            when caste = 'obc' then 'Other Backward Class'
            when caste = 'sc' then 'Scheduled Caste'
            when caste = 'st' then 'Scheduled Tribe'
            when caste = 'dnt' then 'Denotified Tribe'
            when caste = 'ut' then 'UT'
            when caste = 'others' then 'Others'
            when caste = 'not_specified' then 'Not Specified'
        end as caste_group
    from cleaned_source
)

select *
from caste_category
where caste_group is not null
