{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- women who struggle with mental health before/during their periods

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
        menstrual_anxiety
    from {{ ref('forms_mapping') }}
    where
        menstrual_anxiety is not null
        and menstrual_anxiety <> 'N/A'
),

mental_health_category as (
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
            when menstrual_anxiety = 'not_at_all' then 'Not at all'
            when menstrual_anxiety = 'rarely' then 'Rarely'
            when menstrual_anxiety = 'sometimes' then 'Sometimes'
            when menstrual_anxiety = 'often' then 'Often'
            when menstrual_anxiety = 'always' then 'Always'
        end as mental_health_group
    from cleaned_source
)

select *
from mental_health_category
where mental_health_group is not null
