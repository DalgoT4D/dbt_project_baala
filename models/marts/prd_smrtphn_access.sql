{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- how women access menstrual health info

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
        smartphone_access
    from {{ ref('forms_mapping') }}
    where
        smartphone_access is not null
        and smartphone_access <> 'N/A'
),

smartphone_access_category as (
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
            when smartphone_access in ('yes___i_have_my_own_smartphone','yes___but_it_is_shared_with_other_member') then 'Yes'
            when smartphone_access = 'no___i_do_not_have_access_to_smartphone' then 'No'
        end as smartphone_access_category
    from cleaned_source
)

select *
from smartphone_access_category
where smartphone_access_category is not null
