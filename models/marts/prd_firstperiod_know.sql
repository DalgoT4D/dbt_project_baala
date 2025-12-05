{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- women who had known about menstruation before first period

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
        firstperiodknowledge
    from {{ ref('forms_mapping') }}
    where
        firstperiodknowledge is not null
        and firstperiodknowledge <> 'N/A'
),

firstperiodknowledge as (
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
            when firstperiodknowledge = 'yes' then 'Yes'
            when firstperiodknowledge = 'no' then 'No'
        end as firstperiodknowledge
    from cleaned_source
)

select *
from firstperiodknowledge
where firstperiodknowledge is not null
