{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- calculate average age at menarche
-- only ages between 5 and 20 are considered valid
-- if age is given as year of menarche, convert to age by subtracting from current year

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
        cast(firstperiodage as integer) as firstperiodage
    from {{ ref('forms_mapping') }}
    where
        firstperiodage is not null 
        and firstperiodage <> 'N/A'
        and firstperiodage ~ '^[0-9]+$'
),

age_menarche as (
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
            when firstperiodage > 5 and firstperiodage < 20 then firstperiodage
            when firstperiodage between 2020 and 2025 then (age_int - (extract(year from current_date) - firstperiodage))
        end as age_menarche
    from cleaned_source
)

select *
from age_menarche
where age_menarche is not null
