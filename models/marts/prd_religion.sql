{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- calculate religion percentage composition of respondents

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
        religion
    from {{ ref('forms_mapping') }}
    where
        religion is not null
        and religion <> 'N/A'
),

religion_category as (
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
            when religion = 'hindu' then 'Hindu'
            when religion = 'muslim' then 'Muslim'
            when religion in ('christian','christianity') then 'Christian'
            when religion = 'sikh' then 'Sikh'
            when religion in ('buddhist','neo__buddhism','neo___buddhism') then 'Buddhist'
            when religion = 'jain' then 'Jain'
            when religion = 'parsi' then 'Parsi'
            when religion in ('others','option_2','option_1') then 'Others'
            when religion = 'no_response' then 'Not Specified'
        end as religion_group
    from cleaned_source
)

select *
from religion_category
where religion_group is not null
