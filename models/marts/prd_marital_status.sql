{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- calculate marital status percentage composition of respondents

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
        marital_status as women_marital_status
    from {{ ref('forms_mapping') }}
    where
        marital_status is not null 
        and marital_status <> 'N/A'
),

women_marital_status_category as (
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
            when women_marital_status = 'not_married' then 'Not Married'
            when women_marital_status = 'married' or women_marital_status = 'currently_married' then 'Married'
            when women_marital_status = 'separated__divorce' or women_marital_status = 'divorcedseparated' or women_marital_status = 'divorced__separated' then 'Divorced'
            when women_marital_status = 'widow' then 'Widowed'
        end as marital_status_group
    from cleaned_source
)

select *
from women_marital_status_category
where marital_status_group is not null
