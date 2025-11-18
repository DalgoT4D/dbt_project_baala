{{
    config(
        materialized='table',
        tags=['staging', 'combined']
    )
}}

-- statging model to clean forms_mapping data

with source as (
    select 
        nullif(btrim(forms), '') as form_name,
        nullif(btrim(state), '') as state_name,
        nullif(btrim(district), '') as district_name,
        nullif(btrim(sub_district), '') as sub_district_name,
        nullif(btrim(block), '') as block_name,
        nullif(btrim(town_village), '') as town_village_name,
        nullif(btrim(unit_type), '') as unit_type,
        nullif(btrim(beneficiary_type), '') as beneficiary_type,
        nullif(btrim(place), '') as place,
        nullif(btrim(if_school_then_school_type), '') as school_type,
        nullif(btrim(if_school_then_enrollment_type), '') as enrollment_type,
        nullif(btrim(if_community_then_community_type), '') as community_type,
        nullif(btrim(respondent_type), '') as respondent_type
    from {{ source('forms_mapping_data', 'forms_mapping_sheet') }}
),

cleaned as (
    select  
        form_name,
        state_name,
        district_name,
        sub_district_name,
        block_name,
        town_village_name,
        unit_type,
        beneficiary_type,
        place,
        case when place = 'School' then school_type else community_type end as place_type,
        case when place = 'School' then enrollment_type else 'N/A' end as enrollment_type,
        respondent_type

    from source
)

select *
from cleaned
where
    form_name is not NULL
    and form_name != 'NA'
