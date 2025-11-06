{{
    config(
        materialized='table',
        tags=['mart', 'combined']
    )
}}

-- mart model to apply forms_mapping logic to all_forms_combined table
-- business logic to only preserve forms of interest and append extra columns for metrics calculation

with source as (
    select *, 
        case when age <> 'N/A' and age ~ '^[0-9]+$' then cast(age as integer) else null end as age_int
    from {{ ref('all_forms_combined') }}
),

forms_mapping as (
    select *
    from {{ ref('stg_forms_mapping') }}
),

forms_mapped as (
    SELECT
        s.form_name,
        s.firstperiodage,
        s.age,
        s.age_int,
        case 
            when s.age_int is null then 'N/A'
            when s.age_int >= 7 and s.age_int <= 9 then '7-9 years'
            when s.age_int >= 10 and s.age_int <= 14 then '10-14 years'
            when s.age_int >= 15 and s.age_int <= 24 then '15-24 years'
            when s.age_int >= 25 and s.age_int <= 34 then '25-34 years'
            when s.age_int >= 35 and s.age_int <= 44 then '35-44 years'
            when s.age_int >= 45 and s.age_int <= 54 then '45-54 years'
            when s.age_int >= 55 then '55 and above'
        end as age_group,
        s.menstrual_material,
        s.menstrual_disposal,
        s.menstrual_pms,
        s.menstrual_symptoms,
        s.menstrual_tradition,
        s.menstrual_spend,
        s.menstrual_prediction,
        s.menstrual_info,
        s.menstrual_setback,
        s.smartphone_access,
        s.firstperiodknowledge,
        s.menstrual_anxiety,
        s.religion,
        s.caste,
        s.occupation,
        s.education,
        s.ration_card,
        s.marital_status,
        s.menstrual_remedies,
        CASE
            when f.state_name is not null then f.state_name
            else s.state_name
        END as state_name,
        CASE
            when f.district_name is not null then f.district_name
            else s.district_name
        END as district_name,
        CASE
            when f.block_name is not null then f.block_name
            else s.block_name
        END as block_name,
        f.sub_district_name,
        CASE
            when f.town_village_name is not null then f.town_village_name
            else s.village_name
        END as town_village_name,
        CASE
            when f.unit_type is not null then f.unit_type
            else s.unit_type
        END as unit_type,
        f.beneficiary_type,
        f.place,
        f.place_type,
        f.enrollment_type

    from source s inner join forms_mapping f
    on s.form_name = f.form_name
)

select *
from forms_mapped
where form_name is not null