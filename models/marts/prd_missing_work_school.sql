{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- how many girls/women missing school/work due to menstrual

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
        respondent_type,
        menstrual_setback
    from {{ ref('forms_mapping') }}
    where
        menstrual_setback is not null
        and menstrual_setback <> 'N/A'
),

--rows with space-sperated multiple choices are exploded into individual rows
multiple_choice_exploded as (
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
        respondent_type,
        trim(word) as menstrual_setback
    from (
        select
            *,
            regexp_split_to_table(menstrual_setback, '\s+') as word
        from cleaned_source
    )
), 

menstrual_setback_category as (
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
        respondent_type,
        case 
            when respondent_type in ('Women','Mixed') and menstrual_setback in ('always','sometimes','attending_paid_work','yes','attending_school','often') then 'Missed Paid Work'
            when respondent_type = 'Girls' and menstrual_setback in ('yes','sometimes','attending_school') then 'Missed Attending School'
            when menstrual_setback in ('participating_in_religious_activities','option_2','participating_in_social_activities','religious_activities','cooking_food','eating_with_others','bathing_in_regular_places') then 'Missed Other Activities'
            when menstrual_setback in ('no','rarely','not_at_all','none_of_the_activities') then 'No Effects'
        end as menstrual_setback_category
    from multiple_choice_exploded
)

select *
from menstrual_setback_category
where menstrual_setback_category is not null
