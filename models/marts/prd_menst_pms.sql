{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- period related PMS suffered

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
        menstrual_pms
    from {{ ref('forms_mapping') }}
    where
        menstrual_pms is not null
        and menstrual_pms <> 'N/A'
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
        trim(word) as menstrual_pms
    from (
        select
            *,
            regexp_split_to_table(menstrual_pms, '\s+') as word
        from cleaned_source
    )
), 

menstrual_pms_category as (
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
            when menstrual_pms in ('social_withdrawal','mood_swings','irritability','depression','anxiety','angry_outbursts') then 'Emotional & Psychological Symptoms'
            when menstrual_pms in ('weakness','poor_concentration','fainting__light_headedness') then 'Cognitive Symptoms'
            when menstrual_pms in ('swelling_of_arms_and_legs','stomach_cramps','skin_problems_like_acne','nausea','leg_pain___knee_pain','headache','gastrointestinal_problems','breast_tenderness','body_pain','backache','abdominal_swelling') then 'Physical Symptoms'
            when menstrual_pms in ('changes_in_sleep') then 'Sleep-Related Symptoms'
            when menstrual_pms in ('i_dont_experience_pms','menopause') then 'Menopause/No PMS'
            when menstrual_pms in ('others') then 'Others'
        end as menstrual_pms_category
    from multiple_choice_exploded
)

select *
from menstrual_pms_category
where menstrual_pms_category is not null
