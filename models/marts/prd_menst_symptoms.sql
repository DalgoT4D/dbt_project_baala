{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- period related symptoms suffered

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
        menstrual_symptoms
    from {{ ref('forms_mapping') }}
    where
        menstrual_symptoms is not null
        and menstrual_symptoms <> 'N/A'
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
        trim(word) as menstrual_symptoms
    from (
        select
            *,
            regexp_split_to_table(menstrual_symptoms, '\s+') as word
        from cleaned_source
    )
), 

menstrual_symptoms_category as (
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
            when menstrual_symptoms in ('mood_swings','irritability_angry_outbursts','depression__anxiety___mood_swings') then 'Emotional & Psychological Symptoms'
            when menstrual_symptoms in ('') then 'Cognitive Symptoms'
            when menstrual_symptoms in ('scanty_bleeding','nausea','menstrual_cramps__manageable','leg_pain','lack_of_appetite','knee_pain','irregular_menstruation','heavy_bleeding','headache','gastrointestinal_problems','fatigue','breast_tenderness','body_pain','backache','Body_pain/_Backache_/_Headache/_leg_pain') then 'Physical Symptoms'
            when menstrual_symptoms in ('changes_in_sleep') then 'Sleep-Related Symptoms'
            when menstrual_symptoms in ('no_symptoms') then 'Menopause/No Symptoms'
            when menstrual_symptoms in ('others_1','others') then 'Others'
        end as menstrual_symptoms_category
    from multiple_choice_exploded
)

select *
from menstrual_symptoms_category
where menstrual_symptoms_category is not null
