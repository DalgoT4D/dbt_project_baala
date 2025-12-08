{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- how women remedy period pain

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
        menstrual_remedies
    from {{ ref('forms_mapping') }}
    where
        menstrual_remedies is not null
        and menstrual_remedies <> 'N/A'
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
        trim(word) as menstrual_remedies
    from (
        select
            *,
            regexp_split_to_table(menstrual_remedies, '\s+') as word
        from cleaned_source
    )
), 

menstrual_remedies_category as (
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
            when menstrual_remedies in ('took_medication_pain_relievers','medication__pain_relievers') then 'Took medication/pain relievers'
            when menstrual_remedies in ('used_a_heat_pack_or_hot_water_bottle') then 'Used a heat pack or hot water bottle'
            when menstrual_remedies in ('used_an_ice_pack','heating_pads___ice_packs') then 'Used an Heat/Ice pack'
            when menstrual_remedies in ('used_traditional_remedies_herbs','home_remedies_such_as_massage_oils___tea') then 'Used traditional remedies/herbs'
            when menstrual_remedies in ('skip_routine_activities','rested_took_a_break','rest','') then 'Rested/took a break'
            when menstrual_remedies in ('yoga__meditation','exercise','did_stretches_exercises') then 'Did stretches/exercises'
            when menstrual_remedies in ('hot_tea_liquids') then 'Hot tea/liquids'
            when menstrual_remedies in ('changed_diet') then 'Changed diet'
            when menstrual_remedies in ('did_nothing_despite_experiencing_pain','did_nothing','continue_with_routine_activities') then 'Did nothing despite experiencing pain'
            when menstrual_remedies in ('prayers','others___specify','other') then 'Other'
            when menstrual_remedies in ('not_applicable_did_not_experience_pain_o','i_had_no_pain') then 'Not applicable/did not experience pain or discomfort'
        end as menstrual_remedies_category
    from multiple_choice_exploded
)

select *
from menstrual_remedies_category
where menstrual_remedies_category is not null
