{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- categorize reusability of menstrual material used by respondents

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
        menstrual_material
    from {{ ref('forms_mapping') }}
    where
        menstrual_material is not null
        and menstrual_material <> 'N/A'
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
        trim(word) as menstrual_material
    from (
        select
            *,
            regexp_split_to_table(menstrual_material, '\s+') as word
        from cleaned_source
    )
), 

menst_material_reusability_category as (
    select
        *,
        case 
            when menstrual_material in ('a__disposable_sanitary_napkins','disposable_sanitary_pads_such_as_whisper', 'disposableclothpads', 'disposablesanitarynapkins', 'i_exclusively_use_cotton__wool', 'i_exclusively_use_disposable_sanitary_pa', 'i_exclusively_use_sand', 'i_exclusively_use_tampon', 'i_exclusively_use_toilet_paper', 'i_use_baala_pads', 'sand', 'tampon', 'tampons') then 'Disposable Products'
            when menstrual_material in ('b__cloth','baala_cloth_pads','c__cloth_pads','cloth','i_exclusively_use_cloth', 'i_exclusively_use_menstrual_cup', 'i_exclusively_use_reusable_sanitary_clot', 'i_exclusively_use_usual_underwear', 'i_use_combination_of_cloth_and_disposabl', 'i_use_combination_of_cloth_and_reusable_', 'menstrualcup', 'reusable_sanitary_pads', 'reusableclothpads', 'underwear') then 'Reusable  Products'
            when menstrual_material in ('i__other', 'didnotanswer', 'not_responded', 'other', 'others', 'please_specify___if_i_use_a_combination_', 'please_specify___if_i_use_a_co') then 'Others/Did not answer'
            else 'incorrect choice'
        end as menst_material_reusability_group
    from multiple_choice_exploded
),

menst_material_safety_category as (
    select
        *,
        case 
            when menstrual_material in ('a__disposable_sanitary_napkins', 'baala_cloth_pads', 'c__cloth_pads', 'disposable_sanitary_pads_such_as_whisper', 'disposableclothpads', 'disposablesanitarynapkins', 'f__period_panty', 'g__period_disc', 'i_exclusively_use_disposable_sanitary_pa', 'i_exclusively_use_menstrual_cup', 'i_exclusively_use_period_panties', 'i_exclusively_use_reusable_sanitary_clot', 'i_exclusively_use_tampon', 'i_use_baala_pads', 'menstrualcup', 'perioddisc', 'periodpanty', 'reusable_sanitary_pads', 'reusableclothpads', 'tampon', 'tampons') then 'Safe Products'
            when menstrual_material in ('b__cloth', 'cloth', 'i_exclusively_use_cloth', 'i_exclusively_use_cotton__wool', 'i_exclusively_use_sand', 'i_exclusively_use_toilet_paper', 'i_exclusively_use_usual_underwear', 'i_use_combination_of_cloth_and_disposabl', 'i_use_combination_of_cloth_and_reusable_', 'sand', 'underwear') then 'Potentially Unsafe Products'
            when menstrual_material in ('i__other', 'didnotanswer', 'not_responded', 'other', 'others', 'please_specify___if_i_use_a_combination_', 'please_specify___if_i_use_a_co', '') then 'Others/Did not answer'
            else 'incorrect choice'
        end as menst_material_safety_group
    from multiple_choice_exploded
),

menst_material_env_category as (
    select
        *,
        case 
            when menstrual_material in ('a__disposable_sanitary_napkins','disposable_sanitary_pads_such_as_whisper','disposableclothpads','disposablesanitarynapkins','i_exclusively_use_disposable_sanitary_pa','i_exclusively_use_tampon') then 'High Impact'
            when menstrual_material in ('baala_cloth_pads','c__cloth_pads','i_exclusively_use_menstrual_cup','i_exclusively_use_reusable_sanitary_clot','i_use_baala_pads','menstrualcup') then 'Low Impact'
            when menstrual_material in ('b__cloth','cloth','i_exclusively_use_cloth','i_exclusively_use_cotton__wool','i_exclusively_use_sand','i_exclusively_use_toilet_paper','i_exclusively_use_usual_underwear','i_use_combination_of_cloth_and_disposabl','i_use_combination_of_cloth_and_reusable_') then 'Variable due to safety'
            when menstrual_material in ('i__other', 'didnotanswer', 'not_responded', 'other', 'others', 'please_specify___if_i_use_a_combination_', 'please_specify___if_i_use_a_co') then 'Others/Did not answer'
            else 'incorrect choice'
        end as menst_material_env_group
    from multiple_choice_exploded
)

--final select with union of all three categories
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
    classification,
    category
from (
    (
        select
            *,
            'Reusability' as classification,
            menst_material_reusability_group as category
        from menst_material_reusability_category
    )
    union all
    (
        select
            *,
            'Safety' as classification,
            menst_material_safety_group as category
        from menst_material_safety_category
    )
    union all
    (
        select
            *,
            'Environmental Impact' as classification,
            menst_material_env_group as category
        from menst_material_env_category
    )
) as s
