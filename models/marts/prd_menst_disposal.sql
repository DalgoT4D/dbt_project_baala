{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- how women dispose of menstrual material 

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
        menstrual_disposal
    from {{ ref('forms_mapping') }}
    where
        menstrual_disposal is not null
        and menstrual_disposal <> 'N/A'
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
        trim(word) as menstrual_disposal
    from (
        select
            *,
            regexp_split_to_table(menstrual_disposal, '\s+') as word
        from cleaned_source
    )
), 

disposal_category as (
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
            when menstrual_disposal in ('pad_disposal_bin') then 'Pad Disposal Bin'
            when menstrual_disposal in ('drain') then 'Drain'
            when menstrual_disposal in ('toilet') then 'Toilet'
            when menstrual_disposal in ('open_field') then 'Open Field'
            when menstrual_disposal in ('wash_and_throw_in_bag','routine_waste__bag_in_dustbin','routine_waste','d__i_wrap_the_product_and_take', 'b__i_throw_it_in_a_dustbin_aft','a__i_directly_throw_it_in_a_du') then 'Routine Waste'
            when menstrual_disposal in ('incinerator','burn') then 'Burn'
            when menstrual_disposal in ('dig') then 'Dig'
            when menstrual_disposal in ('wash_and_reuse') then 'Reuse'
            when menstrual_disposal in ('others__specify','others','not_responded','f__other','e__there_is_no_place_to_dispos') then 'Others'
        end as disposal_category
    from multiple_choice_exploded
)

select *
from disposal_category
where disposal_category is not null
