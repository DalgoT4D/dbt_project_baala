{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- categorization of restrictive traditions suffered by respondents during menstruation

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
        menstrual_tradition
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
        trim(word) as menstrual_tradition
    from (
        select
            *,
            regexp_split_to_table(menstrual_tradition, '\s+') as word
        from cleaned_source
    )
), 

restrictive_tradition_category as (
    select
        *,
        case 
            when menstrual_tradition in ('girls_should_not_go_to_school_during_the','girlsshouldnotgotoschoolduringtheirperio','girls_women_should_not_participate_in_so','girls/womenshouldnotparticipateinsociale','girls_women_should_not_talk_to_boys_men_','girls/womenshouldnottalktoboys/menoranyo','girls_women_should_stay_in_a_separate_ro','girls/womenshouldstayinaseparateroomduri') then 'Social Restrictions'
            when menstrual_tradition in ('girls_women_should_not_cook_or_do_househ','girls/womenshouldnotcookordohouseholdcho','girls_women_should_not_enter_the_kitchen','girls/womenshouldnotenterthekitchen.','girls_women_should_not_exercise','girls/womenshouldnotexercise') then 'Physical/Activity Restrictions'
            when menstrual_tradition in ('girls_women_do_not_wash_their_hair_durin','girls/womendonotwashtheirhairduringperio','girls__women_should_not_wash_their_bodie','girls/womenshouldnotwashtheirbodiesortak','girls_women_sleep_on_the_floor_during_pe','girls/womensleeponthefloorduringperiods') then 'Hygiene Restrictions'
            when menstrual_tradition in ('girls_women_do_not_drink_milk_or_consume','girls/womendonotdrinkmilkorconsumeotherd','girls__women_dont_touch_pickle','girls_women_should_avoid_certain_foods__','girls/womenshouldavoidcertainfoods,likes','girls__women_should_not_eat_ho','girls_women_should_not_touch_pickles') then 'Dietary Restrictions'
            when menstrual_tradition in ('girls_women_can_be_affected_by_evil_spir','girls/womencanbeaffectedbyevilspiritsdur','girls_women_should_not_enter_places_of_w','girls/womenshouldnotenterplacesofworship') then 'Religious/Spiritual Restrictions'
            when menstrual_tradition in ('girls_women_do_not_touch_flowe','girls___women_should_not_touch','girls_women_should_not_touch_fresh_fruit','girls/womenshouldnottouchfreshfruitsandv','girls_women_should_not_touch_fruit_beari','girls_women_should_not_touch_fruit_beari_1','girls/womenshouldnottouchfruitbearingtre') then 'Restrictions on Contact with Objects/Environment'
            when menstrual_tradition in ('none_of_these','noneofthese.') then 'None'
            when menstrual_tradition in ('other') then 'Others'
        end as menst_material_reusability_group
    from multiple_choice_exploded
)

select *
from restrictive_tradition_category
where restrictive_tradition_category is not null
