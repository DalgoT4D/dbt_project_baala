{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- women can predict their menstrual cycle

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
        menstrual_prediction
    from {{ ref('forms_mapping') }}
    where
        menstrual_prediction is not null
        and menstrual_prediction <> 'N/A'
),

menstrual_prediction_category as (
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
            when menstrual_prediction in ('yes__other','yes, i keep a calendar/track', 'yes__i_keep_a_calendar_track', 'yes, my body tells me', 'yes, other', 'yes__i_am_on_oral_contraceptives_and_i_k','yes__my_body_tells_me','yes, i am on oral contraceptives when my period will begin') then 'Yes'
            when menstrual_prediction in ('no__i_just_know_it_comes_every_month','no , i just know it comes every month','no__i_don_t_know_when_it_will_start','no, i don''t know when it will start','no , my mother keeps track') then 'No'
            when menstrual_prediction in ('no___menopause_sterilize', 'no___menopause_sterilized') then 'Menopause/Sterilized'
            when menstrual_prediction in ('not_responded') then 'Not Responded'
        end as menstrual_prediction_category
    from cleaned_source
)

select *
from menstrual_prediction_category
where menstrual_prediction_category is not null
