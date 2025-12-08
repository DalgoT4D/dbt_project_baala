{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- how women get information about menstrual health

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
        menstrual_info
    from {{ ref('forms_mapping') }}
    where
        menstrual_info is not null
        and menstrual_info <> 'N/A'
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
        trim(word) as menstrual_info
    from (
        select
            *,
            regexp_split_to_table(menstrual_info, '\s+') as word
        from cleaned_source
    )
), 

mmenstrual_info_category as (
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
            when menstrual_info = 'friends_peers' then 'Friends'
            when menstrual_info = 'healthcare_professionals' then 'Healthcare Professionals'
            when menstrual_info = 'i_haven_t_received_much_information_on_t' then 'Do Not Receive Info'
            when menstrual_info = 'parents_guardians' then 'Parents'
            when menstrual_info = 'school_workshops' then 'School Workshop'
            when menstrual_info = 'social_media' then 'Social Media'
            when menstrual_info = 'teachers' then 'School Teachers'
        end as menstrual_info_category
    from multiple_choice_exploded
)

select *
from mmenstrual_info_category
where menstrual_info_category is not null
