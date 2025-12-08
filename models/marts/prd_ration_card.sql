{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- calculate ration card category percentage composition of respondents

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
        ration_card
    from {{ ref('forms_mapping') }}
    where
        ration_card is not null
        and ration_card <> 'N/A'
),

ration_card_category as (
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
            when ration_card = 'apl' then 'APL (White)'
            when ration_card = 'no_rc' then 'No RC'
            when ration_card = 'bpl' then 'BPL (Pink)'
            when ration_card in ('most_vulnerable___yellow_','most_vulnerable___yellow') then 'Most Vulnerable (Yellow)'
            when ration_card = 'other' then 'Others'
            when ration_card = 'don_t_know' then 'Donot Know'
        end as ration_card_group
    from cleaned_source
)

select *
from ration_card_category
where ration_card_group is not null
