{{
    config(
        materialized='table',
        tags=['mart', 'metric']
    )
}}

-- how much women spend monthly on menstrual hygiene

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
        menstrual_spend
    from {{ ref('forms_mapping') }}
    where
        menstrual_spend is not null
        and menstrual_spend <> 'N/A'
),

menstrual_spend_category as (
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
            when menstrual_spend in ('_10___a_month','0') then '0-10 Rupees'
            when menstrual_spend = '_10_30___a_month' then '10-30 Rupees'
            when menstrual_spend = '_30_50__a_month' then '30-50 Rupees'
            when menstrual_spend = '_50_80__a_month' then '50-80 Rupees'
            when menstrual_spend = '_80_100__a_month' then '80-100 Rupees'
            when menstrual_spend = '_100_120___a_month' then '100-120 Rupees'
            when menstrual_spend = '_120_150___a_month' then '120-150 Rupees'
            when menstrual_spend = '_150___a_month' then '>150 Rupees'
            when menstrual_spend = 'do_not_know' then 'Unknown'
        end as menstrual_spend_category
    from cleaned_source
)

select *
from menstrual_spend_category
where menstrual_spend_category is not null
