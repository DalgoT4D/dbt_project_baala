{{
    config(
        materialized='table',
        tags=['intermediate', 'survey', 'baseline', 'community']
    )
}}

-- Intermediate model for Ntpc Community 2024 Baseline Survey with business logic and transformations
-- This model applies business logic and transformations on top of the staging layer

with transformed as (
    select
    -- All fields except Airbyte and data quality columns
        {{ 
            dbt_utils.star(
                from=ref('stg_ntpc_community_2024_baseline_survey'), 
                except=[
                    "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta",
                    "has_json_data", "has_submission_time", "has_end_time"
                ]
            ) 
        }}
        
    from {{ ref('stg_ntpc_community_2024_baseline_survey') }}
    
    -- Add any business logic, filtering, or transformations here
    -- For example:
    -- where survey_id is not null
)
{% set rel = ref('stg_ntpc_community_2024_baseline_survey') %}

{% set colname_per = get_column_by_substring_postgres(rel, 'first_menstrual_period') %}
{% set colname_age = get_column_by_substring_postgres(rel, 'Age_of_the_respondent') %}
{% set colname_menst_mat = get_column_by_substring_postgres(rel, 'mixed_material') %}
{% set colname_menst_disp = get_column_by_substring_postgres(rel, 'Ask_the_respondent_ispose_the_materials') %}
{% set colname_pms = get_column_by_substring_postgres(rel, '-') %}
{% set colname_menst_symp = get_column_by_substring_postgres(rel, '-') %}
{% set colname_periodtrad = get_column_by_substring_postgres(rel, 'What_are_some_of_the_y_you_or_your_family') %}
{% set colname_menst_spend = get_column_by_substring_postgres(rel, '-') %}
{% set colname_menst_pred = get_column_by_substring_postgres(rel, 'Ask_the_respondent_struation_will_start') %}
{% set colname_menst_info = get_column_by_substring_postgres(rel, '-') %}
{% set colname_menst_setback = get_column_by_substring_postgres(rel, '-') %}
{% set colname_smrtphn_access = get_column_by_substring_postgres(rel, '-') %}
{% set colname_firstperiod = get_column_by_substring_postgres(rel, 'Ask_the_respondent_w_about_menstruation') %}
{% set colname_menst_anxiety = get_column_by_substring_postgres(rel, '-') %}
{% set colname_religion = get_column_by_substring_postgres(rel, 'What_is_the_respondent_s_relig') %}
{% set colname_caste = get_column_by_substring_postgres(rel, 'What_is_the_respondent_s_caste') %}
{% set colname_occupation = get_column_by_substring_postgres(rel, 'What_is_the_respondent_s_emplo') %}
{% set colname_edu = get_column_by_substring_postgres(rel, 'What_is_the_responde_ion_education_status') %}
{% set colname_ration = get_column_by_substring_postgres(rel, 'What_is_the_responde_s_ration_card_status') %}
{% set colname_marit_status = get_column_by_substring_postgres(rel, 'What_is_the_respondent_s_marital_status') %}
{% set colname_menst_rem = get_column_by_substring_postgres(rel, 'Ask_the_respondent_What_reme') %}
{% set colname_state = get_column_by_substring_postgres(rel, 'state') %}
{% set colname_district = get_column_by_substring_postgres(rel, 'district') %}
{% set colname_block = get_column_by_substring_postgres(rel, 'block') %}
{% set colname_village = get_column_by_substring_postgres(rel, 'village') %}
{% set colname_unit_type = get_column_by_substring_postgres(rel, 'unit') %}

select 
    -- Apply any final transformations or column selections
    {% if colname_per != 'N/A' %}
        {{ colname_per }} as firstperiodage
    {% else %}
        'N/A' as firstperiodage
    {% endif %},
    {% if colname_age != 'N/A' %}
        {{ colname_age }} as age
    {% else %}
        'N/A' as age
    {% endif %},
    {% if colname_menst_mat != 'N/A' %}
        {{ colname_menst_mat }} as menstrual_material
    {% else %}
        'N/A' as menstrual_material
    {% endif %},
    {% if colname_menst_disp != 'N/A' %}
        {{ colname_menst_disp }} as menstrual_disposal
    {% else %}
        'N/A' as menstrual_disposal
    {% endif %},
    {% if colname_pms != 'N/A' %}
        {{ colname_pms }} as menstrual_pms
    {% else %}
        'N/A' as menstrual_pms
    {% endif %},
    {% if colname_menst_symp != 'N/A' %}
        {{ colname_menst_symp }} as menstrual_symptoms
    {% else %}
        'N/A' as menstrual_symptoms
    {% endif %},
    {% if colname_periodtrad != 'N/A' %}
        {{ colname_periodtrad }} as menstrual_tradition
    {% else %}
        'N/A' as menstrual_tradition
    {% endif %},
    {% if colname_menst_spend != 'N/A' %}
        {{ colname_menst_spend }} as menstrual_spend
    {% else %}
        'N/A' as menstrual_spend
    {% endif %},
    {% if colname_menst_pred != 'N/A' %}
        {{ colname_menst_pred }} as menstrual_prediction
    {% else %}
        'N/A' as menstrual_prediction
    {% endif %},
    {% if colname_menst_info != 'N/A' %}
        {{ colname_menst_info }} as menstrual_info
    {% else %}
        'N/A' as menstrual_info
    {% endif %},
    {% if colname_menst_setback != 'N/A' %}
        {{ colname_menst_setback }} as menstrual_setback
    {% else %}
        'N/A' as menstrual_setback
    {% endif %},
    {% if colname_smrtphn_access != 'N/A' %}
        {{ colname_smrtphn_access }} as smartphone_access
    {% else %}
        'N/A' as smartphone_access
    {% endif %},
    {% if colname_firstperiod != 'N/A' %}
        {{ colname_firstperiod }} as firstperiodknowledge
    {% else %}
        'N/A' as firstperiodknowledge
    {% endif %},
    {% if colname_menst_anxiety != 'N/A' %}
        {{ colname_menst_anxiety }} as menstrual_anxiety
    {% else %}
        'N/A' as menstrual_anxiety
    {% endif %},
    {% if colname_religion != 'N/A' %}
        {{ colname_religion }} as religion
    {% else %}
        'N/A' as religion
    {% endif %},
    {% if colname_caste != 'N/A' %}
        {{ colname_caste }} as caste
    {% else %}
        'N/A' as caste
    {% endif %},
    {% if colname_occupation != 'N/A' %}
        {{ colname_occupation }} as occupation
    {% else %}
        'N/A' as occupation
    {% endif %},
    {% if colname_edu != 'N/A' %}
        {{ colname_edu }} as education
    {% else %}
        'N/A' as education
    {% endif %},
    {% if colname_ration != 'N/A' %}
        {{ colname_ration }} as ration_card
    {% else %}
        'N/A' as ration_card
    {% endif %},
    {% if colname_marit_status != 'N/A' %}
        {{ colname_marit_status }} as marital_status
    {% else %}
        'N/A' as marital_status
    {% endif %},
    {% if colname_menst_rem != 'N/A' %}
        {{ colname_menst_rem }} as menstrual_remedies
    {% else %}
        'N/A' as menstrual_remedies
    {% endif %},
    {% if colname_state != 'N/A' %}
        {{ colname_state }} as state_name
    {% else %}
        'N/A' as state_name
    {% endif %},
    {% if colname_district != 'N/A' %}
        {{ colname_district }} as district_name
    {% else %}
        'N/A' as district_name
    {% endif %},
    {% if colname_block != 'N/A' %}
        {{ colname_block }} as block_name
    {% else %}
        'N/A' as block_name
    {% endif %},
    {% if colname_village != 'N/A' %}
        {{ colname_village }} as village_name
    {% else %}
        'N/A' as village_name
    {% endif %},
    {% if colname_unit_type != 'N/A' %}
        {{ colname_unit_type }} as unit_type
    {% else %}
        'N/A' as unit_type
    {% endif %}

from transformed
