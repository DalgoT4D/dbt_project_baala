{{
    config(
        materialized='table',
        tags=['intermediate', 'survey', 'baseline', 'community']
    )
}}

-- Intermediate model for Mosonie Irctc Community 2025 Baseline Survey with business logic and transformations
-- This model applies business logic and transformations on top of the staging layer

with transformed as (
    select
        -- All fields except Airbyte and data quality columns
        {{ 
            dbt_utils.star(
                from=ref('stg_mosonie_irctc_community_2025_baseline_survey'), 
                except=[
                    "_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta",
                    "has_json_data", "has_submission_time", "has_end_time"
                ]
            ) 
        }}
        
    from {{ ref('stg_mosonie_irctc_community_2025_baseline_survey') }}
    
    -- Add any business logic, filtering, or transformations here
    -- For example:
    -- where survey_id is not null
)
{% set rel = ref('stg_mosonie_irctc_community_2025_baseline_survey') %}

{% set colname_per = get_column_by_substring_postgres(rel, 'woman_ageperiod') %}
{% set colname_age = get_column_by_substring_postgres(rel, 'woman_age') %}
{% set colname_menst_mat = get_column_by_substring_postgres(rel, 'What_is_the_main_absorbent_mat_001') %}
{% set colname_menst_disp = get_column_by_substring_postgres(rel, 'woman_dispose') %}
{% set colname_pms = get_column_by_substring_postgres(rel, '-') %}
{% set colname_menst_symp = get_column_by_substring_postgres(rel, 'What_symptoms_do_you_experienc') %}
{% set colname_periodtrad = get_column_by_substring_postgres(rel, 'What_are_some_of_the_tradition') %}
{% set colname_menst_spend = get_column_by_substring_postgres(rel, 'On_average_how_much_money_do_') %}
{% set colname_menst_pred = get_column_by_substring_postgres(rel, 'Can_you_usually_predict_when_m') %}
{% set colname_menst_info = get_column_by_substring_postgres(rel, '-') %}
{% set colname_menst_setback = get_column_by_substring_postgres(rel, '-') %}
{% set colname_smrtphn_access = get_column_by_substring_postgres(rel, '-') %}
{% set colname_firstperiod = get_column_by_substring_postgres(rel, '-') %}
{% set colname_menst_anxiety = get_column_by_substring_postgres(rel, '-') %}
{% set colname_religion = get_column_by_substring_postgres(rel, '-') %}
{% set colname_caste = get_column_by_substring_postgres(rel, '-') %}
{% set colname_occupation = get_column_by_substring_postgres(rel, '-') %}
{% set colname_edu = get_column_by_substring_postgres(rel, '-') %}
{% set colname_ration = get_column_by_substring_postgres(rel, '-') %}
{% set colname_marit_status = get_column_by_substring_postgres(rel, '-') %}
{% set colname_menst_rem = get_column_by_substring_postgres(rel, '-') %}

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
    {% endif %}

from transformed
