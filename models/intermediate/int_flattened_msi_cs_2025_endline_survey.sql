{{
    config(
        materialized='table',
        tags=['intermediate', 'flattened', 'survey', 'endline', 'women_health']
    )
}}

-- Flattened intermediate model for msi_cs_2025_endline_survey
-- This model flattens the JSONB data from the staging layer into individual columns
-- Focuses on women's health and menstruation survey data

with flattened as (
    select
        -- Standard metadata fields
        _id::integer as survey_id,
        
        -- Text fields (categorical responses)
        data->>'State' as state,
        data->>'woman_uw' as woman_underweight,
        data->>'perioddef' as period_definition,
        data->>'periodmen' as period_mentruation,
        data->>'woman_lmp' as woman_last_menstrual_period,
        data->>'woman_pms' as woman_pms_symptoms,
        data->>'periodfood' as period_food_preferences,
        data->>'periodhinfo' as period_health_info,
        data->>'pms_routine' as pms_routine,
        data->>'woman_caste' as woman_caste,
        data->>'woman_clean' as woman_cleanliness_practices,
        data->>'woman_knowm' as woman_knowledge_menstruation,
        data->>'woman_change' as woman_pad_change_frequency,
        data->>'woman_mblood' as woman_menstrual_blood_flow,
        data->>'Name_of_block' as name_of_block,
        data->>'periodmatinfo' as period_material_info,
        data->>'woman_mc_preg' as woman_menstrual_cycle_pregnancy,
        data->>'woman_mcdur_kn' as woman_menstrual_cycle_duration_knowledge,
        data->>'Name_of_village' as name_of_village,
        data->>'period_pain_rem' as period_pain_relief,
        data->>'Name_of_district' as name_of_district,
        data->>'Name_of_surveyor' as name_of_surveyor,
        data->>'woman_puberty_kn' as woman_puberty_knowledge,
        data->>'Name_of_the_woman' as name_of_the_woman,
        data->>'Place_of_interview' as place_of_interview,
        data->>'Surveyor_s_comments' as surveyor_comments,
        data->>'woman_material_space' as woman_material_storage_space,
        data->>'Woman_s_marital_status' as woman_marital_status,
        data->>'What_is_the_woman_s_religion' as woman_religion,
        data->>'_4_2_Did_you_use_the_pads_you' as did_use_pads,
        data->>'How_do_you_wash_the_Baala_pads' as how_wash_baala_pads,
        data->>'What_are_some_of_the_tradition' as traditional_practices,
        data->>'What_is_the_main_absorbent_mat' as main_absorbent_material,
        data->>'What_is_the_respondent_s_emplo' as respondent_employment,
        data->>'What_symptoms_do_you_experienc' as menstrual_symptoms,
        data->>'_10_What_are_some_of_the_chang' as changes_experienced,
        data->>'_3_1_What_do_you_remember_lear' as workshop_learning_remembered,
        data->>'Do_you_have_access_to_smartphone' as smartphone_access,
        data->>'Should_menstruation_be_kept_a_secret' as menstruation_secret_attitude,
        data->>'Ask_the_respondent_th_worker_or_doctor' as ask_health_worker_doctor,
        data->>'Ask_the_respondent_ispose_the_materials' as material_disposal_method,
        data->>'After_the_birth_of_a_period_has_returned' as period_returned_after_birth,
        data->>'Did_you_face_any_of_while_using_the_pads' as pad_usage_challenges,
        data->>'Do_you_know_what_san_pads_are_made_up_of' as sanitary_pad_material_knowledge,
        data->>'Do_you_think_3_pads_t_are_enough_for_you' as pad_quantity_sufficiency,
        data->>'Can_you_usually_pred_struation_will_start' as period_prediction_ability,
        data->>'Do_you_feel_embarras_y_menstrual_products' as menstrual_product_embarrassment,
        data->>'How_many_main_stages_er_reproductive_life' as reproductive_life_stages_knowledge,
        data->>'How_often_do_you_fee_t_on_your_daily_life' as daily_life_impact_frequency,
        data->>'Thank_you_for_consid_olidarity_Initiative' as solidarity_initiative_response,
        data->>'What_is_the_responde_t_s_education_status' as respondent_education_status,
        
        -- Numeric fields (ages, counts, spending)
        case 
            when data->>'woman_age' ~ '^[0-9]+$' then (data->>'woman_age')::integer 
            else null 
        end as woman_age,
        
        case 
            when data->>'woman_members' ~ '^[0-9]+$' then (data->>'woman_members')::integer 
            else null 
        end as woman_family_members,
        
        case 
            when data->>'woman_ageperiod' ~ '^[0-9]+$' then (data->>'woman_ageperiod')::integer 
            else null 
        end as woman_age_at_first_period,
        
        case 
            when data->>'If_yes_for_how_many_period_c' ~ '^[0-9]+$' then (data->>'If_yes_for_how_many_period_c')::integer 
            else null 
        end as periods_used_count,
        
        case 
            when data->>'On_average_how_much_g_a_sanitary_product' ~ '^[0-9]+(\.[0-9]+)?$' then (data->>'On_average_how_much_g_a_sanitary_product')::decimal 
            else null 
        end as sanitary_product_spending,
        
        -- Date fields
        case 
            when data->>'Date_of_visit' ~ '^\d{4}-\d{2}-\d{2}$' then (data->>'Date_of_visit')::date 
            else null 
        end as date_of_visit,
        
        -- Age range fields (categorical)
        data->>'menopause_age' as menopause_age

    from {{ ref('stg_msi_cs_2025_endline_survey') }}
    where data is not null
)

select * from flattened
