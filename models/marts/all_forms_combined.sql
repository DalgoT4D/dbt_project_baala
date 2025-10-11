{{
    config(
        materialized='table',
        tags=['mart', 'combined']
    )
}}

-- mart model to combine all intermediate survey models into one comprehensive table
-- This model applies business logic on top of the intermediate layer

with barula_community_2025_baseline_survey as (
    select
        'Baruala Community 2025 baseline survey' as form_name,
        * 
    from {{ ref('int_barula_community_2025_baseline_survey') }}
),

baseline_questionnaire_bareilly as (
    select
        'Baseline Questionnaire Bareilly' as form_name,
        * 
    from {{ ref('int_baseline_questionnaire_bareilly') }}
),

bir_community_baseline2025 as (
    select
        'Bir Community Baseline_2025' as form_name,
        * 
    from {{ ref('int_bir_community_baseline2025') }}
),

caf_ajmer_2024_baselineendline as (
    select
        'CAF Ajmer 2024 baseline/endline survey' as form_name,
        *
    from {{ ref('int_caf_ajmer_2024_baselineendline_survey') }}
),

caf_ajmer_students_2025_baseline as (
    select
        'CAF Ajmer students 2025 baseline survey' as form_name,
        * 
    from {{ ref('int_caf_ajmer_students_2025_baseline_survey') }}
),

caf_delhi_school_2024_baseline as (
    select
        'CAF Delhi School 2024 baseline survey' as form_name,
        *
    from {{ ref('int_caf_delhi_school_2024_baseline_survey') }}
),

caf_delhi_school_2025_endline as (
    select
        'CAF Delhi School 2025 endline survey' as form_name,
        *
    from {{ ref('int_caf_delhi_school_2025_endline_survey') }}
),

elephanta_community_2025_baseline as (
    select
        'Elephanta Community 2025 baseline survey' as form_name,
        *
    from {{ ref('int_elephanta_community_2025_baseline_survey') }}
),

elephanta_community_2025_endline_survey as (
    select
        'Elephanta Community 2025 endline survey' as form_name,
        *
    from {{ ref('int_elephanta_community_2025_endline_survey') }}
),

fbc_community_2024_baseline as (
    select
        'FBC Community 2024 baseline survey' as form_name,
        *
    from {{ ref('int_fbc_community_2024_baseline_survey') }}
),

fbc_school_boys_2024_baseline as (
    select
        'FBC School Boys 2024 baseline survey' as form_name,
        *
    from {{ ref('int_fbc_school_boys_2024_baseline_survey') }}
),

fbc_school_boys_2024_endline as (
    select
        'FBC School Boys 2024 endline survey' as form_name,
        *
    from {{ ref('int_fbc_school_boys_2024_endline_survey') }}
),

fbc_school_girls_2024_baseline as (
    select
        'FBC School Girls 2024 baseline survey' as form_name,
        *
    from {{ ref('int_fbc_school_girls_2024_baseline_survey') }}
),

fbc_school_girls_2024_endline as (
    select
        'FBC School Girls 2024 endline survey' as form_name,
        *
    from {{ ref('int_fbc_school_girls_2024_endline_survey') }}
),

fbc_women_jhatingri_baseline as (
    select
        'FBC Women Jhatingri Baseline Questionaire' as form_name,
        *
    from {{ ref('int_fbc_women_jhatingri_baseline_questionaire') }}
),

ffec_community_2025_baseline as (
    select
        'FFEC Community 2025 Baseline survey' as form_name,
        *
    from {{ ref('int_ffec_community_2025_baseline_survey') }}
),

jakson_cc_trainers_2025 as (
    select
        'Jakson CC trainers 2025 survey' as form_name,
        *
    from {{ ref('int_jakson_cc_trainers_2025_survey') }}
),

jakson_community_2024_baseline as (
    select
        'Jakson Community 2024 baseline survey' as form_name,
        *
    from {{ ref('int_jakson_community_2024_baseline_survey') }}
),

jakson_community_2025_endline as (
    select
        'Jakson community 2025 endline survey' as form_name,
        *
    from {{ ref('int_jakson_community_2025_endline_survey') }}
),

jk_fenner_hyd_2025_baseline as (
    select
        'JK Fenner Hyd 2025 baseline survey' as form_name,
        *
    from {{ ref('int_jk_fenner_hyd_2025_baseline_survey') }}
),

jk_fenner_hyd_2025_phase_2_baseline_survey as (
    select
        'JK Fenner Hyd 2025 phase 2baseline survey' as form_name,
        *
    from {{ ref('int_jk_fenner_hyd_2025_phase_2_baseline_survey') }}
),

kokari_2024_baseline as (
    select
        'Kokari 2024 baseline survey' as form_name,
        *
    from {{ ref('int_kokari_2024_baseline_survey') }}
),

kokari_community_2025_baseline as (
    select
        'Kokari Community 2025 baseline survey' as form_name,
        *
    from {{ ref('int_kokari_community_2025_baseline_survey') }}
),

kokari_community_2025_endline as (
    select
        'Kokari Community 2025 endline survey' as form_name,
        *
    from {{ ref('int_kokari_community_2025_endline_survey') }}
),

majuli_needs_assessment as (
    select
        'Majuli Needs Assessment' as form_name,
        *
    from {{ ref('int_majuli_needs_assessment') }}
),

mamta_2025_endline as (
    select
        'Mamta 2025 endline survey' as form_name,
        *
    from {{ ref('int_mamta_2025_endline_survey') }}
),

mamta_school_2023_workshop_long as (
    select
        'Mamta School 2023 Workshop Long Survey' as form_name,
        *
    from {{ ref('int_mamta_school_2023_workshop_long_survey') }}
),

mamta_school_2023_workshop_short_ece as (
    select
        'Mamta School 2023 workshop short survey' as form_name,
        *
    from {{ ref('int_mamta_school_2023_workshop_short_survey_ece') }}
),

mamta_school_2024_baseline as (
    select
        'MAMTA school 2024 baseline survey' as form_name,
        *
    from {{ ref('int_mamta_school_2024_baseline_survey') }}
),

mamta_school_2025_endline as (
    select
        'MAMTA school 2025 endline survey' as form_name,
        *
    from {{ ref('int_mamta_school_2025_endline_survey') }}
),

mosonie_dc_project_teachers_2025_baseline as (
    select
        'Mosonie DC Project teachers 2025 Baseline survey' as form_name,
        *
    from {{ ref('int_mosonie_dc_project_teachers_2025_baseline_survey') }}
),

mosonie_dc_student_2025_baseline as (
    select
        'Mosonie DC Student 2025 baseline survey' as form_name,
        *
    from {{ ref('int_mosonie_dc_student_2025_baseline_survey') }}
),

mosonie_irctc_community_2025_baseline as (
    select
        'Mosonie IRCTC Community 2025 baseline survey' as form_name,
        *
    from {{ ref('int_mosonie_irctc_community_2025_baseline_survey') }}
),

msi_cs_2024_baseline as (
    select
        'MSI CS 2024 baseline Survey' as form_name,
        *
    from {{ ref('int_msi_cs_2024_baseline_survey') }}
),

msi_cs_2024_endline as (
    select
        'MSI CS 2024 endline Survey' as form_name,
        *
    from {{ ref('int_msi_cs_2024_endline_survey') }}
),

msi_cs_2025_baseline as (
    select
        'MSI CS 2025 baseline survey' as form_name,
        *
    from {{ ref('int_msi_cs_2025_baseline_survey') }}
),

msi_cs_2025_endline as (
    select
        'MSI CS 2025 endline survey' as form_name,
        *
    from {{ ref('int_msi_cs_2025_endline_survey') }}
),

msi_trainers_2025_baseline as (
    select
        'MSI Trainers 2025 baseline survey' as form_name,
        *
    from {{ ref('int_msi_trainers_2025_baseline_survey') }}
),

ntpc_community_2024_baseline as (
    select
        'NTPC Community 2024 baseline survey' as form_name,
        *
    from {{ ref('int_ntpc_community_2024_baseline_survey') }}
),

ntpc_community_2025_endline as (
    select
        'NTPC Community 2025 endline survey' as form_name,
        *
    from {{ ref('int_ntpc_community_2025_endline_survey') }}
),

observation_checklist as (
    select
        'Observation checklist' as form_name,
        *
    from {{ ref('int_observation_checklist') }}
),

rajkot_community_2025_baseline_survey as (
    select
        'Rajkot Community 2025 baseline survey' as form_name,
        *
    from {{ ref('int_rajkot_community_2025_baseline_survey') }}
),

tfi_school_2024_baseline as (
    select
        'TFI School 2024 baseline survey' as form_name,
        *
    from {{ ref('int_tfi_school_2024_baseline_survey') }}
),

tfi_school_2024_endline as (
    select
        'TFI School 2024 endline survey' as form_name,
        *
    from {{ ref('int_tfi_school_2024_endline_survey') }}
),

yflo_community_2025_baseline as (
    select
        'YFLO community 2025 baseline survey' as form_name,
        *
    from {{ ref('int_yflo_community_2025_baseline_survey') }}
)

select * from barula_community_2025_baseline_survey
union all
select * from baseline_questionnaire_bareilly
union all
select * from bir_community_baseline2025
union all
select * from caf_ajmer_2024_baselineendline
union all
select * from caf_ajmer_students_2025_baseline
union all
select * from caf_delhi_school_2024_baseline
union all
select * from caf_delhi_school_2025_endline
union all
select * from elephanta_community_2025_baseline
union all
select * from elephanta_community_2025_endline_survey
union all
select * from fbc_community_2024_baseline
union all
select * from fbc_school_boys_2024_baseline
union all
select * from fbc_school_boys_2024_endline
union all
select * from fbc_school_girls_2024_baseline
union all
select * from fbc_school_girls_2024_endline
union all
select * from fbc_women_jhatingri_baseline
union all
select * from ffec_community_2025_baseline
union all
select * from jakson_cc_trainers_2025
union all
select * from jakson_community_2024_baseline
union all
select * from jakson_community_2025_endline
union all
select * from jk_fenner_hyd_2025_baseline
union all
select * from jk_fenner_hyd_2025_phase_2_baseline_survey
union all
select * from kokari_2024_baseline
union all
select * from kokari_community_2025_baseline
union all
select * from kokari_community_2025_endline
union all
select * from majuli_needs_assessment
union all
select * from mamta_2025_endline
union all
select * from mamta_school_2023_workshop_long
union all
select * from mamta_school_2023_workshop_short_ece
union all
select * from mamta_school_2024_baseline
union all
select * from mamta_school_2025_endline
union all
select * from mosonie_dc_project_teachers_2025_baseline
union all
select * from mosonie_dc_student_2025_baseline
union all
select * from mosonie_irctc_community_2025_baseline
union all
select * from msi_cs_2024_baseline
union all
select * from msi_cs_2024_endline
union all
select * from msi_cs_2025_baseline
union all
select * from msi_cs_2025_endline
union all
select * from msi_trainers_2025_baseline
union all
select * from ntpc_community_2024_baseline
union all
select * from ntpc_community_2025_endline
union all
select * from observation_checklist
union all
select * from rajkot_community_2025_baseline_survey
union all
select * from tfi_school_2024_baseline
union all
select * from tfi_school_2024_endline
union all
select * from yflo_community_2025_baseline