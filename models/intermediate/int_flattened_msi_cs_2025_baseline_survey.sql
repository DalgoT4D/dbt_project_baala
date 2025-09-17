{{
    config(
        materialized='table',
        tags=['intermediate', 'flattened', 'survey']
    )
}}

-- Flattened intermediate model for msi_cs_2025_baseline_survey
-- This model flattens the JSONB data from the staging layer into individual columns

with flattened as (
    select
        _id,
        submission_timestamp,
        {{ flatten_json_columns(ref('stg_msi_cs_2025_baseline_survey'), "data") }}
    from {{ ref('stg_msi_cs_2025_baseline_survey') }}
)
select * from flattened
