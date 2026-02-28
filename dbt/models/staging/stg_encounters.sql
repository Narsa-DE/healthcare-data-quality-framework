{{ config(materialized='view') }}

with source as (
    select * from {{ source('healthcare_raw', 'raw_encounters') }}
),

renamed as (
    select
        encounter_id,
        patient_id,
        try_to_timestamp(encounter_date)    as encounter_date,
        provider_id,
        trim(upper(encounter_type))         as encounter_type,
        facility_id,
        trim(diagnosis_code)                as primary_diagnosis_code,
        processed_at::timestamp             as processed_at,
        current_timestamp()                 as dbt_updated_at
    from source
    where encounter_id is not null
      and patient_id is not null
)

select * from renamed
