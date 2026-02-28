{{ config(materialized='table') }}

with encounters as (
    select * from {{ ref('stg_encounters') }}
),

patients as (
    select * from {{ ref('stg_patients') }}
),

final as (
    select
        e.encounter_id,
        e.patient_id,
        p.first_name,
        p.last_name,
        p.date_of_birth,
        p.gender,
        e.encounter_date,
        e.encounter_type,
        e.facility_id,
        e.primary_diagnosis_code,
        datediff('year', p.date_of_birth, e.encounter_date) as patient_age_at_encounter,
        e.processed_at
    from encounters e
    left join patients p on e.patient_id = p.patient_id
)

select * from final
