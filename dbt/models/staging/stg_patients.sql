{{ config(materialized='view') }}

with source as (
    select * from {{ source('healthcare_raw', 'raw_patients') }}
),

renamed as (
    select
        patient_id,
        trim(upper(last_name))               as last_name,
        trim(initcap(first_name))            as first_name,
        try_to_date(dob, 'YYYYMMDD')         as date_of_birth,
        case
            when gender = 'M' then 'Male'
            when gender = 'F' then 'Female'
            else 'Unknown'
        end                                  as gender,
        address                              as address_raw,
        message_type                         as source_message_type,
        try_to_timestamp(message_timestamp)  as message_timestamp,
        processed_at::timestamp              as processed_at,
        current_timestamp()                  as dbt_updated_at
    from source
    where patient_id is not null
      and message_timestamp is not null
)

select * from renamed
