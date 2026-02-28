-- Snowflake environment setup for Healthcare Data Quality Framework

CREATE DATABASE IF NOT EXISTS HEALTHCARE_DB;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;

USE DATABASE HEALTHCARE_DB;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;
CREATE SCHEMA IF NOT EXISTS RAW;

CREATE TABLE IF NOT EXISTS STAGING.RAW_PATIENTS (
    patient_id          VARCHAR(50),
    last_name           VARCHAR(100),
    first_name          VARCHAR(100),
    dob                 VARCHAR(20),
    gender              VARCHAR(10),
    address             VARCHAR(500),
    message_type        VARCHAR(20),
    message_timestamp   VARCHAR(30),
    processed_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STAGING.RAW_ENCOUNTERS (
    encounter_id        VARCHAR(50),
    patient_id          VARCHAR(50),
    encounter_date      VARCHAR(30),
    provider_id         VARCHAR(50),
    encounter_type      VARCHAR(50),
    facility_id         VARCHAR(50),
    diagnosis_code      VARCHAR(20),
    processed_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
