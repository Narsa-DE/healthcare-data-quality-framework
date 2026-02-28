-- Data Quality Check Queries
-- Run after each ingestion batch to validate data integrity

-- 1. Completeness check
SELECT
    'completeness'                                                              AS check_type,
    COUNT(*)                                                                    AS total_records,
    SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END)                        AS missing_patient_id,
    SUM(CASE WHEN message_timestamp IS NULL THEN 1 ELSE 0 END)                 AS missing_timestamp,
    ROUND(
        (COUNT(*) - SUM(CASE WHEN patient_id IS NULL OR message_timestamp IS NULL THEN 1 ELSE 0 END))
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                                           AS completeness_pct
FROM HEALTHCARE_DB.STAGING.RAW_PATIENTS
WHERE processed_at >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP());

-- 2. Duplicate detection
SELECT patient_id, message_timestamp, COUNT(*) AS duplicate_count
FROM HEALTHCARE_DB.STAGING.RAW_PATIENTS
GROUP BY patient_id, message_timestamp
HAVING COUNT(*) > 1;

-- 3. Latency check
SELECT
    COUNT(*)                                                                    AS total_records,
    MIN(message_timestamp)                                                      AS oldest_record,
    MAX(message_timestamp)                                                      AS newest_record,
    DATEDIFF('minute', MIN(message_timestamp), CURRENT_TIMESTAMP())            AS max_latency_minutes
FROM HEALTHCARE_DB.STAGING.RAW_PATIENTS
WHERE processed_at >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP());
