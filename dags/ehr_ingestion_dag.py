"""
EHR Data Ingestion DAG - Orchestrates end-to-end ingestion of
Electronic Health Records from S3 into Snowflake staging tables.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ehr_data_ingestion",
    default_args=default_args,
    description="Ingest HL7, CCD, and JSON EHR data into Snowflake",
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["healthcare", "ingestion", "ehr"],
)


def check_s3_for_new_files(**context):
    import boto3
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket="healthcare-ehr-landing", Prefix="incoming/")
    files = [obj["Key"] for obj in response.get("Contents", [])]
    context["task_instance"].xcom_push(key="new_files", value=files)
    print(f"Found {len(files)} new files for processing")
    return files


def run_quality_pre_check(**context):
    from src.quality.completeness_checker import CompletenessChecker
    files = context["task_instance"].xcom_pull(key="new_files")
    checker = CompletenessChecker()
    results = checker.validate_batch(files)
    failed = [r for r in results if not r["passed"]]
    if failed:
        raise ValueError(f"Pre-check failed for {len(failed)} files")
    print(f"Pre-checks passed for {len(files)} files")


def send_completion_alert(**context):
    files = context["task_instance"].xcom_pull(key="new_files")
    print(f"EHR Ingestion complete: {len(files)} files processed successfully.")


check_s3 = PythonOperator(task_id="check_s3_for_new_files", python_callable=check_s3_for_new_files, dag=dag)
pre_quality_check = PythonOperator(task_id="run_quality_pre_check", python_callable=run_quality_pre_check, dag=dag)
parse_hl7 = GlueJobOperator(task_id="parse_hl7_messages", job_name="healthcare-hl7-parser", aws_conn_id="aws_default", dag=dag)
parse_ccd = GlueJobOperator(task_id="parse_ccd_documents", job_name="healthcare-ccd-parser", aws_conn_id="aws_default", dag=dag)
load_to_staging = SnowflakeOperator(task_id="load_to_snowflake_staging", sql="sql/load_staging.sql", snowflake_conn_id="snowflake_default", dag=dag)
post_quality_check = SnowflakeOperator(task_id="run_post_quality_checks", sql="sql/quality_check_queries.sql", snowflake_conn_id="snowflake_default", dag=dag)
alert = PythonOperator(task_id="send_completion_alert", python_callable=send_completion_alert, dag=dag)

check_s3 >> pre_quality_check >> [parse_hl7, parse_ccd] >> load_to_staging >> post_quality_check >> alert
