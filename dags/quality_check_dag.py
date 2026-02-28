"""
Data Quality Check DAG - Runs automated quality validation
on all staged healthcare data every hour.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "healthcare_quality_checks",
    default_args=default_args,
    description="Hourly data quality validation for staged EHR data",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["healthcare", "quality"],
)


def evaluate_quality_results(**context):
    """Evaluate query results and raise alert if thresholds breached."""
    results = context["task_instance"].xcom_pull(task_ids="run_quality_checks")
    threshold = 99.0
    for row in (results or []):
        completeness = float(row.get("completeness_pct", 100))
        if completeness < threshold:
            raise ValueError(f"Quality breach: completeness={completeness}% < {threshold}%")
    print("All quality checks passed!")


run_checks = SnowflakeOperator(
    task_id="run_quality_checks",
    sql="sql/quality_check_queries.sql",
    snowflake_conn_id="snowflake_default",
    dag=dag,
)

evaluate = PythonOperator(
    task_id="evaluate_quality_results",
    python_callable=evaluate_quality_results,
    dag=dag,
)

run_checks >> evaluate
