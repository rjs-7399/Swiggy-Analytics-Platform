"""
Master DAG that orchestrates the Swiggy data platform ETL process.
This DAG coordinates the execution of dimension and fact processing DAGs.
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pendulum
import logging

# Set timezone for consistent scheduling
local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['data-alerts@swiggy.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}


def check_source_data_availability(**kwargs):
    """
    Check if all required source data is available for processing.
    Returns a dictionary of entity availability status.
    """
    # In a real implementation, this would check for data in source
    # systems or landing zones
    logging.info("Checking source data availability...")

    # Simulate data availability check
    entities = ['customer', 'restaurant', 'location', 'menu', 'orders']
    availability = {entity: True for entity in entities}

    # Set these as XCom variables for downstream tasks
    for entity, is_available in availability.items():
        kwargs['ti'].xcom_push(key=f'{entity}_available', value=is_available)

    # Return overall status
    return all(availability.values())


with DAG(
        'swiggy_master_etl',
        default_args=default_args,
        description='Master DAG for Swiggy ETL pipeline',
        schedule_interval='0 1 * * *',  # Run at 1 AM daily
        catchup=False,
        tags=['swiggy', 'master'],
) as dag:
    # Start pipeline
    start = DummyOperator(
        task_id='start_pipeline',
    )

    # Check for all required source data
    check_source_data = PythonOperator(
        task_id='check_source_data',
        python_callable=check_source_data_availability,
        provide_context=True,
    )

    # Trigger dimension processing DAG
    trigger_dimension_processing = TriggerDagRunOperator(
        task_id='trigger_dimension_processing',
        trigger_dag_id='swiggy_dimension_processing',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    # Trigger fact table processing DAG
    trigger_fact_processing = TriggerDagRunOperator(
        task_id='trigger_fact_processing',
        trigger_dag_id='swiggy_fact_processing',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    # Trigger KPI views generation
    trigger_kpi_generation = TriggerDagRunOperator(
        task_id='trigger_kpi_generation',
        trigger_dag_id='swiggy_kpi_generation',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    # End pipeline
    end = DummyOperator(
        task_id='end_pipeline',
    )

    # Set dependencies
    start >> check_source_data >> trigger_dimension_processing >> trigger_fact_processing >> trigger_kpi_generation >> end