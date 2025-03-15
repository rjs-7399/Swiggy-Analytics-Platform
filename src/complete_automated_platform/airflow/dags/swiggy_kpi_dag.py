"""
DAG for generating KPI views from the fact and dimension tables.
This DAG runs after the fact tables are fully populated.
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
import pendulum
import sys, os

# Add project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

# Import Spark processing modules
from spark.gold.views.yearly_revenue_kpi import build_yearly_revenue_kpi
from spark.gold.views.monthly_revenue_kpi import build_monthly_revenue_kpi
from spark.gold.views.restaurant_revenue_kpi import build_restaurant_revenue_kpi
from spark.gold.views.daily_revenue_kpi import build_daily_revenue_kpi

# Set timezone
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

with DAG(
        'swiggy_kpi_generation',
        default_args=default_args,
        description='Generate KPI views from fact and dimension tables',
        schedule_interval=None,  # Triggered by master DAG
        catchup=False,
        tags=['swiggy', 'kpi'],
) as dag:
    # Start KPI view generation
    start = DummyOperator(
        task_id='start_kpi_generation',
    )

    # Wait for fact table processing to complete
    wait_for_facts = ExternalTaskSensor(
        task_id='wait_for_facts',
        external_dag_id='swiggy_fact_processing',
        external_task_id='end_fact_processing',
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
    )

    # KPI view generation tasks
    yearly_revenue_kpi = PythonOperator(
        task_id='yearly_revenue_kpi',
        python_callable=build_yearly_revenue_kpi,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    monthly_revenue_kpi = PythonOperator(
        task_id='monthly_revenue_kpi',
        python_callable=build_monthly_revenue_kpi,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    restaurant_revenue_kpi = PythonOperator(
        task_id='restaurant_revenue_kpi',
        python_callable=build_restaurant_revenue_kpi,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    daily_revenue_kpi = PythonOperator(
        task_id='daily_revenue_kpi',
        python_callable=build_daily_revenue_kpi,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    # End KPI view generation
    end = DummyOperator(
        task_id='end_kpi_generation',
    )

    # Set dependencies
    start >> wait_for_facts >> [yearly_revenue_kpi, monthly_revenue_kpi,
                                restaurant_revenue_kpi, daily_revenue_kpi] >> end