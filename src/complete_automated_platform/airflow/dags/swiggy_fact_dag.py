"""
DAG responsible for processing fact tables in the Swiggy data platform.
Depends on dimension tables being processed first.
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
import pendulum
from datetime import timedelta
import sys, os

# Add project root to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

# Import Spark processing modules
from spark.bronze.ingest_orders import ingest_orders_data
from spark.bronze.ingest_order_items import ingest_order_items_data
from spark.bronze.ingest_delivery import ingest_delivery_data

from spark.silver.transform_orders import transform_orders_data
from spark.silver.transform_order_items import transform_order_items_data
from spark.silver.transform_delivery import transform_delivery_data

from spark.gold.fact_order_item import build_order_item_fact

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
        'swiggy_fact_processing',
        default_args=default_args,
        description='Process fact tables for Swiggy data platform',
        schedule_interval=None,  # Triggered by master DAG
        catchup=False,
        tags=['swiggy', 'facts'],
) as dag:
    # Start fact processing
    start = DummyOperator(
        task_id='start_fact_processing',
    )

    # Wait for dimension processing to complete
    wait_for_dimensions = ExternalTaskSensor(
        task_id='wait_for_dimensions',
        external_dag_id='swiggy_dimension_processing',
        external_task_id='end_dimension_processing',
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
    )

    # Bronze layer ingestion tasks
    ingest_orders = PythonOperator(
        task_id='ingest_orders',
        python_callable=ingest_orders_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    ingest_order_items = PythonOperator(
        task_id='ingest_order_items',
        python_callable=ingest_order_items_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    ingest_delivery = PythonOperator(
        task_id='ingest_delivery',
        python_callable=ingest_delivery_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    # Silver layer transformation tasks
    transform_orders = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_orders_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    transform_order_items = PythonOperator(
        task_id='transform_order_items',
        python_callable=transform_order_items_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    transform_delivery = PythonOperator(
        task_id='transform_delivery',
        python_callable=transform_delivery_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    # Gold layer fact table creation task
    build_order_item_fact_table = PythonOperator(
        task_id='build_order_item_fact_table',
        python_callable=build_order_item_fact,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    # End fact processing
    end = DummyOperator(
        task_id='end_fact_processing',
    )

    # Set dependencies
    start >> wait_for_dimensions

    # Process orders data
    wait_for_dimensions >> ingest_orders >> transform_orders
    wait_for_dimensions >> ingest_order_items >> transform_order_items
    wait_for_dimensions >> ingest_delivery >> transform_delivery

    # Build fact table (requires all silver tables)
    [transform_orders, transform_order_items, transform_delivery] >> build_order_item_fact_table >> end