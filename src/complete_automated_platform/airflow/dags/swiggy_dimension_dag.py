"""
DAG responsible for processing dimension tables in the Swiggy data platform.
Handles bronze to silver and silver to gold transformations for all dimension entities.
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
import pendulum
from datetime import timedelta
import sys, os

# Add the project root to the Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

# Import Spark processing modules
from spark.bronze.ingest_customer import ingest_customer_data
from spark.bronze.ingest_restaurant import ingest_restaurant_data
from spark.bronze.ingest_location import ingest_location_data
from spark.bronze.ingest_menu import ingest_menu_data
from spark.bronze.ingest_delivery_agent import ingest_delivery_agent_data

from spark.silver.transform_customer import transform_customer_data
from spark.silver.transform_restaurant import transform_restaurant_data
from spark.silver.transform_location import transform_location_data
from spark.silver.transform_menu import transform_menu_data
from spark.silver.transform_delivery_agent import transform_delivery_agent_data

from spark.gold.dimension_customer import build_customer_dimension
from spark.gold.dimension_restaurant import build_restaurant_dimension
from spark.gold.dimension_location import build_location_dimension
from spark.gold.dimension_menu import build_menu_dimension
from spark.gold.dimension_date import build_date_dimension
from spark.gold.dimension_delivery_agent import build_delivery_agent_dimension

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
        'swiggy_dimension_processing',
        default_args=default_args,
        description='Process all dimension tables for Swiggy data platform',
        schedule_interval=None,  # Triggered by master DAG
        catchup=False,
        tags=['swiggy', 'dimensions'],
) as dag:
    # Start dimension processing
    start = DummyOperator(
        task_id='start_dimension_processing',
    )

    # Bronze layer ingestion tasks
    ingest_customer = PythonOperator(
        task_id='ingest_customer',
        python_callable=ingest_customer_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    ingest_restaurant = PythonOperator(
        task_id='ingest_restaurant',
        python_callable=ingest_restaurant_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    ingest_location = PythonOperator(
        task_id='ingest_location',
        python_callable=ingest_location_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    ingest_menu = PythonOperator(
        task_id='ingest_menu',
        python_callable=ingest_menu_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    ingest_delivery_agent = PythonOperator(
        task_id='ingest_delivery_agent',
        python_callable=ingest_delivery_agent_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    # Silver layer transformation tasks
    transform_customer = PythonOperator(
        task_id='transform_customer',
        python_callable=transform_customer_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    transform_restaurant = PythonOperator(
        task_id='transform_restaurant',
        python_callable=transform_restaurant_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    transform_location = PythonOperator(
        task_id='transform_location',
        python_callable=transform_location_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    transform_menu = PythonOperator(
        task_id='transform_menu',
        python_callable=transform_menu_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    transform_delivery_agent = PythonOperator(
        task_id='transform_delivery_agent',
        python_callable=transform_delivery_agent_data,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    # Gold layer dimension building tasks
    build_customer_dim = PythonOperator(
        task_id='build_customer_dim',
        python_callable=build_customer_dimension,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    build_restaurant_dim = PythonOperator(
        task_id='build_restaurant_dim',
        python_callable=build_restaurant_dimension,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    build_location_dim = PythonOperator(
        task_id='build_location_dim',
        python_callable=build_location_dimension,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    build_menu_dim = PythonOperator(
        task_id='build_menu_dim',
        python_callable=build_menu_dimension,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    build_delivery_agent_dim = PythonOperator(
        task_id='build_delivery_agent_dim',
        python_callable=build_delivery_agent_dimension,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    # Date dimension is special case (built from scratch, not source data)
    build_date_dim = PythonOperator(
        task_id='build_date_dim',
        python_callable=build_date_dimension,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    # End dimension processing
    end = DummyOperator(
        task_id='end_dimension_processing',
    )

    # Set dependencies for each entity - Bronze to Silver to Gold
    start >> ingest_customer >> transform_customer >> build_customer_dim
    start >> ingest_restaurant >> transform_restaurant >> build_restaurant_dim
    start >> ingest_location >> transform_location >> build_location_dim
    start >> ingest_menu >> transform_menu >> build_menu_dim
    start >> ingest_delivery_agent >> transform_delivery_agent >> build_delivery_agent_dim

    # Date dimension has different path
    start >> build_date_dim

    # All dimensions must complete before ending
    [build_customer_dim, build_restaurant_dim, build_location_dim,
     build_menu_dim, build_delivery_agent_dim, build_date_dim] >> end