"""
Bronze Layer: Customer data ingestion
Loads raw customer data into Bronze layer Delta table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os
from datetime import datetime


def ingest_customer_data(execution_date):
    """
    Ingest customer data from source files to bronze layer.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Bronze-Customer-Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    source_path = f"/data/source/customer/{execution_date}/*.csv"
    bronze_path = "/data/bronze/customer"

    # Read source data (all columns as string to avoid type conversion errors)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .csv(source_path)

    # Add metadata columns
    df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_execution_date", lit(execution_date))

    # Write to bronze layer using Delta format
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(bronze_path)

    # Get metrics for logging
    row_count = df.count()

    # Log metrics
    print(f"Ingested {row_count} customer records to bronze layer")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "customer",
        "layer": "bronze",
        "execution_date": execution_date,
        "row_count": row_count
    }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    ingest_customer_data(execution_date)