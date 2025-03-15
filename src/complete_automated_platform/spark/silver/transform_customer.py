"""
Silver Layer: Customer data transformation
Validates and transforms customer data from Bronze to Silver layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when, lit
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType
import os
from datetime import datetime

def transform_customer_data(execution_date):
    """
    Transform and validate customer data from bronze to silver layer.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Silver-Customer-Transformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    bronze_path = "/data/bronze/customer"
    silver_path = "/data/silver/customer"

    # Read bronze data with execution_date filter
    bronze_df = spark.read \
        .format("delta") \
        .load(bronze_path) \
        .filter(col("_execution_date") == execution_date)

    # Apply transformations and validations
    silver_df = bronze_df \
        .withColumn("customer_id", col("customerid").cast(IntegerType())) \
        .withColumn("name", col("name").cast(StringType())) \
        .withColumn("mobile", col("mobile").cast(StringType())) \
        .withColumn("email", col("email").cast(StringType())) \
        .withColumn("login_by_using", col("loginbyusing").cast(StringType())) \
        .withColumn("gender", col("gender").cast(StringType())) \
        .withColumn("dob", to_timestamp(col("dob"), "yyyy-MM-dd").cast(DateType())) \
        .withColumn("anniversary", to_timestamp(col("anniversary"), "yyyy-MM-dd").cast(DateType())) \
        .withColumn("preferences", col("preferences")) \
        .withColumn("created_dt", to_timestamp(col("createddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("modified_dt", to_timestamp(col("modifieddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("_silver_processed_dt", current_timestamp())

    # Select the final set of columns
    silver_df = silver_df.select(
        "customer_id",
        "name",
        "mobile",
        "email",
        "login_by_using",
        "gender",
        "dob",
        "anniversary",
        "preferences",
        "created_dt",
        "modified_dt",
        "_ingestion_timestamp",
        "_source_file",
        "_execution_date",
        "_silver_processed_dt"
    )

    # Data validation
    # Count records with NULL primary keys
    null_pk_count = silver_df.filter(col("customer_id").isNull()).count()

    # Add validation result to log
    if null_pk_count > 0:
        print(f"WARNING: {null_pk_count} records have NULL customer_id")

    # Write to silver layer using Delta format with merge schema
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(silver_path)

    # Get metrics for logging
    row_count = silver_df.count()

    print(f"Transformed {row_count} customer records to silver layer")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "customer",
        "layer": "silver",
        "execution_date": execution_date,
        "row_count": row_count,
        "null_pk_count": null_pk_count
    }

if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    transform_customer_data(execution_date)