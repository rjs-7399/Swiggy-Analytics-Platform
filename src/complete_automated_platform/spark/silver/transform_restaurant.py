"""
Silver Layer: Restaurant data transformation
Validates and transforms restaurant data from Bronze to Silver layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when, lit
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, DecimalType
import os
from datetime import datetime


def transform_restaurant_data(execution_date):
    """
    Transform and validate restaurant data from bronze to silver layer.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Silver-Restaurant-Transformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    bronze_path = "/data/bronze/restaurant"
    silver_path = "/data/silver/restaurant"

    # Read bronze data with execution_date filter
    bronze_df = spark.read \
        .format("delta") \
        .load(bronze_path) \
        .filter(col("_execution_date") == execution_date)

    # Apply transformations and validations
    silver_df = bronze_df \
        .withColumn("restaurant_id", col("restaurantid").cast(IntegerType())) \
        .withColumn("name", col("name").cast(StringType())) \
        .withColumn("cuisine_type", col("cuisinetype").cast(StringType())) \
        .withColumn("pricing_for_two", col("pricing_for_2").cast(DecimalType(10, 2))) \
        .withColumn("restaurant_phone", col("restaurant_phone").cast(StringType())) \
        .withColumn("operating_hours", col("operatinghours").cast(StringType())) \
        .withColumn("location_id_fk", col("locationid").cast(IntegerType())) \
        .withColumn("active_flag", col("activeflag").cast(StringType())) \
        .withColumn("open_status", col("openstatus").cast(StringType())) \
        .withColumn("locality", col("locality").cast(StringType())) \
        .withColumn("restaurant_address", col("restaurant_address").cast(StringType())) \
        .withColumn("created_dt", to_timestamp(col("createddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("modified_dt", to_timestamp(col("modifieddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("_silver_processed_dt", current_timestamp())

    # Select the final set of columns
    silver_df = silver_df.select(
        "restaurant_id",
        "name",
        "cuisine_type",
        "pricing_for_two",
        "restaurant_phone",
        "operating_hours",
        "location_id_fk",
        "active_flag",
        "open_status",
        "locality",
        "restaurant_address",
        "created_dt",
        "modified_dt",
        "_ingestion_timestamp",
        "_source_file",
        "_execution_date",
        "_silver_processed_dt"
    )

    # Data validation
    # Count records with NULL primary keys
    null_pk_count = silver_df.filter(col("restaurant_id").isNull()).count()

    # Add validation result to log
    if null_pk_count > 0:
        print(f"WARNING: {null_pk_count} records have NULL restaurant_id")

    # Write to silver layer using Delta format with merge schema
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(silver_path)

    # Get metrics for logging
    row_count = silver_df.count()

    print(f"Transformed {row_count} restaurant records to silver layer")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "restaurant",
        "layer": "silver",
        "execution_date": execution_date,
        "row_count": row_count,
        "null_pk_count": null_pk_count
    }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    transform_restaurant_data(execution_date)