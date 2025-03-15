"""
Silver Layer: Menu data transformation
Validates and transforms menu data from Bronze to Silver layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when, lit
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, DecimalType, BooleanType
import os
from datetime import datetime


def transform_menu_data(execution_date):
    """
    Transform and validate menu data from bronze to silver layer.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Silver-Menu-Transformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    bronze_path = "/data/bronze/menu"
    silver_path = "/data/silver/menu"

    # Read bronze data with execution_date filter
    bronze_df = spark.read \
        .format("delta") \
        .load(bronze_path) \
        .filter(col("_execution_date") == execution_date)

    # Apply transformations and validations
    silver_df = bronze_df \
        .withColumn("menu_id", col("menuid").cast(IntegerType())) \
        .withColumn("restaurant_id_fk", col("restaurantid").cast(IntegerType())) \
        .withColumn("item_name", col("itemname").cast(StringType())) \
        .withColumn("description", col("description").cast(StringType())) \
        .withColumn("price", col("price").cast(DecimalType(10, 2))) \
        .withColumn("category", col("category").cast(StringType())) \
        .withColumn("availability",
                    when(col("availability").isin("Y", "YES", "TRUE", "1", "AVAILABLE"), True)
                    .when(col("availability").isin("N", "NO", "FALSE", "0", "UNAVAILABLE"), False)
                    .otherwise(lit(True))
                    ) \
        .withColumn("item_type", col("itemtype").cast(StringType())) \
        .withColumn("created_dt", to_timestamp(col("createddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("modified_dt", to_timestamp(col("modifieddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("_silver_processed_dt", current_timestamp())

    # Select the final set of columns
    silver_df = silver_df.select(
        "menu_id",
        "restaurant_id_fk",
        "item_name",
        "description",
        "price",
        "category",
        "availability",
        "item_type",
        "created_dt",
        "modified_dt",
        "_ingestion_timestamp",
        "_source_file",
        "_execution_date",
        "_silver_processed_dt"
    )

    # Data validation
    # Count records with NULL primary keys
    null_pk_count = silver_df.filter(col("menu_id").isNull()).count()
    null_fk_count = silver_df.filter(col("restaurant_id_fk").isNull()).count()

    # Add validation result to log
    if null_pk_count > 0:
        print(f"WARNING: {null_pk_count} records have NULL menu_id")
    if null_fk_count > 0:
        print(f"WARNING: {null_fk_count} records have NULL restaurant_id_fk")

    # Write to silver layer using Delta format with merge schema
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(silver_path)

    # Get metrics for logging
    row_count = silver_df.count()

    print(f"Transformed {row_count} menu records to silver layer")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "menu",
        "layer": "silver",
        "execution_date": execution_date,
        "row_count": row_count,
        "null_pk_count": null_pk_count,
        "null_fk_count": null_fk_count
    }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    transform_menu_data(execution_date)