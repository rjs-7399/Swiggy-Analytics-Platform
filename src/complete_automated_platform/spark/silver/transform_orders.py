"""
Silver Layer: Orders data transformation
Validates and transforms orders data from Bronze to Silver layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when, lit
from pyspark.sql.types import IntegerType, StringType, TimestampType, DecimalType
import os
from datetime import datetime


def transform_orders_data(execution_date):
    """
    Transform and validate orders data from bronze to silver layer.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Silver-Orders-Transformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    bronze_path = "/data/bronze/orders"
    silver_path = "/data/silver/orders"

    # Read bronze data with execution_date filter
    bronze_df = spark.read \
        .format("delta") \
        .load(bronze_path) \
        .filter(col("_execution_date") == execution_date)

    # Apply transformations and validations
    silver_df = bronze_df \
        .withColumn("order_id", col("orderid").cast(IntegerType())) \
        .withColumn("customer_id_fk", col("customerid").cast(IntegerType())) \
        .withColumn("restaurant_id_fk", col("restaurantid").cast(IntegerType())) \
        .withColumn("order_date", to_timestamp(col("orderdate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("total_amount", col("totalamount").cast(DecimalType(10, 2))) \
        .withColumn("status", col("status").cast(StringType())) \
        .withColumn("payment_method", col("paymentmethod").cast(StringType())) \
        .withColumn("created_dt", to_timestamp(col("createddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("modified_dt", to_timestamp(col("modifieddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("_silver_processed_dt", current_timestamp())

    # Select the final set of columns
    silver_df = silver_df.select(
        "order_id",
        "customer_id_fk",
        "restaurant_id_fk",
        "order_date",
        "total_amount",
        "status",
        "payment_method",
        "created_dt",
        "modified_dt",
        "_ingestion_timestamp",
        "_source_file",
        "_execution_date",
        "_silver_processed_dt"
    )

    # Data validation
    # Count records with NULL primary keys or foreign keys
    null_pk_count = silver_df.filter(col("order_id").isNull()).count()
    null_customer_fk_count = silver_df.filter(col("customer_id_fk").isNull()).count()
    null_restaurant_fk_count = silver_df.filter(col("restaurant_id_fk").isNull()).count()

    # Add validation result to log
    if null_pk_count > 0:
        print(f"WARNING: {null_pk_count} records have NULL order_id")
    if null_customer_fk_count > 0:
        print(f"WARNING: {null_customer_fk_count} records have NULL customer_id_fk")
    if null_restaurant_fk_count > 0:
        print(f"WARNING: {null_restaurant_fk_count} records have NULL restaurant_id_fk")

    # Write to silver layer using Delta format with merge schema
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(silver_path)

    # Get metrics for logging
    row_count = silver_df.count()

    print(f"Transformed {row_count} orders records to silver layer")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "orders",
        "layer": "silver",
        "execution_date": execution_date,
        "row_count": row_count,
        "null_pk_count": null_pk_count,
        "null_customer_fk_count": null_customer_fk_count,
        "null_restaurant_fk_count": null_restaurant_fk_count
    }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    transform_orders_data(execution_date)