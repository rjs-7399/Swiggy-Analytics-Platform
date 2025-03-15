"""
Gold Layer: Customer dimension table (SCD Type 2)
Implements slowly changing dimension type 2 for customer data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, sha2, concat_ws, lit, expr, lag,
    coalesce, when, row_number
)
from pyspark.sql.window import Window
import os
from datetime import datetime

# Import SCD utilities
from spark.utils.scd_utils import apply_scd_type2


def build_customer_dimension(execution_date):
    """
    Build customer dimension with SCD Type 2.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Gold-Customer-Dimension") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    silver_path = "/data/silver/customer"
    gold_path = "/data/gold/dimension/customer_dim"

    # Read silver layer data with execution_date filter
    silver_df = spark.read \
        .format("delta") \
        .load(silver_path) \
        .filter(col("_execution_date") == execution_date)

    # If we're processing data for the first time, create a new dimension table
    gold_exists = spark._jvm.org.apache.hadoop.fs.Path(gold_path).getFileSystem(
        spark._jsparkSession.sparkContext._jsc.hadoopConfiguration()
    ).exists(spark._jvm.org.apache.hadoop.fs.Path(gold_path))

    if not gold_exists:
        # First-time dimension table creation (all current records)
        dim_df = silver_df.select(
            # Generate a hash key for the natural key
            sha2(concat_ws("||", col("customer_id")), 256).alias("customer_hk"),
            col("customer_id").alias("customer_id"),
            col("name"),
            col("mobile"),
            col("email"),
            col("login_by_using"),
            col("gender"),
            col("dob"),
            col("anniversary"),
            col("preferences"),
            current_timestamp().alias("eff_start_date"),
            lit(None).cast("timestamp").alias("eff_end_date"),
            lit(True).alias("is_current")
        )

        # Write to gold layer
        dim_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)

        row_count = dim_df.count()
        print(f"Created customer dimension with {row_count} records")

    else:
        # Read existing gold dimension
        gold_df = spark.read.format("delta").load(gold_path)

        # Apply SCD Type 2 logic using the utility function
        updated_dim_df = apply_scd_type2(
            spark=spark,
            source_df=silver_df,
            dim_df=gold_df,
            natural_key="customer_id",
            surrogate_key="customer_hk",
            tracking_columns=[
                "name", "mobile", "email", "login_by_using",
                "gender", "dob", "anniversary", "preferences"
            ],
            effective_start_date="eff_start_date",
            effective_end_date="eff_end_date",
            current_flag="is_current"
        )

        # Get metrics for history tracking
        new_versions_count = updated_dim_df \
            .filter((col("eff_start_date") == current_timestamp()) & (col("is_current") == True)) \
            .count()

        expired_count = updated_dim_df \
            .filter((col("eff_end_date") == current_timestamp()) & (col("is_current") == False)) \
            .count()

        print(
            f"SCD Type 2 customer dimension update: {new_versions_count} new versions, {expired_count} expired records")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "customer",
        "layer": "gold",
        "dimension": "customer_dim",
        "execution_date": execution_date,
        "new_versions_count": new_versions_count if 'new_versions_count' in locals() else None,
        "expired_count": expired_count if 'expired_count' in locals() else None,
        "new_dimension": not gold_exists
    }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    build_customer_dimension(execution_date)