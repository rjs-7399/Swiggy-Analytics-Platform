"""
Gold Layer: Restaurant dimension table (SCD Type 2)
Implements slowly changing dimension type 2 for restaurant data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, sha2, concat_ws, lit
)
import os
from datetime import datetime

# Import SCD utilities
from spark.utils.scd_utils import apply_scd_type2

def build_restaurant_dimension(execution_date):
    """
    Build restaurant dimension with SCD Type 2.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Gold-Restaurant-Dimension") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    silver_path = "/data/silver/restaurant"
    gold_path = "/data/gold/dimension/restaurant_dim"

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
            sha2(concat_ws("||", col("restaurant_id")), 256).alias("restaurant_hk"),
            col("restaurant_id"),
            col("name"),
            col("cuisine_type"),
            col("pricing_for_two"),
            col("restaurant_phone"),
            col("operating_hours"),
            col("location_id_fk"),
            col("active_flag"),
            col("open_status"),
            col("locality"),
            col("restaurant_address"),
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
        print(f"Created restaurant dimension with {row_count} records")

        return {
            "entity": "restaurant",
            "layer": "gold",
            "dimension": "restaurant_dim",
            "execution_date": execution_date,
            "new_dimension": True,
            "row_count": row_count
        }
    else:
        # Read existing gold dimension
        gold_df = spark.read.format("delta").load(gold_path)

        # Apply SCD Type 2 logic using the utility function
        updated_dim_df = apply_scd_type2(
            spark=spark,
            source_df=silver_df,
            dim_df=gold_df,
            natural_key="restaurant_id",
            surrogate_key="restaurant_hk",
            tracking_columns=[
                "name", "cuisine_type", "pricing_for_two", "restaurant_phone",
                "operating_hours", "location_id_fk", "active_flag", "open_status",
                "locality", "restaurant_address"
            ],
            effective_start_date="eff_start_date",
            effective_end_date="eff_end_date",
            current_flag="is_current"
        )

        # Write updated dimension to gold layer
        updated_dim_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)

        # Get metrics for history tracking
        new_versions_count = updated_dim_df \
            .filter((col("eff_start_date") == current_timestamp()) & (col("is_current") == True)) \
            .count()

        expired_count = updated_dim_df \
            .filter((col("eff_end_date") == current_timestamp()) & (col("is_current") == False)) \
            .count()

        print(f"SCD Type 2 restaurant dimension update: {new_versions_count} new versions, {expired_count} expired records")

        return {
            "entity": "restaurant",
            "layer": "gold",
            "dimension": "restaurant_dim",
            "execution_date": execution_date,
            "new_dimension": False,
            "new_versions_count": new_versions_count,
            "expired_count": expired_count
        }

if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    build_restaurant_dimension(execution_date)