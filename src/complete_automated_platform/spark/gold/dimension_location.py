"""
Gold Layer: Restaurant Location dimension table (SCD Type 2)
Implements slowly changing dimension type 2 for location data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, sha2, concat_ws, lit, when
)
import os
from datetime import datetime

# Import SCD utilities
from spark.utils.scd_utils import apply_scd_type2


def build_location_dimension(execution_date):
    """
    Build restaurant location dimension with SCD Type 2.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Gold-Location-Dimension") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    silver_path = "/data/silver/location"
    gold_path = "/data/gold/dimension/restaurant_location_dim"

    # Read silver layer data with execution_date filter
    silver_df = spark.read \
        .format("delta") \
        .load(silver_path) \
        .filter(col("_execution_date") == execution_date)

    # Enrich with city tier information
    silver_df = silver_df.withColumn(
        "city_tier",
        when(
            col("city").isin(
                "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai",
                "Kolkata", "Pune", "Ahmedabad"
            ),
            "Tier-1"
        ).when(
            col("city").isin(
                "Jaipur", "Lucknow", "Kanpur", "Nagpur", "Indore", "Bhopal",
                "Patna", "Vadodara", "Coimbatore", "Ludhiana", "Agra", "Nashik",
                "Ranchi", "Meerut", "Raipur", "Guwahati", "Chandigarh"
            ),
            "Tier-2"
        ).otherwise("Tier-3")
    ).withColumn(
        "capital_city_flag",
        when(
            (col("state") == "Delhi" & col("city") == "New Delhi") |
            (col("state") == "Maharashtra" & col("city") == "Mumbai") |
            (col("state") == "Karnataka" & col("city") == "Bengaluru") |
            (col("state") == "Telangana" & col("city") == "Hyderabad") |
            (col("state") == "Tamil Nadu" & col("city") == "Chennai"),
            True
        ).otherwise(False)
    )

    # If we're processing data for the first time, create a new dimension table
    gold_exists = spark._jvm.org.apache.hadoop.fs.Path(gold_path).getFileSystem(
        spark._jsparkSession.sparkContext._jsc.hadoopConfiguration()
    ).exists(spark._jvm.org.apache.hadoop.fs.Path(gold_path))

    if not gold_exists:
        # First-time dimension table creation (all current records)
        dim_df = silver_df.select(
            # Generate a hash key for the natural key
            sha2(concat_ws("||", col("location_id")), 256).alias("restaurant_location_hk"),
            col("location_id"),
            col("city"),
            col("state"),
            col("state_code"),
            col("is_union_territory"),
            col("capital_city_flag"),
            col("city_tier"),
            col("zip_code"),
            col("active_flag"),
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
        print(f"Created restaurant location dimension with {row_count} records")

        return {
            "entity": "location",
            "layer": "gold",
            "dimension": "restaurant_location_dim",
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
            natural_key="location_id",
            surrogate_key="restaurant_location_hk",
            tracking_columns=[
                "city", "state", "state_code", "is_union_territory",
                "capital_city_flag", "city_tier", "zip_code", "active_flag"
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

        print(
            f"SCD Type 2 location dimension update: {new_versions_count} new versions, {expired_count} expired records")

        return {
            "entity": "location",
            "layer": "gold",
            "dimension": "restaurant_location_dim",
            "execution_date": execution_date,
            "new_dimension": False,
            "new_versions_count": new_versions_count,
            "expired_count": expired_count
        }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    build_location_dimension(execution_date)