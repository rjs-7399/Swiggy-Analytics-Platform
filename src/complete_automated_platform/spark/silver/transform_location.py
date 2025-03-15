"""
Silver Layer: Location data transformation
Validates and transforms location data from Bronze to Silver layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when, lit
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, BooleanType
import os
from datetime import datetime


def transform_location_data(execution_date):
    """
    Transform and validate location data from bronze to silver layer.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Silver-Location-Transformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    bronze_path = "/data/bronze/location"
    silver_path = "/data/silver/location"

    # Read bronze data with execution_date filter
    bronze_df = spark.read \
        .format("delta") \
        .load(bronze_path) \
        .filter(col("_execution_date") == execution_date)

    # Apply transformations and validations
    silver_df = bronze_df \
        .withColumn("location_id", col("locationid").cast(IntegerType())) \
        .withColumn("city", col("city").cast(StringType())) \
        .withColumn("state", col("state").cast(StringType())) \
        .withColumn("zip_code", col("zipcode").cast(StringType())) \
        .withColumn("active_flag",
                    when(col("activeflag").isin("Y", "YES", "TRUE", "1", "ACTIVE"), "Y")
                    .when(col("activeflag").isin("N", "NO", "FALSE", "0", "INACTIVE"), "N")
                    .otherwise(col("activeflag"))
                    ) \
        .withColumn("created_dt", to_timestamp(col("createdate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("modified_dt", to_timestamp(col("modifieddate"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("_silver_processed_dt", current_timestamp())

    # Enrich with state code and union territory flags
    silver_df = silver_df.withColumn(
        "state_code",
        when(col("state") == "Delhi", "DL")
        .when(col("state") == "Maharashtra", "MH")
        .when(col("state") == "Uttar Pradesh", "UP")
        .when(col("state") == "Gujarat", "GJ")
        .when(col("state") == "Rajasthan", "RJ")
        .when(col("state") == "Kerala", "KL")
        .when(col("state") == "Karnataka", "KA")
        .when(col("state") == "Tamil Nadu", "TN")
        .when(col("state") == "Telangana", "TG")
        .when(col("state") == "Punjab", "PB")
        .otherwise(lit(None).cast(StringType()))
    ).withColumn(
        "is_union_territory",
        when(col("state").isin("Delhi", "Chandigarh", "Puducherry", "Jammu and Kashmir"), True)
        .otherwise(False)
    )

    # Select the final set of columns
    silver_df = silver_df.select(
        "location_id",
        "city",
        "state",
        "state_code",
        "is_union_territory",
        "zip_code",
        "active_flag",
        "created_dt",
        "modified_dt",
        "_ingestion_timestamp",
        "_source_file",
        "_execution_date",
        "_silver_processed_dt"
    )

    # Data validation
    # Count records with NULL primary keys
    null_pk_count = silver_df.filter(col("location_id").isNull()).count()

    # Add validation result to log
    if null_pk_count > 0:
        print(f"WARNING: {null_pk_count} records have NULL location_id")

    # Write to silver layer using Delta format with merge schema
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(silver_path)

    # Get metrics for logging
    row_count = silver_df.count()

    print(f"Transformed {row_count} location records to silver layer")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "location",
        "layer": "silver",
        "execution_date": execution_date,
        "row_count": row_count,
        "null_pk_count": null_pk_count
    }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    transform_location_data(execution_date)