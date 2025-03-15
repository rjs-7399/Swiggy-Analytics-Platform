"""
Gold Layer: Date dimension table
Creates a date dimension for all dates in the relevant range
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit, col, expr, date_format, dayofweek, dayofmonth,
    dayofyear, weekofyear, month, quarter, year, sha2
)
import os
from datetime import datetime, timedelta


def build_date_dimension(execution_date):
    """
    Build date dimension table.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Gold-Date-Dimension") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define path
    gold_path = "/data/gold/dimension/date_dim"

    # Check if date dimension exists
    date_dim_exists = spark._jvm.org.apache.hadoop.fs.Path(gold_path).getFileSystem(
        spark._jsparkSession.sparkContext._jsc.hadoopConfiguration()
    ).exists(spark._jvm.org.apache.hadoop.fs.Path(gold_path))

    if not date_dim_exists:
        # Find the min order date from silver orders (or default to 3 years back)
        try:
            orders_df = spark.read.format("delta").load("/data/silver/orders")
            min_date = orders_df.agg({"order_date": "min"}).collect()[0][0]

            if min_date is None:
                min_date = datetime.now() - timedelta(days=365 * 3)  # Default to 3 years back
        except:
            min_date = datetime.now() - timedelta(days=365 * 3)  # Default to 3 years back

        # Find the max date (current date + 2 years)
        max_date = datetime.now() + timedelta(days=365 * 2)

        # Create a date range
        dates_list = []
        current_date = min_date
        while current_date <= max_date:
            dates_list.append((current_date.strftime("%Y-%m-%d"),))
            current_date += timedelta(days=1)

        # Create DataFrame with dates
        dates_df = spark.createDataFrame(dates_list, ["calendar_date"])

        # Add date dimension attributes
        date_dim_df = dates_df \
            .withColumn("date_dim_hk", sha2(col("calendar_date"), 256)) \
            .withColumn("year", year(col("calendar_date"))) \
            .withColumn("quarter", quarter(col("calendar_date"))) \
            .withColumn("month", month(col("calendar_date"))) \
            .withColumn("week", weekofyear(col("calendar_date"))) \
            .withColumn("day_of_year", dayofyear(col("calendar_date"))) \
            .withColumn("day_of_week", dayofweek(col("calendar_date"))) \
            .withColumn("day_of_the_month", dayofmonth(col("calendar_date"))) \
            .withColumn("day_name", date_format(col("calendar_date"), "EEEE"))

        # Write date dimension to gold layer
        date_dim_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_path)

        date_count = date_dim_df.count()
        print(f"Created date dimension with {date_count} dates from {min_date} to {max_date}")

        return {
            "entity": "date",
            "layer": "gold",
            "dimension": "date_dim",
            "execution_date": execution_date,
            "min_date": min_date.strftime("%Y-%m-%d"),
            "max_date": max_date.strftime("%Y-%m-%d"),
            "date_count": date_count
        }
    else:
        print("Date dimension already exists. No update required.")
        return {
            "entity": "date",
            "layer": "gold",
            "dimension": "date_dim",
            "execution_date": execution_date,
            "status": "already_exists"
        }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    build_date_dimension(execution_date)