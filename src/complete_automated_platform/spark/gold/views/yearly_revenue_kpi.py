"""
Gold Layer: Yearly Revenue KPI View
Creates a view for yearly revenue analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, year, round
)
import os
from datetime import datetime


def build_yearly_revenue_kpi(execution_date):
    """
    Build yearly revenue KPI view from the order item fact table.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Gold-Yearly-Revenue-KPI") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    fact_path = "/data/gold/fact/order_item_fact"
    date_dim_path = "/data/gold/dimension/date_dim"
    kpi_view_path = "/data/gold/view/vw_yearly_revenue_kpis"

    # Read fact and dimension tables
    fact_df = spark.read.format("delta").load(fact_path)
    date_dim = spark.read.format("delta").load(date_dim_path)
    fact_df = fact_df.filter(col("delivery_status") == "Delivered")

    # Build yearly KPI view
    yearly_kpi = fact_df.join(
        date_dim,
        fact_df.order_date_dim_key == date_dim.date_dim_hk
    ).groupBy(
        date_dim.year
    ).agg(
        sum("subtotal").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        round(sum("subtotal") / count("order_id"), 2).alias("avg_revenue_per_order"),
        round(sum("subtotal") / count("order_item_id"), 2).alias("avg_revenue_per_item"),
        max("subtotal").alias("max_order_value")
    ).orderBy("year")

    # Write yearly KPI view to Delta table
    yearly_kpi.write \
        .format("delta") \
        .mode("overwrite") \
        .save(kpi_view_path)

    # Log metrics
    num_years = yearly_kpi.count()
    latest_year = yearly_kpi.agg({"year": "max"}).collect()[0][0]
    latest_revenue = yearly_kpi.filter(col("year") == latest_year) \
        .select("total_revenue").collect()[0][0]

    print(f"Built yearly revenue KPI view with {num_years} years of data")
    print(f"Latest year: {latest_year}, Revenue: ₹{latest_revenue:.2f}")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "kpi_view",
        "view": "vw_yearly_revenue_kpis",
        "execution_date": execution_date,
        "num_years": num_years,
        "latest_year": latest_year,
        "latest_revenue": latest_revenue
    }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    build_yearly_revenue_kpi(execution_date)