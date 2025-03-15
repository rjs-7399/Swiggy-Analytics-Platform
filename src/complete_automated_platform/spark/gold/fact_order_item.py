"""
Gold Layer: Order Item Fact Table
Builds the central fact table connecting all dimensions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, sha2, concat_ws, lit, expr,
    coalesce, broadcast, count, sum, avg, max
)
import os
from datetime import datetime


def build_order_item_fact(execution_date):
    """
    Build order item fact table by joining all relevant dimensions.

    Args:
        execution_date: The execution date for the pipeline run
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Swiggy-Gold-Order-Item-Fact") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define paths
    silver_order_items_path = "/data/silver/order_items"
    silver_orders_path = "/data/silver/orders"
    silver_delivery_path = "/data/silver/delivery"
    gold_path = "/data/gold/fact/order_item_fact"

    # Load silver transactional data
    order_items_df = spark.read.format("delta").load(silver_order_items_path) \
        .filter(col("_execution_date") == execution_date)

    orders_df = spark.read.format("delta").load(silver_orders_path) \
        .filter(col("_execution_date") == execution_date)

    delivery_df = spark.read.format("delta").load(silver_delivery_path) \
        .filter(col("_execution_date") == execution_date)

    # Load dimension tables for lookups (using only current records for dimensions)
    customer_dim = spark.read.format("delta").load("/data/gold/dimension/customer_dim") \
        .filter(col("is_current") == True)

    address_dim = spark.read.format("delta").load("/data/gold/dimension/customeraddress_dim") \
        .filter(col("is_current") == True)

    restaurant_dim = spark.read.format("delta").load("/data/gold/dimension/restaurant_dim") \
        .filter(col("is_current") == True)

    location_dim = spark.read.format("delta").load("/data/gold/dimension/restaurant_location_dim") \
        .filter(col("is_current") == True)

    menu_dim = spark.read.format("delta").load("/data/gold/dimension/menu_dim") \
        .filter(col("is_current") == True)

    agent_dim = spark.read.format("delta").load("/data/gold/dimension/deliveryagent_dim") \
        .filter(col("is_current") == True)

    date_dim = spark.read.format("delta").load("/data/gold/dimension/date_dim")

    # Join all tables to create the fact table
    # Start with order items
    fact_df = order_items_df

    # Join with orders
    fact_df = fact_df.join(
        orders_df,
        fact_df.order_id_fk == orders_df.order_id,
        "left"
    )

    # Join with delivery
    fact_df = fact_df.join(
        delivery_df,
        fact_df.order_id_fk == delivery_df.order_id_fk,
        "left"
    )

    # Join with dimensions
    # Use broadcast join for dimensions since they're typically smaller
    fact_df = fact_df.join(
        broadcast(customer_dim),
        fact_df.customer_id_fk == customer_dim.customer_id,
        "left"
    ).join(
        broadcast(address_dim),
        (fact_df.customer_id_fk == address_dim.customer_id_fk) &
        (col("delivery_address") == address_dim.address_id),
        "left"
    ).join(
        broadcast(restaurant_dim),
        fact_df.restaurant_id_fk == restaurant_dim.restaurant_id,
        "left"
    ).join(
        broadcast(location_dim),
        restaurant_dim.location_id_fk == location_dim.location_id,
        "left"
    ).join(
        broadcast(menu_dim),
        fact_df.menu_id_fk == menu_dim.menu_id,
        "left"
    ).join(
        broadcast(agent_dim),
        fact_df.delivery_agent_id_fk == agent_dim.delivery_agent_id,
        "left"
    ).join(
        broadcast(date_dim),
        fact_df.order_date.cast("date") == date_dim.calendar_date,
        "left"
    )

    # Select only the columns needed for the fact table
    fact_df = fact_df.select(
        # Surrogate key for the fact table
        sha2(concat_ws("||",
                       col("order_item_id"),
                       col("order_id")
                       ), 256).alias("order_item_fact_sk"),

        # Natural keys
        col("order_item_id"),
        col("order_id"),

        # Dimension foreign keys
        col("customer_hk").alias("customer_dim_key"),
        col("customer_address_hk").alias("customer_address_dim_key"),
        col("restaurant_hk").alias("restaurant_dim_key"),
        col("restaurant_location_hk").alias("restaurant_location_dim_key"),
        col("menu_dim_hk").alias("menu_dim_key"),
        col("delivery_agent_hk").alias("delivery_agent_dim_key"),
        col("date_dim_hk").alias("order_date_dim_key"),

        # Measures
        col("quantity"),
        col("price"),
        col("subtotal"),

        # Additional attributes
        col("delivery_status"),
        col("estimated_time"),

        # Metadata
        current_timestamp().alias("_processing_time"),
        lit(execution_date).alias("_execution_date")
    )

    # Check for potential data issues
    null_count = fact_df.filter(
        col("customer_dim_key").isNull() |
        col("restaurant_dim_key").isNull() |
        col("menu_dim_key").isNull()
    ).count()

    if null_count > 0:
        print(f"WARNING: {null_count} records have NULL dimension keys")

    # Determine write mode (append vs overwrite)
    # For a fact table, usually we use append, but checking if this is the first run
    fact_exists = spark._jvm.org.apache.hadoop.fs.Path(gold_path).getFileSystem(
        spark._jsparkSession.sparkContext._jsc.hadoopConfiguration()
    ).exists(spark._jvm.org.apache.hadoop.fs.Path(gold_path))

    write_mode = "append" if fact_exists else "overwrite"

    # Write fact table to gold layer
    fact_df.write \
        .format("delta") \
        .mode(write_mode) \
        .save(gold_path)

    # Get metrics for reporting
    row_count = fact_df.count()

    # Additional metrics for business insights
    revenue = fact_df.agg(sum("subtotal").alias("total_revenue")).collect()[0]["total_revenue"]
    order_count = fact_df.select("order_id").distinct().count()
    avg_order_value = revenue / order_count if order_count > 0 else 0

    print(f"Processed {row_count} order item facts with {order_count} unique orders and ₹{revenue:.2f} total revenue")

    # Return metrics for potential XCom usage in Airflow
    return {
        "entity": "order_item",
        "layer": "gold",
        "fact": "order_item_fact",
        "execution_date": execution_date,
        "row_count": row_count,
        "order_count": order_count,
        "total_revenue": revenue,
        "avg_order_value": avg_order_value,
        "null_dim_count": null_count
    }


if __name__ == "__main__":
    # For local testing
    execution_date = datetime.now().strftime("%Y-%m-%d")
    build_order_item_fact(execution_date)