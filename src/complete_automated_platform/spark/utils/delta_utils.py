"""
Utility functions for working with Delta Lake in the ETL pipeline
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable
from typing import List, Dict, Any, Optional
import os


def initialize_spark_with_delta() -> SparkSession:
    """
    Initialize a Spark session with Delta Lake support.

    Returns:
        SparkSession with Delta Lake configuration
    """
    return SparkSession.builder \
        .appName("Swiggy-Data-Platform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def write_to_delta(
        df: DataFrame,
        path: str,
        mode: str = "append",
        partition_by: Optional[List[str]] = None
) -> None:
    """
    Write a DataFrame to Delta table.

    Args:
        df: DataFrame to write
        path: Target Delta table path
        mode: Write mode ('append', 'overwrite', 'error', 'ignore')
        partition_by: List of partition columns
    """
    writer = df.write \
        .format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)


def upsert_to_delta(
        spark: SparkSession,
        source_df: DataFrame,
        target_path: str,
        join_columns: List[str],
        update_columns: Optional[List[str]] = None,
        delete_condition: Optional[str] = None
) -> None:
    """
    Perform an upsert (update/insert) operation on a Delta table.

    Args:
        spark: SparkSession
        source_df: Source DataFrame with updates
        target_path: Target Delta table path
        join_columns: Columns to join on for matching records
        update_columns: Columns to update (None = all columns)
        delete_condition: Condition for deleting records (None = no deletes)
    """
    # Check if target exists
    target_exists = spark._jvm.org.apache.hadoop.fs.Path(target_path).getFileSystem(
        spark._jsparkSession.sparkContext._jsc.hadoopConfiguration()
    ).exists(spark._jvm.org.apache.hadoop.fs.Path(target_path))

    if not target_exists:
        # First-time write if target doesn't exist
        write_to_delta(source_df, target_path, "overwrite")
        return

    # Load target as DeltaTable
    target_table = DeltaTable.forPath(spark, target_path)

    # Prepare join condition
    join_condition = " AND ".join([f"target.{col} = source.{col}" for col in join_columns])

    # Prepare update expressions
    if update_columns:
        update_expr = {
            col: f"source.{col}"
            for col in update_columns
        }
    else:
        # Update all columns except join columns
        all_columns = source_df.columns
        update_expr = {
            col: f"source.{col}"
            for col in all_columns
            if col not in join_columns
        }

    # Add metadata columns
    update_expr["_updated_at"] = "current_timestamp()"

    # Prepare insert expressions
    insert_expr = {
        col: f"source.{col}"
        for col in source_df.columns
    }

    # Add metadata columns
    insert_expr["_updated_at"] = "current_timestamp()"
    insert_expr["_inserted_at"] = "current_timestamp()"

    # Start merge operation
    merge_builder = target_table.alias("target") \
        .merge(source_df.alias("source"), join_condition)

    # Add matched update clause
    merge_builder = merge_builder.whenMatchedUpdate(
        set=update_expr
    )

    # Add delete clause if specified
    if delete_condition:
        merge_builder = merge_builder.whenMatchedDelete(
            condition=delete_condition
        )

    # Add insert clause
    merge_builder = merge_builder.whenNotMatchedInsert(
        values=insert_expr
    )

    # Execute merge
    merge_builder.execute()


def vacuum_delta_table(
        spark: SparkSession,
        path: str,
        retention_hours: int = 168  # 7 days default
) -> None:
    """
    Vacuum a Delta table to remove old snapshots.

    Args:
        spark: SparkSession
        path: Delta table path
        retention_hours: Retention period in hours
    """
    if retention_hours < 168:
        # Set spark conf to allow shorter retention
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        print(f"WARNING: Setting Delta retention to {retention_hours} hours (less than 7 days)")

    # Load table and vacuum
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.vacuum(retention_hours)


def get_delta_history(
        spark: SparkSession,
        path: str,
        limit: int = 10
) -> DataFrame:
    """
    Get the history of a Delta table.

    Args:
        spark: SparkSession
        path: Delta table path
        limit: Maximum number of history entries to return

    Returns:
        DataFrame with Delta table history
    """
    delta_table = DeltaTable.forPath(spark, path)
    return delta_table.history(limit)


def optimize_delta_table(
        spark: SparkSession,
        path: str,
        zorder_by: Optional[List[str]] = None
) -> None:
    """
    Optimize a Delta table using OPTIMIZE and Z-ORDER.

    Args:
        spark: SparkSession
        path: Delta table path
        zorder_by: List of columns to Z-ORDER by
    """
    if zorder_by:
        # Use SQL for OPTIMIZE with Z-ORDER
        zorder_cols = ", ".join(zorder_by)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder_cols})")
    else:
        # Simple OPTIMIZE without Z-ORDER
        spark.sql(f"OPTIMIZE delta.`{path}`")


def compare_delta_versions(
        spark: SparkSession,
        path: str,
        version1: int,
        version2: int
) -> DataFrame:
    """
    Compare two versions of a Delta table.

    Args:
        spark: SparkSession
        path: Delta table path
        version1: First version number
        version2: Second version number

    Returns:
        DataFrame with differences between versions
    """
    # Read both versions
    df1 = spark.read.format("delta").option("versionAsOf", version1).load(path)
    df2 = spark.read.format("delta").option("versionAsOf", version2).load(path)

    # Register temporary views
    df1.createOrReplaceTempView("version1")
    df2.createOrReplaceTempView("version2")

    # Get all columns
    all_columns = df1.columns
    col_list = ", ".join(all_columns)

    # Query to find differences
    diff_query = f"""
    SELECT 'Only in Version {version1}' as diff_type, {col_list}
    FROM version1
    EXCEPT
    SELECT 'Only in Version {version1}' as diff_type, {col_list}
    FROM version2

    UNION ALL

    SELECT 'Only in Version {version2}' as diff_type, {col_list}
    FROM version2
    EXCEPT
    SELECT 'Only in Version {version2}' as diff_type, {col_list}
    FROM version1
    """

    return spark.sql(diff_query)