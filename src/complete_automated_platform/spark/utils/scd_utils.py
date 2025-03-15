"""
Utility functions for implementing Slowly Changing Dimension Type 2
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws, when, array
)
from typing import List, Optional

def apply_scd_type2(
    spark: SparkSession,
    source_df: DataFrame,
    dim_df: DataFrame,
    natural_key: str,
    surrogate_key: str,
    tracking_columns: List[str],
    effective_start_date: str = "eff_start_date",
    effective_end_date: str = "eff_end_date",
    current_flag: str = "is_current"
) -> DataFrame:
    """
    Apply SCD Type 2 logic to update a dimension table.

    Args:
        spark: SparkSession
        source_df: Source DataFrame with new/changed data
        dim_df: Target dimension DataFrame
        natural_key: Natural key column name (e.g., 'customer_id')
        surrogate_key: Surrogate key column name (e.g., 'customer_hk')
        tracking_columns: List of columns to track for changes
        effective_start_date: Column name for effective start date
        effective_end_date: Column name for effective end date
        current_flag: Column name for current record flag

    Returns:
        Updated dimension DataFrame with SCD Type 2 logic applied
    """

    # Rename source columns to avoid conflicts
    source_columns = [natural_key] + tracking_columns
    source_renamed = source_df.select([
        col(c).alias(f"source_{c}") for c in source_columns
    ])

    # Rename dimension columns for the join
    dim_renamed = dim_df.select([
        col(c).alias(f"dim_{c}") if c in source_columns else col(c)
        for c in dim_df.columns
    ])

    # Add hash values to detect changes
    source_hash = source_df.withColumn(
        "source_hash",
        sha2(concat_ws("||", *[col(c) for c in tracking_columns]), 256)
    )

    dim_hash = dim_df.withColumn(
        "dim_hash",
        sha2(concat_ws("||", *[col(c) for c in tracking_columns]), 256)
    )

    # 1. Find records that need to be expired (existing records that changed)
    changed_records = dim_hash.join(
        source_hash,
        (dim_hash[natural_key] == source_hash[natural_key]) &
        (dim_hash["dim_hash"] != source_hash["source_hash"]) &
        (dim_hash[current_flag] == True),
        "inner"
    ).select(
        dim_hash[surrogate_key],
        dim_hash[natural_key],
        lit(current_timestamp()).alias(effective_end_date),
        lit(False).alias(current_flag)
    )

    # 2. Apply updates to expire old records
    dim_expired = dim_df.join(
        changed_records,
        dim_df[surrogate_key] == changed_records[surrogate_key],
        "left_outer"
    ).select(
        dim_df["*"],
        when(
            changed_records[surrogate_key].isNotNull(),
            changed_records[effective_end_date]
        ).otherwise(dim_df[effective_end_date]).alias(effective_end_date + "_new"),
        when(
            changed_records[surrogate_key].isNotNull(),
            changed_records[current_flag]
        ).otherwise(dim_df[current_flag]).alias(current_flag + "_new")
    ).drop(effective_end_date, current_flag) \
     .withColumnRenamed(effective_end_date + "_new", effective_end_date) \
     .withColumnRenamed(current_flag + "_new", current_flag)

    # 3. Find records that need to be inserted (new records or changed records)
    # New records that don't exist in dimension
    new_records = source_hash.join(
        dim_hash,
        source_hash[natural_key] == dim_hash[natural_key],
        "left_anti"
    )

    # Changed records that need new versions
    changed_record_sources = source_hash.join(
        changed_records,
        source_hash[natural_key] == changed_records[natural_key],
        "inner"
    )

    # Combine new and changed for insertion
    records_to_insert = new_records.unionByName(
        changed_record_sources,
        allowMissingColumns=True
    )

    # Generate insert DataFrame with surrogate keys and SCD attributes
    if records_to_insert.count() > 0:
        # Select all columns from source, plus SCD columns
        columns_to_select = [col(c) for c in source_df.columns if c != surrogate_key]

        insert_df = records_to_insert.select(columns_to_select) \
            .withColumn(
                surrogate_key,
                sha2(concat_ws("||",
                    col(natural_key),
                    current_timestamp(),
                    sha2(concat_ws("||", *[col(c) for c in tracking_columns]), 256)
                ), 256)
            ) \
            .withColumn(effective_start_date, current_timestamp()) \
            .withColumn(effective_end_date, lit(None).cast("timestamp")) \
            .withColumn(current_flag, lit(True))

        # 4. Combine expired and new records
        result_df = dim_expired.unionByName(insert_df, allowMissingColumns=True)
    else:
        # No changes, return original dimension
        result_df = dim_expired

    return result_df

def merge_scd_type2_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_path: str,
    natural_key: str,
    surrogate_key: str,
    tracking_columns: List[str],
    effective_start_date: str = "eff_start_date",
    effective_end_date: str = "eff_end_date",
    current_flag: str = "is_current"
) -> None:
    """
    Merge SCD Type 2 changes directly into a Delta table.
    Uses Delta Lake merge capability for more efficient updates.

    Args:
        spark: SparkSession
        source_df: Source DataFrame with new/changed data
        target_table_path: Path to target Delta table
        natural_key: Natural key column name
        surrogate_key: Surrogate key column name
        tracking_columns: List of columns to track for changes
        effective_start_date: Column name for effective start date
        effective_end_date: Column name for effective end date
        current_flag: Column name for current record flag
    """
    from delta.tables import DeltaTable

    # Check if target table exists
    target_exists = spark._jvm.org.apache.hadoop.fs.Path(target_table_path).getFileSystem(
        spark._jsparkSession.sparkContext._jsc.hadoopConfiguration()
    ).exists(spark._jvm.org.apache.hadoop.fs.Path(target_table_path))

    if not target_exists:
        # First-time dimension table creation
        columns_to_select = source_df.columns

        # Create the dimension table with SCD columns
        dim_df = source_df \
            .withColumn(
                surrogate_key,
                sha2(concat_ws("||",
                    col(natural_key),
                    sha2(concat_ws("||", *[col(c) for c in tracking_columns]), 256)
                ), 256)
            ) \
            .withColumn(effective_start_date, current_timestamp()) \
            .withColumn(effective_end_date, lit(None).cast("timestamp")) \
            .withColumn(current_flag, lit(True))

        dim_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(target_table_path)

        return

    # Load the target table as a Delta table
    target_table = DeltaTable.forPath(spark, target_table_path)

    # Add hash values to source to detect changes
    source_with_hash = source_df.withColumn(
        "source_hash",
        sha2(concat_ws("||", *[col(c) for c in tracking_columns]), 256)
    )

    # Define the merge condition
    merge_condition = (
        f"target.{natural_key} = source.{natural_key} AND " +
        f"target.{current_flag} = true AND " +
        f"target.dim_hash != source.source_hash"
    )

    # Update the target table
    target_table.alias("target") \
        .withColumn("dim_hash", sha2(concat_ws("||", *[col(c) for c in tracking_columns]), 256)) \
        .merge(
            source_with_hash.alias("source"),
            merge_condition
        ) \
        .whenMatched() \
        .updateExpr({
            effective_end_date: "current_timestamp()",
            current_flag: "false"
        }) \
        .execute()

    # Get the records that need new versions
    new_version_condition = (
        f"target.{natural_key} = source.{natural_key} AND " +
        f"target.{current_flag} = false AND " +
        f"target.{effective_end_date} = current_timestamp()"
    )

    # Identify records that need new versions
    new_versions = target_table.toDF().alias("target") \
        .join(
            source_with_hash.alias("source"),
            new_version_condition,
            "inner"
        ) \
        .select("source.*")

    # Add new records that don't exist in target
    not_exists_condition = f"NOT EXISTS (SELECT 1 FROM {target_table_path} WHERE {natural_key} = source.{natural_key})"

    new_records = source_with_hash.filter(not_exists_condition)

    # Combine all records to insert
    records_to_insert = new_versions.unionByName(new_records, allowMissingColumns=True)

    if records_to_insert.count() > 0:
        # Insert new versions with SCD columns
        insert_df = records_to_insert \
            .withColumn(
                surrogate_key,
                sha2(concat_ws("||",
                    col(natural_key),
                    current_timestamp(),
                    col("source_hash")
                ), 256)
            ) \
            .withColumn(effective_start_date, current_timestamp()) \
            .withColumn(effective_end_date, lit(None).cast("timestamp")) \
            .withColumn(current_flag, lit(True)) \
            .drop("source_hash")

        # Write new versions to target
        insert_df.write \
            .format("delta") \
            .mode("append") \
            .save(target_table_path)