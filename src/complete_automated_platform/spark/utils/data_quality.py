"""
Utility functions for data quality checks in the ETL pipeline
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, isnull, isnan, when, sum, lit
from typing import Dict, List, Any, Tuple


class DataQualityCheck:
    """
    A class to perform data quality checks on DataFrames
    """

    def __init__(self, spark: SparkSession, df: DataFrame, entity_name: str):
        """
        Initialize the DataQualityCheck with a DataFrame and entity name.

        Args:
            spark: SparkSession
            df: DataFrame to check
            entity_name: Name of the entity (e.g., 'customer', 'order')
        """
        self.spark = spark
        self.df = df
        self.entity_name = entity_name
        self.results = {}

    def check_null_values(self, columns: List[str] = None) -> Dict[str, int]:
        """
        Check for NULL values in specified columns.

        Args:
            columns: List of column names to check (None = all columns)

        Returns:
            Dictionary with column names and NULL count
        """
        if columns is None:
            columns = self.df.columns

        null_counts = {}
        for column in columns:
            null_count = self.df.filter(
                col(column).isNull() |
                isnan(col(column))
            ).count()

            null_counts[column] = null_count

        # Add to results
        self.results["null_checks"] = null_counts

        return null_counts

    def check_primary_key(self, pk_column: str) -> Dict[str, Any]:
        """
        Check primary key column for uniqueness and NULL values.

        Args:
            pk_column: Primary key column name

        Returns:
            Dictionary with uniqueness check results
        """
        # Count total records
        total_count = self.df.count()

        # Count distinct values in PK column
        distinct_count = self.df.select(pk_column).distinct().count()

        # Count NULL values in PK column
        null_count = self.df.filter(col(pk_column).isNull()).count()

        # Check results
        is_unique = (distinct_count + null_count) == total_count

        result = {
            "pk_column": pk_column,
            "total_records": total_count,
            "distinct_values": distinct_count,
            "null_values": null_count,
            "is_unique": is_unique
        }

        # Add to results
        self.results["pk_check"] = result

        return result

    def check_foreign_keys(self, fk_columns: Dict[str, Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
        """
        Check foreign key columns for referential integrity.

        Args:
            fk_columns: Dictionary mapping FK column names to (ref_table, ref_column) tuples

        Returns:
            Dictionary with FK check results
        """
        fk_results = {}

        for fk_column, (ref_table, ref_column) in fk_columns.items():
            # Count NULL values in FK column
            null_count = self.df.filter(col(fk_column).isNull()).count()

            # Get distinct FK values
            fk_values = self.df.select(fk_column).distinct()

            # Count distinct FK values
            distinct_count = fk_values.count() - (1 if null_count > 0 else 0)

            # Load reference table
            ref_df = self.spark.read.format("delta").load(ref_table)

            # Get distinct reference values
            ref_values = ref_df.select(ref_column).distinct()

            # Find orphaned values (in FK but not in reference)
            orphaned = fk_values.join(
                ref_values,
                fk_values[fk_column] == ref_values[ref_column],
                "left_anti"
            ).filter(col(fk_column).isNotNull())

            orphaned_count = orphaned.count()

            result = {
                "fk_column": fk_column,
                "ref_table": ref_table,
                "ref_column": ref_column,
                "distinct_values": distinct_count,
                "null_values": null_count,
                "orphaned_values": orphaned_count,
                "orphaned_examples": orphaned.limit(5).collect() if orphaned_count > 0 else []
            }

            fk_results[fk_column] = result

        # Add to results
        self.results["fk_checks"] = fk_results

        return fk_results

    def check_value_distribution(self, column: str, bins: int = 10) -> Dict[str, Any]:
        """
        Check value distribution for a numeric column.

        Args:
            column: Column name
            bins: Number of bins for histogram

        Returns:
            Dictionary with distribution statistics
        """
        # Get basic statistics
        stats = self.df.select(column).summary().collect()

        # Create bins for histogram
        try:
            min_val = float(stats[3][1])  # "min" row
            max_val = float(stats[7][1])  # "max" row

            bin_width = (max_val - min_val) / bins

            histogram = []

            for i in range(bins):
                bin_min = min_val + i * bin_width
                bin_max = min_val + (i + 1) * bin_width

                count = self.df.filter(
                    (col(column) >= bin_min) & (col(column) < bin_max)
                ).count()

                histogram.append({
                    "bin": i,
                    "min": bin_min,
                    "max": bin_max,
                    "count": count
                })

            result = {
                "column": column,
                "min": min_val,
                "max": max_val,
                "mean": float(stats[1][1]),  # "mean" row
                "stddev": float(stats[2][1]),  # "stddev" row
                "histogram": histogram
            }
        except:
            # Not a numeric column or couldn't calculate histogram
            result = {
                "column": column,
                "error": "Could not calculate distribution for non-numeric column"
            }

        # Add to results
        self.results["distribution"] = result

        return result

    def check_duplicate_records(self, columns: List[str] = None) -> Dict[str, Any]:
        """
        Check for duplicate records.

        Args:
            columns: List of column names to check for duplicates (None = all columns)

        Returns:
            Dictionary with duplicate check results
        """
        if columns is None:
            columns = self.df.columns

        # Count duplicates
        dup_counts = self.df.groupBy(*columns).count().filter("count > 1")
        dup_count = dup_counts.count()

        result = {
            "columns": columns,
            "duplicate_count": dup_count,
            "examples": dup_counts.limit(5).collect() if dup_count > 0 else []
        }

        # Add to results
        self.results["duplicate_check"] = result

        return result

    def run_all_checks(
            self,
            pk_column: str = None,
            fk_columns: Dict[str, Tuple[str, str]] = None,
            distribution_column: str = None
    ) -> Dict[str, Any]:
        """
        Run all data quality checks.

        Args:
            pk_column: Primary key column name
            fk_columns: Dictionary mapping FK column names to (ref_table, ref_column) tuples
            distribution_column: Column name for distribution check

        Returns:
            Dictionary with all check results
        """
        # Run checks
        self.check_null_values()

        if pk_column:
            self.check_primary_key(pk_column)

        if fk_columns:
            self.check_foreign_keys(fk_columns)

        if distribution_column:
            self.check_value_distribution(distribution_column)

        return self.results

    def log_results(self, log_level: str = "INFO"):
        """
        Log the data quality check results.

        Args:
            log_level: Log level (INFO, WARNING, ERROR)
        """
        import logging

        # Set log level
        if log_level == "WARNING":
            log_func = logging.warning
        elif log_level == "ERROR":
            log_func = logging.error
        else:
            log_func = logging.info

        # Log entity name
        log_func(f"Data Quality Check Results for {self.entity_name}")

        # Log null checks
        if "null_checks" in self.results:
            log_func(f"NULL Value Checks:")
            for column, count in self.results["null_checks"].items():
                if count > 0:
                    log_func(f"  - Column '{column}': {count} NULL values")

        # Log PK check
        if "pk_check" in self.results:
            pk_check = self.results["pk_check"]
            log_func(f"Primary Key Check for {pk_check['pk_column']}:")
            log_func(f"  - Is Unique: {pk_check['is_unique']}")
            log_func(f"  - Total Records: {pk_check['total_records']}")
            log_func(f"  - Distinct Values: {pk_check['distinct_values']}")
            log_func(f"  - NULL Values: {pk_check['null_values']}")

        # Log FK checks
        if "fk_checks" in self.results:
            log_func(f"Foreign Key Checks:")
            for fk_column, fk_check in self.results["fk_checks"].items():
                log_func(f"  - FK Column '{fk_column}' -> {fk_check['ref_table']}.{fk_check['ref_column']}:")
                log_func(f"    - Distinct Values: {fk_check['distinct_values']}")
                log_func(f"    - NULL Values: {fk_check['null_values']}")
                log_func(f"    - Orphaned Values: {fk_check['orphaned_values']}")

        # Log duplicate check
        if "duplicate_check" in self.results:
            dup_check = self.results["duplicate_check"]
            log_func(f"Duplicate Records Check:")
            log_func(f"  - Columns: {', '.join(dup_check['columns'])}")
            log_func(f"  - Duplicate Count: {dup_check['duplicate_count']}")

        # Log overall status
        has_issues = (
                any(count > 0 for count in self.results.get("null_checks", {}).values()) or
                (self.results.get("pk_check", {}).get("is_unique") == False) or
                any(check.get("orphaned_values", 0) > 0 for check in self.results.get("fk_checks", {}).values()) or
                self.results.get("duplicate_check", {}).get("duplicate_count", 0) > 0
        )

        if has_issues:
            log_func(f"Data Quality Issues Found for {self.entity_name}")
        else:
            logging.info(f"No Data Quality Issues Found for {self.entity_name}")