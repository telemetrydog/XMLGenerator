"""
Creates Spark SQL tables and loads CSV data into them.

All columns are STRING (matching the ACORD 21208 flat-CSV format).
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from config.schema_config import get_spark_schema


def create_table(spark: SparkSession, ddl: str) -> None:
    """Execute a CREATE TABLE DDL statement."""
    spark.sql(ddl)


def load_csv(
    spark: SparkSession,
    csv_path: str,
    table_name: str,
    schema: StructType | None = None,
    database: str | None = None,
) -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame and register as a temp view.

    All columns are STRING (no date parsing needed). Uses PERMISSIVE mode
    so rows with wrong column counts are still ingested.

    Args:
        spark: active SparkSession
        csv_path: path to the CSV file
        table_name: target table / temp-view name
        schema: optional StructType; defaults to get_spark_schema()
        database: optional database prefix (used only for persistent table)

    Returns:
        The DataFrame.
    """
    if schema is None:
        schema = get_spark_schema()

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(csv_path)
    )

    df.createOrReplaceTempView(table_name)

    # Persist to managed table only when a database is explicitly given
    # (skipped on serverless where the default catalog may not be writable)
    if database:
        qualified = f"{database}.{table_name}"
        spark.sql(f"DROP TABLE IF EXISTS {qualified}")
        df.write.saveAsTable(qualified)

    return df


def read_table(
    spark: SparkSession,
    table_name: str,
    database: str | None = None,
) -> DataFrame:
    """Read the full contents of a table as a DataFrame."""
    qualified = f"{database}.{table_name}" if database else table_name
    return spark.table(qualified)
