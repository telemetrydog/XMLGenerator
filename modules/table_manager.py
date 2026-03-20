"""
Creates Spark SQL tables and loads CSV data into them.
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
    Read a CSV file into a Spark DataFrame and persist it to a table.

    The CSV is read with permissive mode so invalid rows are still ingested
    (they'll be caught later by the validator). Columns that fail to parse
    (e.g. bad dates) are set to null.

    Args:
        spark: active SparkSession
        csv_path: path to the CSV file
        table_name: target table name
        schema: optional StructType; defaults to get_spark_schema()
        database: optional database prefix

    Returns:
        The DataFrame that was written to the table.
    """
    if schema is None:
        schema = get_spark_schema()

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(csv_path)
    )

    qualified = f"{database}.{table_name}" if database else table_name
    df.createOrReplaceTempView(table_name)
    spark.sql(f"DROP TABLE IF EXISTS {qualified}")
    df.write.saveAsTable(qualified)

    return df


def read_table(spark: SparkSession, table_name: str, database: str | None = None) -> DataFrame:
    """Read the full contents of a table as a DataFrame."""
    qualified = f"{database}.{table_name}" if database else table_name
    return spark.table(qualified)
