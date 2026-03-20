"""
Generates Spark SQL DDL (CREATE TABLE) from the ACORD 212/21208 schema configuration.
"""

from __future__ import annotations

import os

from config.schema_config import FIELD_DEFINITIONS


def generate_ddl(table_name: str, database: str | None = None) -> str:
    """
    Build a Spark-SQL CREATE TABLE statement from FIELD_DEFINITIONS.

    All columns are STRING (matching the CSV text format).

    Args:
        table_name: target table name
        database: optional database/schema prefix

    Returns:
        DDL string ready for spark.sql()
    """
    qualified = f"{database}.{table_name}" if database else table_name
    columns = []
    for fd in FIELD_DEFINITIONS:
        comment = fd["description"].replace("'", "''")
        columns.append(f"    {fd['column_name']} STRING COMMENT '{comment}'")

    columns_sql = ",\n".join(columns)
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
        f"{columns_sql}\n"
        f") USING DELTA"
    )
    return ddl


def save_ddl(ddl: str, output_path: str) -> str:
    """Write DDL to a .sql file and return the path."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(ddl)
    return output_path
