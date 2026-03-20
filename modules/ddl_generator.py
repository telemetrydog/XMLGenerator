"""
Generates Spark SQL DDL (CREATE TABLE) from the ACORD schema configuration.
"""

from __future__ import annotations

import os

from config.schema_config import FIELD_DEFINITIONS, SPARK_TYPE_MAP


_SPARK_TO_SQL_TYPE = {
    "STRING": "STRING",
    "DATE": "DATE",
}


def generate_ddl(table_name: str, database: str | None = None) -> str:
    """
    Build a Spark-SQL CREATE TABLE statement from FIELD_DEFINITIONS.

    Args:
        table_name: target table name (e.g. 'values_inquiry')
        database: optional database/schema prefix

    Returns:
        DDL string ready for spark.sql()
    """
    qualified = f"{database}.{table_name}" if database else table_name
    columns = []
    for fd in FIELD_DEFINITIONS:
        sql_type = _SPARK_TO_SQL_TYPE[fd["spark_type"]]
        not_null = " NOT NULL" if fd["required"] else ""
        comment = f' COMMENT \'{fd["description"]}\''
        columns.append(f"    {fd['column_name']} {sql_type}{not_null}{comment}")

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
