"""Tests for table manager module."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import get_spark_schema, get_column_names
from modules.csv_generator import generate_sample_csv
from modules.table_manager import create_table, load_csv, read_table
from modules.ddl_generator import generate_ddl


class TestCreateTable:
    def test_table_creation_does_not_raise(self, spark):
        ddl = generate_ddl("test_create_tbl").replace("USING DELTA", "USING PARQUET")
        create_table(spark, ddl)
        tables = [t.name for t in spark.catalog.listTables()]
        assert "test_create_tbl" in tables


class TestLoadCSV:
    def test_load_returns_dataframe(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        generate_sample_csv(csv_path)
        df = load_csv(spark, csv_path, "test_load_tbl")
        assert df is not None
        assert df.count() == 9

    def test_dataframe_has_correct_columns(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        generate_sample_csv(csv_path)
        df = load_csv(spark, csv_path, "test_cols_tbl")
        assert df.columns == get_column_names()

    def test_valid_rows_parsed_correctly(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        generate_sample_csv(csv_path, num_valid=3, num_invalid=0)
        df = load_csv(spark, csv_path, "test_valid_tbl")
        rows = df.collect()
        assert len(rows) == 3
        assert rows[0]["PolNumber"] == "ANN-2026-00001"
        assert rows[0]["TransType"] == "212"

    def test_invalid_date_becomes_null(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        generate_sample_csv(csv_path)
        df = load_csv(spark, csv_path, "test_nulldate_tbl")
        rows = df.collect()
        bad_date_row = [r for r in rows if r["PolNumber"] == "ANN-2026-00008"]
        assert len(bad_date_row) == 1
        assert bad_date_row[0]["TransExeDate"] is None


class TestReadTable:
    def test_read_table_returns_correct_count(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        generate_sample_csv(csv_path, num_valid=5, num_invalid=0)
        load_csv(spark, csv_path, "test_read_tbl")
        df = read_table(spark, "test_read_tbl")
        assert df.count() == 5
