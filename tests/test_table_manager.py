"""Tests for table manager module."""

from __future__ import annotations

import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import get_column_names
from modules.table_manager import create_table, load_csv, read_table
from modules.ddl_generator import generate_ddl
from tests.conftest import write_test_csv, make_test_row


class TestCreateTable:
    def test_table_creation_does_not_raise(self, spark):
        uid = uuid.uuid4().hex[:8]
        ddl = generate_ddl(f"test_create_{uid}").replace("USING DELTA", "USING PARQUET")
        create_table(spark, ddl)
        tables = [t.name for t in spark.catalog.listTables()]
        assert f"test_create_{uid}" in tables


class TestLoadCSV:
    def test_load_returns_dataframe(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        write_test_csv(csv_path, num_valid=3)
        uid = uuid.uuid4().hex[:8]
        df = load_csv(spark, csv_path, f"test_load_{uid}")
        assert df is not None
        assert df.count() == 3

    def test_dataframe_has_correct_columns(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        write_test_csv(csv_path, num_valid=1)
        uid = uuid.uuid4().hex[:8]
        df = load_csv(spark, csv_path, f"test_cols_{uid}")
        assert df.columns == get_column_names()

    def test_valid_rows_parsed_correctly(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        write_test_csv(csv_path, num_valid=3)
        uid = uuid.uuid4().hex[:8]
        df = load_csv(spark, csv_path, f"test_valid_{uid}")
        rows = df.collect()
        assert len(rows) == 3
        assert rows[0]["PolNumber"] == "ANN-2026-00001"
        assert rows[0]["TransType_tc"] == "212"

    def test_empty_field_read_as_empty_string_or_null(self, spark, tmp_dir):
        row = make_test_row("ANN-EMPTY-TEST", DistributorClientAcctNum="")
        csv_path = os.path.join(tmp_dir, "sample.csv")
        write_test_csv(csv_path, rows=[row])
        uid = uuid.uuid4().hex[:8]
        df = load_csv(spark, csv_path, f"test_empty_{uid}")
        result = df.collect()[0]
        val = result["DistributorClientAcctNum"]
        assert val is None or val.strip() == ""


class TestReadTable:
    def test_read_table_returns_correct_count(self, spark, tmp_dir):
        csv_path = os.path.join(tmp_dir, "sample.csv")
        write_test_csv(csv_path, num_valid=5)
        uid = uuid.uuid4().hex[:8]
        tname = f"test_read_{uid}"
        load_csv(spark, csv_path, tname)
        df = read_table(spark, tname)
        assert df.count() == 5
