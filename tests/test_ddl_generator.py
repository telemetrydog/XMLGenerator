"""Tests for DDL generator module."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import FIELD_DEFINITIONS
from modules.ddl_generator import generate_ddl, save_ddl


class TestGenerateDDL:
    def test_contains_all_columns(self):
        ddl = generate_ddl("values_inquiry")
        for fd in FIELD_DEFINITIONS:
            assert fd["column_name"] in ddl

    def test_required_columns_not_null(self):
        ddl = generate_ddl("values_inquiry")
        for fd in FIELD_DEFINITIONS:
            if fd["required"]:
                assert f"{fd['column_name']} STRING NOT NULL" in ddl or \
                       f"{fd['column_name']} DATE NOT NULL" in ddl

    def test_optional_columns_nullable(self):
        ddl = generate_ddl("values_inquiry")
        for fd in FIELD_DEFINITIONS:
            if not fd["required"]:
                col_line = [l for l in ddl.splitlines() if fd["column_name"] in l][0]
                assert "NOT NULL" not in col_line

    def test_uses_delta_format(self):
        ddl = generate_ddl("values_inquiry")
        assert "USING DELTA" in ddl

    def test_create_table_if_not_exists(self):
        ddl = generate_ddl("values_inquiry")
        assert "CREATE TABLE IF NOT EXISTS" in ddl

    def test_database_prefix(self):
        ddl = generate_ddl("values_inquiry", database="insurance_db")
        assert "insurance_db.values_inquiry" in ddl

    def test_comments_included(self):
        ddl = generate_ddl("values_inquiry")
        assert "COMMENT" in ddl

    def test_column_count(self):
        ddl = generate_ddl("values_inquiry")
        column_lines = [l.strip() for l in ddl.splitlines() if l.strip().startswith(("Trans", "Inq", "No", "Test", "Hold", "Pol", "Prod", "Car", "Line", "Policy"))]
        assert len(column_lines) == len(FIELD_DEFINITIONS)


class TestSaveDDL:
    def test_save_creates_file(self, tmp_dir):
        ddl = generate_ddl("values_inquiry")
        path = os.path.join(tmp_dir, "output", "ddl.sql")
        result = save_ddl(ddl, path)
        assert os.path.exists(result)
        with open(result) as f:
            content = f.read()
        assert content == ddl

    def test_save_creates_parent_dirs(self, tmp_dir):
        ddl = generate_ddl("test_table")
        path = os.path.join(tmp_dir, "nested", "dir", "ddl.sql")
        save_ddl(ddl, path)
        assert os.path.exists(path)
