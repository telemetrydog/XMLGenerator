"""Tests for CSV generator module (load / prepare / filter)."""

from __future__ import annotations

import csv
import os
import shutil
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import get_column_names
from modules.csv_generator import load_csv_rows, prepare_csv, get_sample_rows
from tests.conftest import write_test_csv, make_test_row


class TestLoadCsvRows:
    def test_returns_correct_count(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        write_test_csv(path, num_valid=5)
        rows = load_csv_rows(path)
        assert len(rows) == 5

    def test_rows_have_schema_keys(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        write_test_csv(path, num_valid=1)
        rows = load_csv_rows(path)
        columns = set(get_column_names())
        assert set(rows[0].keys()) == columns

    def test_pol_numbers_preserved(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        write_test_csv(path, num_valid=3)
        rows = load_csv_rows(path)
        pol_numbers = [r["PolNumber"] for r in rows]
        assert "ANN-2026-00001" in pol_numbers
        assert "ANN-2026-00002" in pol_numbers
        assert "ANN-2026-00003" in pol_numbers

    def test_header_matches_schema(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        write_test_csv(path, num_valid=1)
        with open(path) as f:
            reader = csv.reader(f)
            header = next(reader)
        assert header == get_column_names()


class TestPrepareCsv:
    def test_copies_file(self, tmp_dir):
        src = os.path.join(tmp_dir, "source.csv")
        write_test_csv(src, num_valid=2)
        dest = os.path.join(tmp_dir, "dest", "out.csv")
        result = prepare_csv(src, dest)
        assert os.path.exists(result)
        with open(result) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 2

    def test_skips_copy_when_same_path(self, tmp_dir):
        src = os.path.join(tmp_dir, "same.csv")
        write_test_csv(src, num_valid=1)
        result = prepare_csv(src, src)
        assert result == src
        assert os.path.exists(result)

    def test_overwrites_existing_dest(self, tmp_dir):
        src = os.path.join(tmp_dir, "source.csv")
        write_test_csv(src, num_valid=3)
        dest = os.path.join(tmp_dir, "dest.csv")
        write_test_csv(dest, num_valid=1)
        prepare_csv(src, dest)
        rows = load_csv_rows(dest)
        assert len(rows) == 3


class TestGetSampleRows:
    def test_returns_all_rows(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        rows_data = [
            make_test_row("POL-001", ExpectedToPass="1"),
            make_test_row("POL-002", ExpectedToPass="1"),
            make_test_row("POL-003", ExpectedToPass="0", PolNumber=""),
        ]
        write_test_csv(path, rows=rows_data)
        rows = get_sample_rows(path, include_invalid=True)
        assert len(rows) == 3

    def test_filters_invalid_rows(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        rows_data = [
            make_test_row("POL-001", ExpectedToPass="1"),
            make_test_row("POL-002", ExpectedToPass="1"),
            make_test_row("POL-003", ExpectedToPass="0", PolNumber=""),
        ]
        write_test_csv(path, rows=rows_data)
        rows = get_sample_rows(path, include_invalid=False)
        assert len(rows) == 2
        assert all(r["ExpectedToPass"] == "1" for r in rows)

    def test_all_rows_have_schema_keys(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        write_test_csv(path, num_valid=2)
        columns = set(get_column_names())
        for row in get_sample_rows(path):
            assert set(row.keys()) == columns
