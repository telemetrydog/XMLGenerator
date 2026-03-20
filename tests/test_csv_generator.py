"""Tests for CSV generator module."""

import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import get_column_names
from modules.csv_generator import generate_sample_csv, get_sample_rows


class TestGenerateSampleCSV:
    def test_file_created(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        result = generate_sample_csv(path)
        assert os.path.exists(result)

    def test_header_matches_schema(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        generate_sample_csv(path)
        with open(path) as f:
            reader = csv.reader(f)
            header = next(reader)
        assert header == get_column_names()

    def test_default_row_count(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        generate_sample_csv(path)
        with open(path) as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == 10  # 1 header + 9 data

    def test_custom_row_counts(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        generate_sample_csv(path, num_valid=3, num_invalid=1)
        with open(path) as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == 5  # 1 header + 4 data

    def test_trans_type_always_212(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        generate_sample_csv(path)
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                assert row["TransType"] == "212"

    def test_invalid_row_has_bad_date(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        generate_sample_csv(path)
        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        invalid_date_row = rows[7]  # 8th data row (0-indexed)
        assert invalid_date_row["TransExeDate"] == "03-19-2026"

    def test_invalid_row_has_missing_polnumber(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        generate_sample_csv(path)
        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        invalid_pol_row = rows[8]  # 9th data row
        assert invalid_pol_row["PolNumber"] == ""

    def test_unique_policy_numbers(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        generate_sample_csv(path)
        with open(path) as f:
            reader = csv.DictReader(f)
            pol_numbers = [r["PolNumber"] for r in reader if r["PolNumber"]]
        assert len(pol_numbers) == len(set(pol_numbers))


class TestGetSampleRows:
    def test_returns_all_rows_with_invalid(self):
        rows = get_sample_rows(include_invalid=True)
        assert len(rows) == 9

    def test_returns_only_valid(self):
        rows = get_sample_rows(include_invalid=False)
        assert len(rows) == 7

    def test_all_rows_have_schema_keys(self):
        columns = set(get_column_names())
        for row in get_sample_rows():
            assert set(row.keys()) == columns
