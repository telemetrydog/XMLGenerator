"""Tests for scorecard generator module."""

import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from modules.csv_generator import generate_sample_csv
from modules.table_manager import load_csv
from modules.xml_generator import generate_all_xmls
from modules.xml_validator import validate_all, ValidationResult
from modules.scorecard_generator import (
    generate_scorecard,
    save_scorecard,
    sort_xml_files,
    SCORECARD_SCHEMA,
)


def _run_pipeline(spark, tmp_dir, num_valid=7, num_invalid=2):
    csv_path = os.path.join(tmp_dir, "sample.csv")
    generate_sample_csv(csv_path, num_valid=num_valid, num_invalid=num_invalid)
    uid = uuid.uuid4().hex[:8]
    df = load_csv(spark, csv_path, f"sct_{uid}")
    out_dir = os.path.join(tmp_dir, "xml_out")
    xml_results = generate_all_xmls(df, out_dir)
    validation_results = validate_all(xml_results)
    return xml_results, validation_results


class TestGenerateScorecard:
    def test_scorecard_row_count(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir)
        sc = generate_scorecard(spark, validation_results, xml_results)
        assert sc.count() == 9

    def test_scorecard_schema(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir, 2, 0)
        sc = generate_scorecard(spark, validation_results, xml_results)
        assert sc.schema == SCORECARD_SCHEMA

    def test_scorecard_has_pass_and_fail(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir, 7, 2)
        sc = generate_scorecard(spark, validation_results, xml_results)
        statuses = [r["Status"] for r in sc.collect()]
        assert "PASS" in statuses
        assert "FAIL" in statuses

    def test_scorecard_error_details_null_for_pass(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir, 3, 0)
        sc = generate_scorecard(spark, validation_results, xml_results)
        for row in sc.collect():
            if row["Status"] == "PASS":
                assert row["ErrorDetails"] is None

    def test_scorecard_error_details_populated_for_fail(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir, 0, 2)
        sc = generate_scorecard(spark, validation_results, xml_results)
        for row in sc.collect():
            if row["Status"] == "FAIL":
                assert row["ErrorDetails"] is not None
                assert len(row["ErrorDetails"]) > 0


class TestSaveScorecard:
    def test_save_creates_csv(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir, 2, 0)
        sc = generate_scorecard(spark, validation_results, xml_results)
        path = os.path.join(tmp_dir, "scorecard.csv")
        result = save_scorecard(sc, path)
        assert os.path.exists(result)
        with open(result) as f:
            lines = f.readlines()
        assert len(lines) == 3  # header + 2 rows


class TestSortXmlFiles:
    def test_files_sorted_correctly(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir, 7, 2)
        success_dir = os.path.join(tmp_dir, "success")
        fail_dir = os.path.join(tmp_dir, "unsuccessful")
        sorted_files = sort_xml_files(validation_results, xml_results, success_dir, fail_dir)
        assert len(sorted_files["success"]) >= 1
        assert len(sorted_files["unsuccessful"]) >= 1
        for f in sorted_files["success"]:
            assert os.path.exists(f)
        for f in sorted_files["unsuccessful"]:
            assert os.path.exists(f)

    def test_all_files_accounted_for(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir, 7, 2)
        success_dir = os.path.join(tmp_dir, "success")
        fail_dir = os.path.join(tmp_dir, "unsuccessful")
        sorted_files = sort_xml_files(validation_results, xml_results, success_dir, fail_dir)
        total = len(sorted_files["success"]) + len(sorted_files["unsuccessful"])
        assert total == 9

    def test_success_dir_only_pass(self, spark, tmp_dir):
        xml_results, validation_results = _run_pipeline(spark, tmp_dir, 3, 0)
        success_dir = os.path.join(tmp_dir, "success")
        fail_dir = os.path.join(tmp_dir, "unsuccessful")
        sorted_files = sort_xml_files(validation_results, xml_results, success_dir, fail_dir)
        assert len(sorted_files["success"]) == 3
        assert len(sorted_files["unsuccessful"]) == 0
