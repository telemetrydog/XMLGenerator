"""Tests for scorecard generator module."""

from __future__ import annotations

import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import xml.etree.ElementTree as ET

from config.schema_config import ACORD_NAMESPACE
from modules.table_manager import load_csv
from modules.xml_generator import generate_all_xmls
from modules.xml_validator import validate_all, ValidationResult
from modules.xml_analyzer import analyze_xml
from modules.scorecard_generator import (
    generate_scorecard,
    generate_enhanced_scorecard,
    save_scorecard,
    sort_xml_files,
    sort_analyzed_files,
    SCORECARD_SCHEMA,
    ENHANCED_SCORECARD_SCHEMA,
)
from tests.conftest import write_test_csv, make_test_row


def _run_pipeline(spark, tmp_dir, num_valid=3, invalid_rows=None):
    """
    Build test data, generate XMLs, and validate them.

    Args:
        num_valid: number of valid rows to generate
        invalid_rows: optional list of pre-built invalid row dicts
    """
    rows = [make_test_row(f"POL-{i:03d}") for i in range(1, num_valid + 1)]
    if invalid_rows:
        rows.extend(invalid_rows)

    csv_path = os.path.join(tmp_dir, "sample.csv")
    write_test_csv(csv_path, rows=rows)
    uid = uuid.uuid4().hex[:8]
    df = load_csv(spark, csv_path, f"sct_{uid}")
    out_dir = os.path.join(tmp_dir, "xml_out")
    xml_results = generate_all_xmls(df, out_dir)
    validation_results = validate_all(xml_results)
    return xml_results, validation_results, rows


class TestGenerateScorecard:
    def test_scorecard_row_count(self, spark, tmp_dir):
        xml_results, validation_results, csv_rows = _run_pipeline(spark, tmp_dir, 3)
        sc = generate_scorecard(spark, validation_results, xml_results)
        assert sc.count() == 3

    def test_scorecard_schema(self, spark, tmp_dir):
        xml_results, validation_results, csv_rows = _run_pipeline(spark, tmp_dir, 2)
        sc = generate_scorecard(spark, validation_results, xml_results)
        assert sc.schema == SCORECARD_SCHEMA

    def test_scorecard_has_pass_and_fail(self, spark, tmp_dir):
        invalid = [make_test_row("", PolNumber="")]
        xml_results, validation_results, csv_rows = _run_pipeline(
            spark, tmp_dir, 2, invalid_rows=invalid
        )
        sc = generate_scorecard(spark, validation_results, xml_results)
        statuses = [r["Status"] for r in sc.collect()]
        assert "PASS" in statuses
        assert "FAIL" in statuses

    def test_scorecard_error_details_null_for_pass(self, spark, tmp_dir):
        xml_results, validation_results, csv_rows = _run_pipeline(spark, tmp_dir, 3)
        sc = generate_scorecard(spark, validation_results, xml_results)
        for row in sc.collect():
            if row["Status"] == "PASS":
                assert row["ErrorDetails"] is None

    def test_scorecard_error_details_populated_for_fail(self, spark, tmp_dir):
        invalid = [make_test_row("", PolNumber="")]
        xml_results, validation_results, csv_rows = _run_pipeline(
            spark, tmp_dir, 0, invalid_rows=invalid
        )
        sc = generate_scorecard(spark, validation_results, xml_results)
        for row in sc.collect():
            if row["Status"] == "FAIL":
                assert row["ErrorDetails"] is not None
                assert len(row["ErrorDetails"]) > 0


class TestSaveScorecard:
    def test_save_creates_csv(self, spark, tmp_dir):
        xml_results, validation_results, csv_rows = _run_pipeline(spark, tmp_dir, 2)
        sc = generate_scorecard(spark, validation_results, xml_results)
        path = os.path.join(tmp_dir, "scorecard.csv")
        result = save_scorecard(sc, path)
        assert os.path.exists(result)
        with open(result) as f:
            lines = f.readlines()
        assert len(lines) == 3  # header + 2 rows


class TestSortXmlFiles:
    def test_files_sorted_correctly(self, spark, tmp_dir):
        invalid = [make_test_row("", PolNumber="")]
        xml_results, validation_results, _ = _run_pipeline(
            spark, tmp_dir, 3, invalid_rows=invalid
        )
        success_dir = os.path.join(tmp_dir, "success")
        fail_dir = os.path.join(tmp_dir, "unsuccessful")
        sorted_files = sort_xml_files(validation_results, xml_results,
                                      success_dir, fail_dir)
        assert len(sorted_files["success"]) >= 1
        assert len(sorted_files["unsuccessful"]) >= 1
        for f in sorted_files["success"]:
            assert os.path.exists(f)
        for f in sorted_files["unsuccessful"]:
            assert os.path.exists(f)

    def test_all_files_accounted_for(self, spark, tmp_dir):
        invalid = [make_test_row("", PolNumber="")]
        xml_results, validation_results, _ = _run_pipeline(
            spark, tmp_dir, 3, invalid_rows=invalid
        )
        success_dir = os.path.join(tmp_dir, "success")
        fail_dir = os.path.join(tmp_dir, "unsuccessful")
        sorted_files = sort_xml_files(validation_results, xml_results,
                                      success_dir, fail_dir)
        total = len(sorted_files["success"]) + len(sorted_files["unsuccessful"])
        assert total == 4

    def test_success_dir_only_pass(self, spark, tmp_dir):
        xml_results, validation_results, _ = _run_pipeline(spark, tmp_dir, 3)
        success_dir = os.path.join(tmp_dir, "success")
        fail_dir = os.path.join(tmp_dir, "unsuccessful")
        sorted_files = sort_xml_files(validation_results, xml_results,
                                      success_dir, fail_dir)
        assert len(sorted_files["success"]) == 3
        assert len(sorted_files["unsuccessful"]) == 0


def _make_analysis_xml(pol_number, extra_tags=None, tmp_dir=None):
    """Write a minimal test XML and return its path."""
    ns = ACORD_NAMESPACE
    ET.register_namespace("acord", ns)
    txlife = ET.Element(f"{{{ns}}}TXLife")
    req = ET.SubElement(txlife, f"{{{ns}}}TXLifeRequest", PrimaryObjectID="H1")
    ET.SubElement(req, f"{{{ns}}}TransRefGUID").text = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    tt = ET.SubElement(req, f"{{{ns}}}TransType")
    tt.text = "Values Inquiry"
    tt.set("tc", "212")
    ET.SubElement(req, f"{{{ns}}}TransExeDate").text = "2026-03-19"
    ET.SubElement(req, f"{{{ns}}}TransExeTime").text = "10:30:00"
    olife = ET.SubElement(req, f"{{{ns}}}OLifE")
    holding = ET.SubElement(olife, f"{{{ns}}}Holding", id="H1")
    htc = ET.SubElement(holding, f"{{{ns}}}HoldingTypeCode")
    htc.text = "Policy"
    htc.set("tc", "2")
    policy = ET.SubElement(holding, f"{{{ns}}}Policy")
    ET.SubElement(policy, f"{{{ns}}}PolNumber").text = pol_number
    ET.SubElement(policy, f"{{{ns}}}CarrierCode").text = "12345"
    for tag in (extra_tags or []):
        ET.SubElement(policy, f"{{{ns}}}{tag}").text = "ext-value"
    xml_str = ET.tostring(txlife, encoding="unicode", xml_declaration=True)
    path = os.path.join(tmp_dir, f"ValuesInquiry_{pol_number}.xml")
    with open(path, "w") as f:
        f.write(xml_str)
    return path


class TestEnhancedScorecard:
    def test_enhanced_schema(self, spark, tmp_dir):
        path = _make_analysis_xml("ENH-001", tmp_dir=tmp_dir)
        with open(path) as f:
            xml_content = f.read()
        results = [analyze_xml(xml_content, filename=os.path.basename(path),
                               filepath=path)]
        sc = generate_enhanced_scorecard(spark, results)
        assert sc.schema == ENHANCED_SCORECARD_SCHEMA

    def test_enhanced_has_conformance(self, spark, tmp_dir):
        path = _make_analysis_xml("ENH-002", tmp_dir=tmp_dir)
        with open(path) as f:
            xml_content = f.read()
        results = [analyze_xml(xml_content, filename=os.path.basename(path),
                               filepath=path)]
        sc = generate_enhanced_scorecard(spark, results)
        row = sc.collect()[0]
        assert row["ConformancePct"] is not None
        assert row["ConformancePct"] > 0

    def test_enhanced_shows_custom_fields(self, spark, tmp_dir):
        path = _make_analysis_xml("ENH-003", extra_tags=["MyExtField"],
                                  tmp_dir=tmp_dir)
        with open(path) as f:
            xml_content = f.read()
        results = [analyze_xml(xml_content, filename=os.path.basename(path),
                               filepath=path)]
        sc = generate_enhanced_scorecard(spark, results)
        row = sc.collect()[0]
        assert "MyExtField" in row["CustomFields"]
        assert row["CustomCount"] == "1"

    def test_enhanced_save_to_csv(self, spark, tmp_dir):
        path = _make_analysis_xml("ENH-005", extra_tags=["Ext1", "Ext2"],
                                  tmp_dir=tmp_dir)
        with open(path) as f:
            xml_content = f.read()
        results = [analyze_xml(xml_content, filename=os.path.basename(path),
                               filepath=path)]
        sc = generate_enhanced_scorecard(spark, results)
        csv_path = os.path.join(tmp_dir, "enhanced_scorecard.csv")
        save_scorecard(sc, csv_path)
        with open(csv_path) as f:
            lines = f.readlines()
        assert len(lines) == 2
        header = lines[0].strip()
        assert "CustomFields" in header
        assert "ConformancePct" in header


class TestSortAnalyzedFiles:
    def test_sorts_by_analysis_result(self, spark, tmp_dir):
        xml_path = _make_analysis_xml("SORT-001", tmp_dir=tmp_dir)
        with open(xml_path) as f:
            xml_content = f.read()
        result = analyze_xml(xml_content,
                             filename=os.path.basename(xml_path),
                             filepath=xml_path)

        success_dir = os.path.join(tmp_dir, "s")
        fail_dir = os.path.join(tmp_dir, "f")
        sorted_files = sort_analyzed_files([result], success_dir, fail_dir)
        total = len(sorted_files["success"]) + len(sorted_files["unsuccessful"])
        assert total == 1
