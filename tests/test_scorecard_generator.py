"""Tests for scorecard generator module."""

import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import xml.etree.ElementTree as ET

from config.schema_config import ACORD_NAMESPACE, ACORD_NS_PREFIX
from modules.csv_generator import generate_sample_csv
from modules.table_manager import load_csv
from modules.xml_generator import generate_all_xmls
from modules.xml_validator import validate_all, ValidationResult
from modules.xml_analyzer import analyze_xml, analyze_xml_directory
from modules.scorecard_generator import (
    generate_scorecard,
    generate_enhanced_scorecard,
    save_scorecard,
    sort_xml_files,
    sort_analyzed_files,
    SCORECARD_SCHEMA,
    ENHANCED_SCORECARD_SCHEMA,
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


def _make_analysis_xml(pol_number, extra_tags=None, tmp_dir=None):
    """Write a test XML and return its path."""
    ns = ACORD_NAMESPACE
    ET.register_namespace(ACORD_NS_PREFIX, ns)
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
        results = [analyze_xml(open(path).read(), filename=os.path.basename(path), filepath=path)]
        sc = generate_enhanced_scorecard(spark, results)
        assert sc.schema == ENHANCED_SCORECARD_SCHEMA

    def test_enhanced_has_conformance(self, spark, tmp_dir):
        path = _make_analysis_xml("ENH-002", tmp_dir=tmp_dir)
        results = [analyze_xml(open(path).read(), filename=os.path.basename(path), filepath=path)]
        sc = generate_enhanced_scorecard(spark, results)
        row = sc.collect()[0]
        assert row["ConformancePct"] is not None
        assert row["ConformancePct"] > 0

    def test_enhanced_shows_custom_fields(self, spark, tmp_dir):
        path = _make_analysis_xml("ENH-003", extra_tags=["MyExtField"], tmp_dir=tmp_dir)
        results = [analyze_xml(open(path).read(), filename=os.path.basename(path), filepath=path)]
        sc = generate_enhanced_scorecard(spark, results)
        row = sc.collect()[0]
        assert "MyExtField" in row["CustomFields"]
        assert row["CustomCount"] == "1"

    def test_enhanced_shows_missing_fields(self, spark, tmp_dir):
        path = _make_analysis_xml("ENH-004", tmp_dir=tmp_dir)
        results = [analyze_xml(open(path).read(), filename=os.path.basename(path), filepath=path)]
        sc = generate_enhanced_scorecard(spark, results)
        row = sc.collect()[0]
        assert row["MissingFields"] is not None
        assert "ProductCode" in row["MissingFields"]

    def test_enhanced_save_to_csv(self, spark, tmp_dir):
        path = _make_analysis_xml("ENH-005", extra_tags=["Ext1", "Ext2"], tmp_dir=tmp_dir)
        results = [analyze_xml(open(path).read(), filename=os.path.basename(path), filepath=path)]
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
        good = _make_analysis_xml("GOOD-001", tmp_dir=tmp_dir)
        good_r = analyze_xml(open(good).read(), filename=os.path.basename(good), filepath=good)
        assert good_r.status == "PASS"

        success_dir = os.path.join(tmp_dir, "s")
        fail_dir = os.path.join(tmp_dir, "f")
        sorted_files = sort_analyzed_files([good_r], success_dir, fail_dir)
        assert len(sorted_files["success"]) == 1
        assert len(sorted_files["unsuccessful"]) == 0
