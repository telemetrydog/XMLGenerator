"""Tests for XML validator module (SOAP-wrapped 21208 WD-Quote)."""

from __future__ import annotations

import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from modules.table_manager import load_csv
from modules.xml_generator import generate_xml_from_row, generate_all_xmls
from modules.xml_validator import validate_xml, validate_all, ValidationResult
from tests.conftest import write_test_csv, make_test_row


def _load_df(spark, tmp_dir, num_valid=1, rows=None):
    csv_path = os.path.join(tmp_dir, "sample.csv")
    if rows is not None:
        write_test_csv(csv_path, rows=rows)
    else:
        write_test_csv(csv_path, num_valid=num_valid)
    uid = uuid.uuid4().hex[:8]
    return load_csv(spark, csv_path, f"valt_{uid}")


class TestValidateXml:
    def test_valid_xml_passes(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        result = validate_xml(xml_str)
        assert result.valid is True, f"Errors: {result.errors}"
        assert len(result.errors) == 0
        assert result.policy_number == "ANN-2026-00001"

    def test_malformed_xml_fails(self):
        result = validate_xml("<not valid xml><<<")
        assert result.valid is False
        assert any("parse error" in e.lower() for e in result.errors)

    def test_missing_pol_number_detected(self, spark, tmp_dir):
        row = make_test_row("", PolNumber="")
        df = _load_df(spark, tmp_dir, rows=[row])
        spark_row = df.collect()[0]
        xml_str = generate_xml_from_row(spark_row)
        result = validate_xml(xml_str)
        assert result.valid is False
        assert any("PolNumber" in e for e in result.errors)

    def test_wrong_root_element(self):
        xml_str = "<WrongRoot><child/></WrongRoot>"
        result = validate_xml(xml_str)
        assert result.valid is False

    def test_to_dict(self):
        result = ValidationResult(
            policy_number="TEST-001",
            valid=False,
            errors=["error1", "error2"],
        )
        d = result.to_dict()
        assert d["policy_number"] == "TEST-001"
        assert d["valid"] is False
        assert len(d["errors"]) == 2


class TestValidateAll:
    def test_valid_batch_all_pass(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 3)
        out_dir = os.path.join(tmp_dir, "xml_out")
        xml_results = generate_all_xmls(df, out_dir)
        validation_results = validate_all(xml_results)
        assert len(validation_results) == 3
        for vr in validation_results:
            assert vr.valid is True, f"{vr.policy_number}: {vr.errors}"

    def test_mixed_batch_catches_invalid(self, spark, tmp_dir):
        valid_rows = [make_test_row(f"POL-{i:03d}") for i in range(3)]
        invalid_row = make_test_row("", PolNumber="")
        all_rows = valid_rows + [invalid_row]
        df = _load_df(spark, tmp_dir, rows=all_rows)
        out_dir = os.path.join(tmp_dir, "xml_out")
        xml_results = generate_all_xmls(df, out_dir)
        validation_results = validate_all(xml_results)
        assert len(validation_results) == 4
        failed = [r for r in validation_results if not r.valid]
        assert len(failed) >= 1

    def test_policy_number_in_results(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 2)
        out_dir = os.path.join(tmp_dir, "xml_out")
        xml_results = generate_all_xmls(df, out_dir)
        validation_results = validate_all(xml_results)
        pol_numbers = {r.policy_number for r in validation_results}
        assert "ANN-2026-00001" in pol_numbers
        assert "ANN-2026-00002" in pol_numbers
