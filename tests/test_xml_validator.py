"""Tests for XML validator module."""

import os
import sys
import uuid
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import ACORD_NAMESPACE, ACORD_NS_PREFIX
from modules.csv_generator import generate_sample_csv
from modules.table_manager import load_csv
from modules.xml_generator import generate_xml_from_row, generate_all_xmls
from modules.xml_validator import validate_xml, validate_all, ValidationResult


def _load_df(spark, tmp_dir, num_valid=3, num_invalid=0):
    csv_path = os.path.join(tmp_dir, "sample.csv")
    generate_sample_csv(csv_path, num_valid=num_valid, num_invalid=num_invalid)
    uid = uuid.uuid4().hex[:8]
    return load_csv(spark, csv_path, f"valt_{uid}")


def _make_valid_xml():
    """Build a minimal valid TXLifeRequest XML string."""
    ET.register_namespace(ACORD_NS_PREFIX, ACORD_NAMESPACE)
    ns = ACORD_NAMESPACE
    txlife = ET.Element(f"{{{ns}}}TXLife")
    req = ET.SubElement(txlife, f"{{{ns}}}TXLifeRequest", PrimaryObjectID="Holding_1")
    ET.SubElement(req, f"{{{ns}}}TransRefGUID").text = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    tt = ET.SubElement(req, f"{{{ns}}}TransType")
    tt.text = "Values Inquiry"
    tt.set("tc", "212")
    ET.SubElement(req, f"{{{ns}}}TransExeDate").text = "2026-03-19"
    ET.SubElement(req, f"{{{ns}}}TransExeTime").text = "10:30:00"
    olife = ET.SubElement(req, f"{{{ns}}}OLifE")
    holding = ET.SubElement(olife, f"{{{ns}}}Holding", id="Holding_1")
    htc = ET.SubElement(holding, f"{{{ns}}}HoldingTypeCode")
    htc.text = "Policy"
    htc.set("tc", "2")
    policy = ET.SubElement(holding, f"{{{ns}}}Policy")
    ET.SubElement(policy, f"{{{ns}}}PolNumber").text = "ANN-2026-00001"
    ET.SubElement(policy, f"{{{ns}}}CarrierCode").text = "12345"
    return ET.tostring(txlife, encoding="unicode", xml_declaration=True)


class TestValidateXml:
    def test_valid_xml_passes(self):
        xml_str = _make_valid_xml()
        result = validate_xml(xml_str)
        assert result.valid is True
        assert len(result.errors) == 0
        assert result.policy_number == "ANN-2026-00001"

    def test_malformed_xml_fails(self):
        result = validate_xml("<not valid xml><<<")
        assert result.valid is False
        assert any("parse error" in e.lower() for e in result.errors)

    def test_missing_required_polnumber(self):
        xml_str = _make_valid_xml()
        root = ET.fromstring(xml_str)
        ns = ACORD_NAMESPACE
        pol = root.find(f".//{{{ns}}}PolNumber")
        pol.getparent() if hasattr(pol, "getparent") else None
        # Remove PolNumber by rebuilding without it
        for policy in root.iter(f"{{{ns}}}Policy"):
            for child in list(policy):
                if child.tag == f"{{{ns}}}PolNumber":
                    policy.remove(child)
        modified = ET.tostring(root, encoding="unicode", xml_declaration=True)
        result = validate_xml(modified)
        assert result.valid is False
        assert any("PolNumber" in e for e in result.errors)

    def test_missing_required_carrier_code(self):
        xml_str = _make_valid_xml()
        root = ET.fromstring(xml_str)
        ns = ACORD_NAMESPACE
        for policy in root.iter(f"{{{ns}}}Policy"):
            for child in list(policy):
                if child.tag == f"{{{ns}}}CarrierCode":
                    policy.remove(child)
        modified = ET.tostring(root, encoding="unicode", xml_declaration=True)
        result = validate_xml(modified)
        assert result.valid is False
        assert any("CarrierCode" in e for e in result.errors)

    def test_wrong_root_element(self):
        xml_str = "<WrongRoot><child/></WrongRoot>"
        result = validate_xml(xml_str)
        assert result.valid is False
        assert any("Root element" in e for e in result.errors)

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
        df = _load_df(spark, tmp_dir, 3, 0)
        out_dir = os.path.join(tmp_dir, "xml_out")
        xml_results = generate_all_xmls(df, out_dir)
        validation_results = validate_all(xml_results)
        assert len(validation_results) == 3
        assert all(r.valid for r in validation_results)

    def test_mixed_batch_catches_invalid(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 7, 2)
        out_dir = os.path.join(tmp_dir, "xml_out")
        xml_results = generate_all_xmls(df, out_dir)
        validation_results = validate_all(xml_results)
        assert len(validation_results) == 9
        failed = [r for r in validation_results if not r.valid]
        assert len(failed) >= 1  # at least the missing PolNumber row fails

    def test_invalid_date_row_detected(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 0, 2)
        out_dir = os.path.join(tmp_dir, "xml_out")
        xml_results = generate_all_xmls(df, out_dir)
        validation_results = validate_all(xml_results)
        pol_missing = [r for r in validation_results if r.policy_number == "UNKNOWN" or r.policy_number == ""]
        assert len(pol_missing) >= 1
