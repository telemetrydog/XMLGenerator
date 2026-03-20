"""Tests for XML analyzer module."""

import os
import sys
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import ACORD_NAMESPACE, ACORD_NS_PREFIX
from modules.xml_analyzer import (
    analyze_xml,
    analyze_xml_file,
    analyze_xml_directory,
    AnalysisResult,
)


NS = ACORD_NAMESPACE


def _build_xml(
    extra_elements: list[tuple[str, str]] | None = None,
    omit_tags: set[str] | None = None,
    pol_number: str = "ANN-TEST-001",
) -> str:
    """Build a TXLifeRequest XML string with optional extras / omissions."""
    ET.register_namespace(ACORD_NS_PREFIX, NS)
    txlife = ET.Element(f"{{{NS}}}TXLife")
    req = ET.SubElement(txlife, f"{{{NS}}}TXLifeRequest", PrimaryObjectID="Holding_1")

    omit = omit_tags or set()

    fields = [
        ("TransRefGUID", "a1b2c3d4-e5f6-7890-abcd-ef1234567890", {}),
        ("TransType", "Values Inquiry", {"tc": "212"}),
        ("TransSubType", "21207", {}),
        ("TransExeDate", "2026-03-19", {}),
        ("TransExeTime", "10:30:00", {}),
    ]
    for tag, text, attribs in fields:
        if tag not in omit:
            el = ET.SubElement(req, f"{{{NS}}}{tag}")
            el.text = text
            for k, v in attribs.items():
                el.set(k, v)

    olife = ET.SubElement(req, f"{{{NS}}}OLifE")
    holding = ET.SubElement(olife, f"{{{NS}}}Holding", id="Holding_1")

    if "HoldingTypeCode" not in omit:
        htc = ET.SubElement(holding, f"{{{NS}}}HoldingTypeCode")
        htc.text = "Policy"
        htc.set("tc", "2")

    policy = ET.SubElement(holding, f"{{{NS}}}Policy")
    policy_fields = [
        ("PolNumber", pol_number, {}),
        ("CarrierCode", "12345", {}),
    ]
    for tag, text, attribs in policy_fields:
        if tag not in omit:
            el = ET.SubElement(policy, f"{{{NS}}}{tag}")
            el.text = text

    if extra_elements:
        for parent_tag, child_tag in extra_elements:
            if parent_tag == "Policy":
                parent = policy
            elif parent_tag == "TXLifeRequest":
                parent = req
            elif parent_tag == "Holding":
                parent = holding
            elif parent_tag == "OLifE":
                parent = olife
            else:
                parent = req
            el = ET.SubElement(parent, f"{{{NS}}}{child_tag}")
            el.text = "custom-value"

    return ET.tostring(txlife, encoding="unicode", xml_declaration=True)


class TestAnalyzeXml:
    def test_standard_xml_all_matched(self):
        xml_str = _build_xml()
        result = analyze_xml(xml_str, filename="test.xml")
        assert result.policy_number == "ANN-TEST-001"
        assert "TransRefGUID" in result.matched_fields
        assert "TransType" in result.matched_fields
        assert "PolNumber" in result.matched_fields
        assert "CarrierCode" in result.matched_fields
        assert len(result.custom_fields) == 0

    def test_identifies_missing_optional_fields(self):
        xml_str = _build_xml()
        result = analyze_xml(xml_str)
        assert "ProductCode" in result.missing_fields
        assert "LineOfBusiness" in result.missing_fields
        assert "ProductType" in result.missing_fields
        assert "PolicyStatus" in result.missing_fields

    def test_identifies_custom_fields(self):
        xml_str = _build_xml(extra_elements=[
            ("Policy", "CustomRiderInfo"),
            ("TXLifeRequest", "VendorSpecificData"),
        ])
        result = analyze_xml(xml_str)
        assert "CustomRiderInfo" in result.custom_fields
        assert "VendorSpecificData" in result.custom_fields

    def test_custom_fields_not_in_matched(self):
        xml_str = _build_xml(extra_elements=[("Policy", "MyExtension")])
        result = analyze_xml(xml_str)
        assert "MyExtension" not in result.matched_fields
        assert "MyExtension" in result.custom_fields

    def test_conformance_100_when_all_present(self):
        xml_str = _build_xml(extra_elements=[
            ("Policy", "ProductCode"),
            ("Policy", "LineOfBusiness"),
            ("Policy", "ProductType"),
            ("Policy", "PolicyStatus"),
            ("TXLifeRequest", "TransEffDate"),
            ("TXLifeRequest", "InquiryLevel"),
            ("TXLifeRequest", "InquiryView"),
            ("TXLifeRequest", "NoResponseOK"),
            ("TXLifeRequest", "TestIndicator"),
        ])
        result = analyze_xml(xml_str)
        assert result.conformance_pct == 100.0
        assert len(result.missing_fields) == 0

    def test_conformance_partial(self):
        xml_str = _build_xml()
        result = analyze_xml(xml_str)
        assert 0 < result.conformance_pct < 100

    def test_missing_required_field_fails_validation(self):
        xml_str = _build_xml(omit_tags={"PolNumber"})
        result = analyze_xml(xml_str)
        assert result.status == "FAIL"
        assert any("PolNumber" in e for e in result.validation.errors)
        assert "PolNumber" in result.missing_fields

    def test_malformed_xml(self):
        result = analyze_xml("<broken>>>", filename="bad.xml")
        assert result.status == "FAIL"
        assert result.conformance_pct == 0.0
        assert result.policy_number == "UNKNOWN"

    def test_to_dict(self):
        xml_str = _build_xml(extra_elements=[("Policy", "FooBar")])
        result = analyze_xml(xml_str)
        d = result.to_dict()
        assert "matched_fields" in d
        assert "missing_fields" in d
        assert "custom_fields" in d
        assert "conformance_pct" in d
        assert "FooBar" in d["custom_fields"]

    def test_structural_elements_not_classified_as_custom(self):
        xml_str = _build_xml()
        result = analyze_xml(xml_str)
        for tag in ["TXLife", "TXLifeRequest", "OLifE", "Holding", "Policy"]:
            assert tag not in result.custom_fields

    def test_many_custom_fields(self):
        extras = [
            ("Policy", "Annuity"),
            ("Policy", "DeathBenefitAmt"),
            ("Policy", "SurrenderCharge"),
            ("Holding", "Investment"),
            ("TXLifeRequest", "CriteriaExpression"),
        ]
        xml_str = _build_xml(extra_elements=extras)
        result = analyze_xml(xml_str)
        assert len(result.custom_fields) == 5
        assert "Annuity" in result.custom_fields
        assert "DeathBenefitAmt" in result.custom_fields


class TestAnalyzeXmlFile:
    def test_analyze_single_file(self, tmp_dir):
        xml_str = _build_xml()
        path = os.path.join(tmp_dir, "test_policy.xml")
        with open(path, "w") as f:
            f.write(xml_str)
        result = analyze_xml_file(path)
        assert result.filename == "test_policy.xml"
        assert result.filepath == path
        assert result.policy_number == "ANN-TEST-001"


class TestAnalyzeXmlDirectory:
    def test_analyze_multiple_files(self, tmp_dir):
        for i in range(3):
            xml_str = _build_xml(pol_number=f"POL-{i}")
            path = os.path.join(tmp_dir, f"policy_{i}.xml")
            with open(path, "w") as f:
                f.write(xml_str)
        results = analyze_xml_directory(tmp_dir)
        assert len(results) == 3
        pols = {r.policy_number for r in results}
        assert pols == {"POL-0", "POL-1", "POL-2"}

    def test_ignores_non_xml_files(self, tmp_dir):
        xml_str = _build_xml()
        with open(os.path.join(tmp_dir, "good.xml"), "w") as f:
            f.write(xml_str)
        with open(os.path.join(tmp_dir, "notes.txt"), "w") as f:
            f.write("not xml")
        results = analyze_xml_directory(tmp_dir)
        assert len(results) == 1

    def test_mixed_valid_and_custom(self, tmp_dir):
        standard = _build_xml(pol_number="STANDARD-001")
        custom = _build_xml(
            pol_number="CUSTOM-001",
            extra_elements=[("Policy", "ExtensionField"), ("Policy", "CarrierRider")],
        )
        with open(os.path.join(tmp_dir, "standard.xml"), "w") as f:
            f.write(standard)
        with open(os.path.join(tmp_dir, "custom.xml"), "w") as f:
            f.write(custom)
        results = analyze_xml_directory(tmp_dir)
        standard_r = [r for r in results if r.policy_number == "STANDARD-001"][0]
        custom_r = [r for r in results if r.policy_number == "CUSTOM-001"][0]
        assert len(standard_r.custom_fields) == 0
        assert len(custom_r.custom_fields) == 2
        assert "ExtensionField" in custom_r.custom_fields
        assert "CarrierRider" in custom_r.custom_fields
