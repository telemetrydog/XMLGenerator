"""
Analyzes external DTCC 21208 Withdrawal-Quote XMLs against our schema.

Walks the incoming XML tree, compares every element/attribute to our
FIELD_DEFINITIONS, and classifies each as matched, missing, or custom.

Handles both SOAP-wrapped and bare TXLife root elements.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

from config.schema_config import (
    ACORD_NAMESPACE,
    SOAP_NAMESPACE,
    OPERATION_NAMESPACE,
    FIELD_DEFINITIONS,
    GROUP_META,
)
from modules.xml_validator import validate_xml, ValidationResult


_NS_MAP = {
    ACORD_NAMESPACE: "ns3",
    SOAP_NAMESPACE: "soap",
    OPERATION_NAMESPACE: "ns2",
}


def _strip_ns(tag: str) -> str:
    """Remove the namespace brace prefix from an element tag."""
    if "{" in tag:
        return tag.split("}", 1)[1]
    return tag


def _build_known_tags() -> set[str]:
    """Return set of element local-names that our schema knows about."""
    tags = set()
    for fd in FIELD_DEFINITIONS:
        if fd["group"] == GROUP_META:
            continue
        t = fd["xml_tag"]
        if t and "_text" not in t and "_ext" not in t and "_AppliesTo" not in t:
            tags.add(t)
    # Structural elements
    tags.update({
        "Envelope", "Body", "processValueInquiry21208",
        "VI21208_Msg", "TXLife", "TXLifeRequest", "OLifE",
        "Holding", "Policy", "Arrangement", "Annuity",
        "TaxWithholding", "OLifEExtension",
        "Party", "Person", "Organization", "Producer",
        "Carrier", "PartialIdentification", "Relation",
    })
    return tags


@dataclass
class AnalysisResult:
    """Full analysis of a single external XML against our schema."""
    policy_number: str
    filename: str
    filepath: str
    validation: ValidationResult
    matched_fields: list[str] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)
    custom_fields: list[str]  = field(default_factory=list)
    conformance_pct: float = 0.0

    @property
    def status(self) -> str:
        return "PASS" if self.validation.valid else "FAIL"

    def to_dict(self) -> dict:
        return {
            "policy_number": self.policy_number,
            "filename": self.filename,
            "status": self.status,
            "errors": self.validation.errors,
            "matched_fields": self.matched_fields,
            "missing_fields": self.missing_fields,
            "custom_fields": self.custom_fields,
            "conformance_pct": self.conformance_pct,
        }


def _collect_element_tags(root: ET.Element) -> set[str]:
    """Recursively collect all unique element local-names."""
    return {_strip_ns(el.tag) for el in root.iter()}


def analyze_xml(xml_string: str, filename: str = "",
                filepath: str = "") -> AnalysisResult:
    """Analyze a single XML string against our schema."""
    validation = validate_xml(xml_string)
    known_tags = _build_known_tags()

    schema_field_tags = set()
    for fd in FIELD_DEFINITIONS:
        if fd["group"] == GROUP_META:
            continue
        t = fd["xml_tag"]
        if t and "_text" not in t and "_ext" not in t and "_AppliesTo" not in t:
            schema_field_tags.add(t)

    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError:
        return AnalysisResult(
            policy_number=validation.policy_number,
            filename=filename, filepath=filepath,
            validation=validation, conformance_pct=0.0,
        )

    xml_tags = _collect_element_tags(root)

    matched = sorted(schema_field_tags & xml_tags)
    missing = sorted(schema_field_tags - xml_tags)

    structural = {
        "Envelope", "Body", "processValueInquiry21208",
        "VI21208_Msg", "TXLife", "TXLifeRequest", "OLifE",
        "Holding", "Policy", "Arrangement", "Annuity",
        "TaxWithholding", "OLifEExtension",
        "Party", "Person", "Organization", "Producer",
        "Carrier", "PartialIdentification", "Relation",
    }
    custom = sorted(xml_tags - known_tags - structural)

    total = len(schema_field_tags)
    conformance = (len(matched) / total * 100) if total else 0.0

    return AnalysisResult(
        policy_number=validation.policy_number,
        filename=filename, filepath=filepath,
        validation=validation,
        matched_fields=matched,
        missing_fields=missing,
        custom_fields=custom,
        conformance_pct=round(conformance, 1),
    )


def analyze_xml_file(filepath: str) -> AnalysisResult:
    """Analyze a single XML file from disk."""
    with open(filepath, "r", encoding="utf-8") as f:
        xml_string = f.read()
    filename = os.path.basename(filepath)
    return analyze_xml(xml_string, filename=filename, filepath=filepath)


def analyze_xml_directory(directory: str) -> list[AnalysisResult]:
    """Analyze all .xml files in a directory."""
    results = []
    for name in sorted(os.listdir(directory)):
        if not name.lower().endswith(".xml"):
            continue
        path = os.path.join(directory, name)
        if os.path.isfile(path):
            results.append(analyze_xml_file(path))
    return results
