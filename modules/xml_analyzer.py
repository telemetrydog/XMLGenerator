"""
Analyzes external DTCC Values Inquiry XMLs against our schema implementation.

Walks the incoming XML tree, compares every element/attribute to our
FIELD_DEFINITIONS, and classifies each as matched (standard), missing
(in our schema but absent from the XML), or custom (in the XML but not
in our schema -- likely a carrier-specific or DTCC extension).
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

from config.schema_config import ACORD_NAMESPACE, FIELD_DEFINITIONS
from modules.xml_validator import validate_xml, ValidationResult


NS = {"acord": ACORD_NAMESPACE}
_NS_BRACE = f"{{{ACORD_NAMESPACE}}}"


def _strip_ns(tag: str) -> str:
    """Remove the namespace brace prefix from an element tag."""
    if tag.startswith(_NS_BRACE):
        return tag[len(_NS_BRACE):]
    return tag


def _build_known_tags() -> set[str]:
    """Return set of element local-names that our schema knows about."""
    tags = set()
    for fd in FIELD_DEFINITIONS:
        tags.add(fd["xml_tag"])
    tags.update({"TXLife", "TXLifeRequest", "OLifE", "Holding", "Policy"})
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
    custom_fields: list[str] = field(default_factory=list)

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
    """Recursively collect all unique element local-names in the tree."""
    tags = set()
    for el in root.iter():
        tags.add(_strip_ns(el.tag))
    return tags


def _collect_element_paths(root: ET.Element, prefix: str = "") -> list[str]:
    """
    Recursively collect full dot-separated paths for every element in the tree.
    E.g. "TXLife.TXLifeRequest.OLifE.Holding.Policy.PolNumber"
    """
    paths = []
    local = _strip_ns(root.tag)
    current = f"{prefix}.{local}" if prefix else local
    paths.append(current)
    for child in root:
        paths.extend(_collect_element_paths(child, current))
    return paths


def analyze_xml(xml_string: str, filename: str = "", filepath: str = "") -> AnalysisResult:
    """
    Analyze a single XML string against our schema.

    Returns an AnalysisResult with validation results plus schema-difference
    breakdown (matched, missing, custom fields).
    """
    validation = validate_xml(xml_string)
    known_tags = _build_known_tags()

    schema_field_tags = {fd["xml_tag"] for fd in FIELD_DEFINITIONS}

    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError:
        return AnalysisResult(
            policy_number=validation.policy_number,
            filename=filename,
            filepath=filepath,
            validation=validation,
            conformance_pct=0.0,
        )

    xml_tags = _collect_element_tags(root)
    xml_paths = _collect_element_paths(root)

    matched = sorted(schema_field_tags & xml_tags)
    missing = sorted(schema_field_tags - xml_tags)

    structural_tags = {"TXLife", "TXLifeRequest", "OLifE", "Holding", "Policy"}
    custom = sorted(xml_tags - known_tags - structural_tags)

    total_schema_fields = len(schema_field_tags)
    conformance = (len(matched) / total_schema_fields * 100) if total_schema_fields else 0.0

    return AnalysisResult(
        policy_number=validation.policy_number,
        filename=filename,
        filepath=filepath,
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
