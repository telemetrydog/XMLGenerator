"""
Validates ACORD TXLifeRequest XML documents against the data dictionary
defined in schema_config (required fields, types, lengths, allowed values,
date/time formats, XML well-formedness).
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

from config.schema_config import ACORD_NAMESPACE, FIELD_DEFINITIONS


NS = {"acord": ACORD_NAMESPACE}

_XPATH_MAP = {
    "TransRefGUID": ".//acord:TXLifeRequest/acord:TransRefGUID",
    "TransType": ".//acord:TXLifeRequest/acord:TransType",
    "TransSubType": ".//acord:TXLifeRequest/acord:TransSubType",
    "TransExeDate": ".//acord:TXLifeRequest/acord:TransExeDate",
    "TransExeTime": ".//acord:TXLifeRequest/acord:TransExeTime",
    "TransEffDate": ".//acord:TXLifeRequest/acord:TransEffDate",
    "InquiryLevel": ".//acord:TXLifeRequest/acord:InquiryLevel",
    "InquiryView": ".//acord:TXLifeRequest/acord:InquiryView",
    "NoResponseOK": ".//acord:TXLifeRequest/acord:NoResponseOK",
    "TestIndicator": ".//acord:TXLifeRequest/acord:TestIndicator",
    "HoldingID": ".//acord:Holding",
    "HoldingTypeCode": ".//acord:Holding/acord:HoldingTypeCode",
    "PolNumber": ".//acord:Policy/acord:PolNumber",
    "ProductCode": ".//acord:Policy/acord:ProductCode",
    "CarrierCode": ".//acord:Policy/acord:CarrierCode",
    "LineOfBusiness": ".//acord:Policy/acord:LineOfBusiness",
    "ProductType": ".//acord:Policy/acord:ProductType",
    "PolicyStatus": ".//acord:Policy/acord:PolicyStatus",
}


@dataclass
class ValidationResult:
    policy_number: str
    valid: bool
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "policy_number": self.policy_number,
            "valid": self.valid,
            "errors": self.errors,
        }


def validate_xml(xml_string: str) -> ValidationResult:
    """
    Validate a single XML string against the ACORD Values Inquiry data dictionary.

    Returns a ValidationResult with any errors found.
    """
    errors: list[str] = []
    policy_number = ""

    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        return ValidationResult(
            policy_number="UNKNOWN",
            valid=False,
            errors=[f"XML parse error: {e}"],
        )

    if root.tag != f"{{{ACORD_NAMESPACE}}}TXLife":
        errors.append(f"Root element must be TXLife, got {root.tag}")

    for fd in FIELD_DEFINITIONS:
        col = fd["column_name"]
        xpath = _XPATH_MAP.get(col)
        if xpath is None:
            continue

        element = root.find(xpath, NS)

        if col == "HoldingID":
            value = element.get("id") if element is not None else None
        elif element is not None:
            tc = element.get("tc")
            value = tc if tc else (element.text or "").strip()
            if not value:
                value = (element.text or "").strip()
        else:
            value = None

        _validate_field(fd, col, value, errors)

        if col == "PolNumber" and value:
            policy_number = value

    return ValidationResult(
        policy_number=policy_number or "UNKNOWN",
        valid=len(errors) == 0,
        errors=errors,
    )


def _validate_field(fd: dict, col: str, value: str | None, errors: list[str]) -> None:
    """Run all validation checks for a single field."""
    if fd["required"] and not value:
        errors.append(f"Required field '{col}' is missing or empty")
        return

    if not value:
        return

    max_len = fd.get("max_length")
    if max_len and len(value) > max_len:
        errors.append(
            f"Field '{col}' exceeds max length {max_len} (got {len(value)})"
        )

    allowed = fd.get("allowed_values")
    if allowed and value not in allowed:
        errors.append(
            f"Field '{col}' has invalid value '{value}'; allowed: {allowed}"
        )

    regex = fd.get("regex")
    if regex and not re.match(regex, value):
        errors.append(
            f"Field '{col}' value '{value}' does not match pattern {regex}"
        )


def validate_all(xml_results: list[dict]) -> list[ValidationResult]:
    """
    Validate a batch of XML results (as returned by generate_all_xmls).

    Returns a list of ValidationResult objects.
    """
    return [validate_xml(r["xml_string"]) for r in xml_results]
