"""
Validates ACORD 212/21208 Withdrawal-Quote XML documents against the
DTCC IFW Data Dictionary rules.

Handles SOAP-wrapped and bare TXLife root elements.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

from config.schema_config import (
    ACORD_NAMESPACE,
    SOAP_NAMESPACE,
    OPERATION_NAMESPACE,
    FIELD_DEFINITIONS,
    GROUP_META,
    GROUP_TAX_FED,
    GROUP_TAX_STATE,
)

NS = {
    "soap": SOAP_NAMESPACE,
    "ns2":  OPERATION_NAMESPACE,
    "ns3":  ACORD_NAMESPACE,
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


# ---------------------------------------------------------------------------
# XPath helpers
# ---------------------------------------------------------------------------

def _txr(sub: str) -> str:
    return (
        ".//ns3:VI21208_Msg/ns3:TXLife/ns3:TXLifeRequest"
        + (f"/{sub}" if sub else "")
    )

_OL   = _txr("ns3:OLifE")
_HOLD = f"{_OL}/ns3:Holding"
_POL  = f"{_HOLD}/ns3:Policy"
_ARR  = f"{_HOLD}/ns3:Arrangement"
_EXT  = f"{_ARR}/ns3:OLifEExtension"

# Static XPath map (excludes TaxWithholding which needs dynamic lookup)
_XPATH: dict[str, tuple[str, str]] = {
    # SOAP
    "SoapBodyPresent":             (".//soap:Body", "exists"),
    "OperationName":               (".//ns2:processValueInquiry21208", "exists"),
    "MessageType":                 (".//ns3:VI21208_Msg", "exists"),
    # TXLifeRequest
    "TXLifeRequest_PrimaryObjectID": (_txr(""), "attr:PrimaryObjectID"),
    "TransRefGUID":      (_txr("ns3:TransRefGUID"),      "text"),
    "TransType_tc":      (_txr("ns3:TransType"),         "tc"),
    "TransType_text":    (_txr("ns3:TransType"),         "text"),
    "TransSubType_tc":   (_txr("ns3:TransSubType"),      "tc"),
    "TransSubType_text": (_txr("ns3:TransSubType"),      "text"),
    "TransExeDate":      (_txr("ns3:TransExeDate"),      "text"),
    "TransExeTime":      (_txr("ns3:TransExeTime"),      "text"),
    "PendingResponseOK_tc":   (_txr("ns3:PendingResponseOK"), "tc"),
    "PendingResponseOK_text": (_txr("ns3:PendingResponseOK"), "text"),
    # Holding
    "Holding_id":             (_HOLD, "attr:id"),
    "HoldingTypeCode_tc":     (f"{_HOLD}/ns3:HoldingTypeCode", "tc"),
    "HoldingTypeCode_text":   (f"{_HOLD}/ns3:HoldingTypeCode", "text"),
    "DistributorClientAcctNum": (f"{_HOLD}/ns3:DistributorClientAcctNum", "text"),
    # Policy
    "Policy_CarrierPartyID":  (_POL, "attr:CarrierPartyID"),
    "PolNumber":              (f"{_POL}/ns3:PolNumber", "text"),
    "Policy_CarrierCode":     (f"{_POL}/ns3:CarrierCode", "text"),
    "CusipNum":               (f"{_POL}/ns3:CusipNum", "text"),
    "Annuity_present":        (f"{_POL}/ns3:Annuity", "exists"),
    # Arrangement
    "ArrMode_tc":    (f"{_ARR}/ns3:ArrMode", "tc"),
    "ArrMode_text":  (f"{_ARR}/ns3:ArrMode", "text"),
    "ArrType_tc":    (f"{_ARR}/ns3:ArrType", "tc"),
    "ArrType_text":  (f"{_ARR}/ns3:ArrType", "text"),
    "ArrSubType_tc":    (f"{_ARR}/ns3:ArrSubType", "tc"),
    "ArrSubType_text":  (f"{_ARR}/ns3:ArrSubType", "text"),
    "ModalAmt":         (f"{_ARR}/ModalAmt", "text"),
    "SourceTransferAmtType_tc":   (f"{_ARR}/ns3:SourceTransferAmtType", "tc"),
    "SourceTransferAmtType_text": (f"{_ARR}/ns3:SourceTransferAmtType", "text"),
    # OLifEExtension
    "OLifEExtension_VendorCode":    (_EXT, "attr:VendorCode"),
    "OLifEExtension_ExtensionCode": (_EXT, "attr:ExtensionCode"),
    "AmountQualifier_tc":   (f"{_EXT}/ns3:AmountQualifier", "tc"),
    "AmountQualifier_text": (f"{_EXT}/ns3:AmountQualifier", "text"),
}


def _extract(root: ET.Element, xpath: str, mode: str) -> str | None:
    """Extract a value from the XML tree."""
    if mode == "exists":
        return "1" if root.find(xpath, NS) is not None else None
    if mode.startswith("attr:"):
        attr_name = mode.split(":", 1)[1]
        el = root.find(xpath, NS)
        return el.get(attr_name) if el is not None else None
    el = root.find(xpath, NS)
    if el is None:
        return None
    if mode == "tc":
        return el.get("tc")
    return (el.text or "").strip() or None


def _find_tax_withholding(root: ET.Element, place_tc: str) -> ET.Element | None:
    """Find a TaxWithholding element by its TaxWithholdingPlace tc value."""
    arr = root.find(_ARR, NS)
    if arr is None:
        return None
    ns3 = f"{{{ACORD_NAMESPACE}}}"
    for tw in arr.findall(f"{ns3}TaxWithholding"):
        place = tw.find(f"{ns3}TaxWithholdingPlace")
        if place is not None and place.get("tc") == place_tc:
            return tw
    return None


def _extract_tax_field(root: ET.Element, place_tc: str,
                       field_type: str) -> str | None:
    """Extract a specific field from a TaxWithholding element."""
    tw = _find_tax_withholding(root, place_tc)
    if tw is None:
        return None
    ns3 = f"{{{ACORD_NAMESPACE}}}"
    if field_type == "id":
        return tw.get("id")
    if field_type == "AppliesToPartyID":
        return tw.get("AppliesToPartyID")
    if field_type == "Place_tc":
        el = tw.find(f"{ns3}TaxWithholdingPlace")
        return el.get("tc") if el is not None else None
    if field_type == "Place_text":
        el = tw.find(f"{ns3}TaxWithholdingPlace")
        return (el.text or "").strip() if el is not None else None
    if field_type == "Type_tc":
        el = tw.find(f"{ns3}TaxWithholdingType")
        return el.get("tc") if el is not None else None
    if field_type == "Type_text":
        el = tw.find(f"{ns3}TaxWithholdingType")
        return (el.text or "").strip() if el is not None else None
    return None


# Tax field mapping: column_name -> (place_tc, field_type)
_TAX_MAP: dict[str, tuple[str, str]] = {
    "TaxFed_id":                 ("1", "id"),
    "TaxFed_AppliesToPartyID":   ("1", "AppliesToPartyID"),
    "TaxFed_Place_tc":           ("1", "Place_tc"),
    "TaxFed_Place_text":         ("1", "Place_text"),
    "TaxFed_Type_tc":            ("1", "Type_tc"),
    "TaxFed_Type_text":          ("1", "Type_text"),
    "TaxState_id":               ("2", "id"),
    "TaxState_AppliesToPartyID": ("2", "AppliesToPartyID"),
    "TaxState_Place_tc":         ("2", "Place_tc"),
    "TaxState_Place_text":       ("2", "Place_text"),
    "TaxState_Type_tc":          ("2", "Type_tc"),
    "TaxState_Type_text":        ("2", "Type_text"),
}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_xml(xml_string: str) -> ValidationResult:
    """Validate a single XML string against the DTCC IFW 21208 data dictionary."""
    errors: list[str] = []
    policy_number = ""

    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        return ValidationResult("UNKNOWN", False, [f"XML parse error: {e}"])

    for fd in FIELD_DEFINITIONS:
        col = fd["column_name"]
        grp = fd["group"]
        if grp == GROUP_META:
            continue

        # Tax fields: use dynamic lookup
        if grp in (GROUP_TAX_FED, GROUP_TAX_STATE):
            tax_info = _TAX_MAP.get(col)
            if tax_info:
                value = _extract_tax_field(root, tax_info[0], tax_info[1])
                _validate_field(fd, col, value, errors)
            continue

        # Party / Relation fields: skip (validated separately)
        if grp.startswith("party_") or grp.startswith("relation_"):
            continue

        # SOAP envelope namespace: special case
        if col == "SoapEnvelopeNs":
            # Just check envelope exists
            tag = root.tag
            if "Envelope" not in tag:
                errors.append("SOAP Envelope element not found")
            continue
        if col == "OperationNs":
            continue  # validated implicitly by OperationName existence

        # Static XPath fields
        xpath_entry = _XPATH.get(col)
        if xpath_entry is None:
            continue
        xpath, mode = xpath_entry
        value = _extract(root, xpath, mode)
        _validate_field(fd, col, value, errors)
        if col == "PolNumber" and value:
            policy_number = value

    # Conditional: ModalAmt required when ArrSubType tc=4
    arr_sub = _extract(root, f"{_ARR}/ns3:ArrSubType", "tc")
    modal   = _extract(root, f"{_ARR}/ModalAmt", "text")
    if arr_sub == "4" and not modal:
        errors.append(
            "Conditional field 'ModalAmt' required when ArrSubType tc=4 "
            "(Specified Amount)"
        )

    # Validate Party elements
    _validate_parties(root, errors)

    # Validate Relation elements
    _validate_relations(root, errors)

    return ValidationResult(
        policy_number=policy_number or "UNKNOWN",
        valid=len(errors) == 0,
        errors=errors,
    )


def _validate_field(fd: dict, col: str, value: str | None,
                    errors: list[str]) -> None:
    if fd["required"] and not value:
        errors.append(f"Required field '{col}' is missing or empty")
        return
    if not value:
        return
    ml = fd.get("max_length", 0)
    if ml and len(value) > ml:
        errors.append(f"Field '{col}' exceeds max length {ml} (got {len(value)})")
    av = fd.get("allowed_values", [])
    if av and value not in av:
        errors.append(f"Field '{col}' has invalid value '{value}'; allowed: {av}")
    rx = fd.get("regex", "")
    if rx and not re.match(rx, value):
        errors.append(f"Field '{col}' value '{value}' does not match pattern {rx}")


def _validate_parties(root: ET.Element, errors: list[str]) -> None:
    olife = root.find(
        ".//ns3:VI21208_Msg/ns3:TXLife/ns3:TXLifeRequest/ns3:OLifE", NS
    )
    if olife is None:
        errors.append("OLifE element not found")
        return
    for party_id in ("Party_Agent", "Party_Distributor",
                     "Party_Carrier", "Party_PrimaryOwner"):
        found = olife.find(f"ns3:Party[@id='{party_id}']", NS)
        if found is None:
            errors.append(f"Required Party '{party_id}' not found")


def _validate_relations(root: ET.Element, errors: list[str]) -> None:
    olife = root.find(
        ".//ns3:VI21208_Msg/ns3:TXLife/ns3:TXLifeRequest/ns3:OLifE", NS
    )
    if olife is None:
        return
    for rel_id in ("Agent_Relation", "Distributor_Relation",
                   "Carrier_Relation", "Owner_Relation"):
        found = olife.find(f"ns3:Relation[@id='{rel_id}']", NS)
        if found is None:
            errors.append(f"Required Relation '{rel_id}' not found")


def validate_all(xml_results: list[dict]) -> list[ValidationResult]:
    """Validate a batch of XML results."""
    return [validate_xml(r["xml_string"]) for r in xml_results]
