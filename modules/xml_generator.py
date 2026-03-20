"""
Generates ACORD-compliant TXLifeRequest XML (TransType 212 - Values Inquiry)
from PySpark DataFrame rows.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from datetime import date
from typing import Any

from pyspark.sql import DataFrame, Row

from config.schema_config import (
    ACORD_NAMESPACE,
    ACORD_NS_PREFIX,
    FIELD_DEFINITIONS,
    get_field_by_column,
)


def _val(value: Any) -> str | None:
    """Convert a Spark Row value to a string suitable for XML, or None if empty."""
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    return s if s else None


def _make_element(parent: ET.Element, tag: str, text: str | None = None, **attribs) -> ET.Element:
    """Create a sub-element with optional text and attributes."""
    el = ET.SubElement(parent, f"{{{ACORD_NAMESPACE}}}{tag}")
    if text is not None:
        el.text = text
    for k, v in attribs.items():
        el.set(k, v)
    return el


def generate_xml_from_row(row: Row) -> str:
    """
    Convert a single Spark Row into an ACORD TXLifeRequest XML string.

    Returns well-formed XML with the ACORD namespace.
    """
    ET.register_namespace(ACORD_NS_PREFIX, ACORD_NAMESPACE)

    txlife = ET.Element(f"{{{ACORD_NAMESPACE}}}TXLife")

    holding_id = _val(row["HoldingID"]) or "Holding_1"
    txlife_req = _make_element(txlife, "TXLifeRequest", PrimaryObjectID=holding_id)

    _add_transaction_fields(txlife_req, row)

    olife = _make_element(txlife_req, "OLifE")
    holding = _make_element(olife, "Holding", id=holding_id)

    _add_holding_fields(holding, row)

    policy = _make_element(holding, "Policy")
    _add_policy_fields(policy, row)

    ET.indent(txlife, space="    ")
    return ET.tostring(txlife, encoding="unicode", xml_declaration=True)


def _add_transaction_fields(parent: ET.Element, row: Row) -> None:
    """Add transaction-level elements to TXLifeRequest."""
    trans_fields = [
        "TransRefGUID", "TransType", "TransSubType",
        "TransExeDate", "TransExeTime", "TransEffDate",
        "InquiryLevel", "InquiryView", "NoResponseOK", "TestIndicator",
    ]
    for col in trans_fields:
        val = _val(row[col])
        if val is None:
            continue
        fd = get_field_by_column(col)
        attribs = {}
        if fd and fd.get("tc_code"):
            attribs["tc"] = fd["tc_code"]
            desc_map = fd.get("tc_description_map")
            tc_desc = fd.get("tc_description")
            if desc_map and val in desc_map:
                text = desc_map[val]
            elif tc_desc:
                text = tc_desc
            else:
                text = val
            _make_element(parent, col, text, **attribs)
        elif fd and fd.get("tc_description_map") and val in fd["tc_description_map"]:
            attribs["tc"] = val
            _make_element(parent, col, fd["tc_description_map"][val], **attribs)
        else:
            _make_element(parent, col, val)


def _add_holding_fields(parent: ET.Element, row: Row) -> None:
    """Add HoldingTypeCode to Holding element."""
    val = _val(row["HoldingTypeCode"])
    if val is not None:
        fd = get_field_by_column("HoldingTypeCode")
        attribs = {}
        if fd and fd.get("tc_code"):
            attribs["tc"] = fd["tc_code"]
        desc = fd.get("tc_description", val) if fd else val
        _make_element(parent, "HoldingTypeCode", desc, **attribs)


def _add_policy_fields(parent: ET.Element, row: Row) -> None:
    """Add Policy child elements."""
    policy_fields = [
        "PolNumber", "ProductCode", "CarrierCode",
        "LineOfBusiness", "ProductType", "PolicyStatus",
    ]
    for col in policy_fields:
        val = _val(row[col])
        if val is None:
            continue
        fd = get_field_by_column(col)
        attribs = {}
        if fd and fd.get("tc_description_map") and val in fd["tc_description_map"]:
            attribs["tc"] = val
            _make_element(parent, col, fd["tc_description_map"][val], **attribs)
        else:
            _make_element(parent, col, val)


def _filename_for_row(row: Row) -> str:
    """Derive an output filename from the row's PolNumber."""
    pol = _val(row["PolNumber"]) or "UNKNOWN"
    safe_pol = pol.replace("/", "_").replace("\\", "_")
    return f"ValuesInquiry_{safe_pol}.xml"


def generate_all_xmls(df: DataFrame, output_dir: str) -> list[dict]:
    """
    Generate one XML file per DataFrame row and write to output_dir.

    Returns a list of dicts with keys: pol_number, filename, filepath, xml_string.
    """
    os.makedirs(output_dir, exist_ok=True)
    results = []

    for row in df.collect():
        xml_str = generate_xml_from_row(row)
        filename = _filename_for_row(row)
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(xml_str)

        results.append({
            "pol_number": _val(row["PolNumber"]) or "",
            "filename": filename,
            "filepath": filepath,
            "xml_string": xml_str,
        })

    return results
