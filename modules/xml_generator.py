"""
Generates SOAP-wrapped ACORD-compliant TXLifeRequest XML
(TransType 212 / TransSubType 21208 – Withdrawal Quote)
from PySpark DataFrame rows **or** plain dicts.

Uses string-template approach because Python’s ElementTree reserves
the ns\\d+ prefix pattern.  The SOAP structure is fixed; only values vary.
"""

from __future__ import annotations

import os
from typing import Any
from xml.sax.saxutils import escape as xml_escape

from pyspark.sql import DataFrame, Row


def _v(row: dict | Row, key: str) -> str | None:
    """Return a stripped string value or None."""
    try:
        val = row[key] if isinstance(row, dict) else row[key]
    except (KeyError, ValueError, IndexError):
        return None
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _esc(val: str | None) -> str:
    """XML-escape a value, returning empty string for None."""
    return xml_escape(val) if val else ""


def _tc_element(tag: str, tc: str | None, text: str | None,
                ns: str = "ns3", indent: str = "") -> str:
    """Build a tc-attributed element line.  Returns empty string if both are empty."""
    if not tc and not text:
        return ""
    parts = [f"{indent}<{ns}:{tag}"]
    if tc:
        parts.append(f' tc="{_esc(tc)}"')
    parts.append(f">{_esc(text or '')}</{ns}:{tag}>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_xml_from_row(row: dict | Row) -> str:
    """
    Convert a single row into a fully SOAP-wrapped ACORD 21208 XML string.
    """
    lines: list[str] = []
    _a = lines.append

    # SOAP Envelope
    _a('<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">')
    _a('\t<soap:Body>')
    _a('\t\t<ns2:processValueInquiry21208'
       ' xmlns:ns2="http://service.iwa.dtcc.com/"'
       ' xmlns:S="http://schemas.xmlsoap.org/soap/envelope/"'
       ' xmlns:ns3="http://ACORD.org/Standards/Life/2"'
       ' xmlns:ns4="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"'
       ' xmlns:ns5="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"'
       ' xmlns:ns6="http://www.w3.org/2000/09/xmldsig#"'
       ' xmlns:soap="http://www.w3.org/2003/05/soap-envelope">')

    # VI21208_Msg > TXLife > TXLifeRequest
    _a('\t\t\t<ns3:VI21208_Msg>')
    _a('\t\t\t\t<ns3:TXLife>')

    primary = _esc(_v(row, "TXLifeRequest_PrimaryObjectID") or "Holding_1")
    _a(f'\t\t\t\t\t<ns3:TXLifeRequest PrimaryObjectID="{primary}">')

    # Transaction header
    _a(f'\t\t\t\t\t\t<ns3:TransRefGUID>{_esc(_v(row, "TransRefGUID"))}</ns3:TransRefGUID>')
    _a(_tc_element("TransType",    _v(row, "TransType_tc"),    _v(row, "TransType_text"),    indent="\t\t\t\t\t\t"))
    _a(_tc_element("TransSubType", _v(row, "TransSubType_tc"), _v(row, "TransSubType_text"), indent="\t\t\t\t\t\t"))
    _a(f'\t\t\t\t\t\t<ns3:TransExeDate>{_esc(_v(row, "TransExeDate"))}</ns3:TransExeDate>')
    _a(f'\t\t\t\t\t\t<ns3:TransExeTime>{_esc(_v(row, "TransExeTime"))}</ns3:TransExeTime>')
    _a(_tc_element("PendingResponseOK", _v(row, "PendingResponseOK_tc"), _v(row, "PendingResponseOK_text"), indent="\t\t\t\t\t\t"))

    # OLifE
    _a('\t\t\t\t\t\t<ns3:OLifE>')

    # Holding
    holding_id = _esc(_v(row, "Holding_id") or "Holding_1")
    _a(f'\t\t\t\t\t\t\t<ns3:Holding id="{holding_id}">')
    _a(_tc_element("HoldingTypeCode", _v(row, "HoldingTypeCode_tc"), _v(row, "HoldingTypeCode_text"), indent="\t\t\t\t\t\t\t\t"))
    dist = _v(row, "DistributorClientAcctNum")
    if dist:
        _a(f'\t\t\t\t\t\t\t\t<ns3:DistributorClientAcctNum>{_esc(dist)}</ns3:DistributorClientAcctNum>')

    # Policy
    carrier_party = _esc(_v(row, "Policy_CarrierPartyID") or "Party_Carrier")
    _a(f'\t\t\t\t\t\t\t\t<ns3:Policy CarrierPartyID="{carrier_party}">')
    pol = _v(row, "PolNumber")
    if pol:
        _a(f'\t\t\t\t\t\t\t\t\t<ns3:PolNumber>{_esc(pol)}</ns3:PolNumber>')
    cc = _v(row, "Policy_CarrierCode")
    if cc:
        _a(f'\t\t\t\t\t\t\t\t\t<ns3:CarrierCode>{_esc(cc)}</ns3:CarrierCode>')
    cusip = _v(row, "CusipNum")
    if cusip:
        _a(f'\t\t\t\t\t\t\t\t\t<ns3:CusipNum>{_esc(cusip)}</ns3:CusipNum>')
    if _v(row, "Annuity_present") == "1":
        _a('\t\t\t\t\t\t\t\t\t<ns3:Annuity/>')
    _a('\t\t\t\t\t\t\t\t</ns3:Policy>')

    # Arrangement
    _a('\t\t\t\t\t\t\t\t<ns3:Arrangement>')
    _a(_tc_element("ArrMode",    _v(row, "ArrMode_tc"),    _v(row, "ArrMode_text"),    indent="\t\t\t\t\t\t\t\t\t"))
    _a(_tc_element("ArrType",    _v(row, "ArrType_tc"),    _v(row, "ArrType_text"),    indent="\t\t\t\t\t\t\t\t\t"))
    _a(_tc_element("ArrSubType", _v(row, "ArrSubType_tc"), _v(row, "ArrSubType_text"), indent="\t\t\t\t\t\t\t\t\t"))
    modal = _v(row, "ModalAmt")
    if modal:
        _a(f'\t\t\t\t\t\t\t\t\t<ModalAmt>{_esc(modal)}</ModalAmt>')
    _a(_tc_element("SourceTransferAmtType", _v(row, "SourceTransferAmtType_tc"), _v(row, "SourceTransferAmtType_text"), indent="\t\t\t\t\t\t\t\t\t"))

    # TaxWithholding (Federal)
    _build_tax_xml(lines, row, "TaxFed")
    # TaxWithholding (State)
    _build_tax_xml(lines, row, "TaxState")

    # OLifEExtension
    vendor   = _v(row, "OLifEExtension_VendorCode")
    ext_code = _v(row, "OLifEExtension_ExtensionCode")
    amt_tc   = _v(row, "AmountQualifier_tc")
    if vendor or amt_tc:
        attrs = ""
        if vendor:
            attrs += f' VendorCode="{_esc(vendor)}"'
        if ext_code:
            attrs += f' ExtensionCode="{_esc(ext_code)}"'
        _a(f'\t\t\t\t\t\t\t\t\t<ns3:OLifEExtension{attrs}>')
        _a(_tc_element("AmountQualifier", amt_tc, _v(row, "AmountQualifier_text"), indent="\t\t\t\t\t\t\t\t\t\t"))
        _a('\t\t\t\t\t\t\t\t\t</ns3:OLifEExtension>')

    _a('\t\t\t\t\t\t\t\t</ns3:Arrangement>')
    _a('\t\t\t\t\t\t\t</ns3:Holding>')

    # Parties
    _build_person_party_xml(lines, row, "Party_Agent",
                            producer_field="Party_Agent_NIPRNumber")
    _build_org_party_xml(lines, row, "Party_Distributor")
    _build_carrier_party_xml(lines, row)
    _build_person_party_xml(lines, row, "Party_PrimaryOwner")
    if _v(row, "Party_PrimaryAnnuitant_id"):
        _build_person_party_xml(lines, row, "Party_PrimaryAnnuitant")

    # Relations
    for prefix in ("Relation_Agent", "Relation_Distributor",
                   "Relation_Carrier", "Relation_Owner",
                   "Relation_Annuitant"):
        _build_relation_xml(lines, row, prefix)

    # Close tree
    _a('\t\t\t\t\t\t</ns3:OLifE>')
    _a('\t\t\t\t\t</ns3:TXLifeRequest>')
    _a('\t\t\t\t</ns3:TXLife>')
    _a('\t\t\t</ns3:VI21208_Msg>')
    _a('\t\t</ns2:processValueInquiry21208>')
    _a('\t</soap:Body>')
    _a('</soap:Envelope>')

    # Filter out empty lines from skipped optional elements
    return "\n".join(line for line in lines if line) + "\n"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_tax_xml(lines: list[str], row: dict | Row, prefix: str) -> None:
    """Build a TaxWithholding child for the given prefix (TaxFed / TaxState)."""
    tw_id   = _v(row, f"{prefix}_id")
    applies = _v(row, f"{prefix}_AppliesToPartyID")
    if not tw_id:
        return
    attrs = f'id="{_esc(tw_id)}"'
    if applies:
        attrs += f' AppliesToPartyID="{_esc(applies)}"'
    lines.append(f'\t\t\t\t\t\t\t\t\t<ns3:TaxWithholding {attrs}>')
    lines.append(_tc_element("TaxWithholdingPlace",
                             _v(row, f"{prefix}_Place_tc"),
                             _v(row, f"{prefix}_Place_text"),
                             indent="\t\t\t\t\t\t\t\t\t\t"))
    lines.append(_tc_element("TaxWithholdingType",
                             _v(row, f"{prefix}_Type_tc"),
                             _v(row, f"{prefix}_Type_text"),
                             indent="\t\t\t\t\t\t\t\t\t\t"))
    lines.append('\t\t\t\t\t\t\t\t\t</ns3:TaxWithholding>')


def _build_person_party_xml(lines: list[str], row: dict | Row,
                            prefix: str, *,
                            producer_field: str = "") -> None:
    """Build a Person-type Party element."""
    pid = _v(row, f"{prefix}_id")
    if not pid:
        return
    T7 = "\t\t\t\t\t\t\t"
    T8 = T7 + "\t"
    T9 = T8 + "\t"
    lines.append(f'{T7}<ns3:Party id="{_esc(pid)}">')
    lines.append(_tc_element("PartyTypeCode",
                             _v(row, f"{prefix}_PartyTypeCode_tc"),
                             _v(row, f"{prefix}_PartyTypeCode_text"),
                             indent=T8))
    lines.append(f'{T8}<ns3:Person>')
    fn = _v(row, f"{prefix}_FirstName")
    ln = _v(row, f"{prefix}_LastName")
    if fn:
        lines.append(f'{T9}<ns3:FirstName>{_esc(fn)}</ns3:FirstName>')
    if ln:
        lines.append(f'{T9}<ns3:LastName>{_esc(ln)}</ns3:LastName>')
    lines.append(f'{T8}</ns3:Person>')

    if producer_field:
        nipr = _v(row, producer_field)
        if nipr:
            lines.append(f'{T8}<ns3:Producer>')
            lines.append(f'{T9}<ns3:NIPRNumber>{_esc(nipr)}</ns3:NIPRNumber>')
            lines.append(f'{T8}</ns3:Producer>')

    id_part = _v(row, f"{prefix}_IDPart")
    if id_part:
        lines.append(f'{T8}<ns3:PartialIdentification>')
        lines.append(f'{T9}<ns3:IdentificationPart>{_esc(id_part)}</ns3:IdentificationPart>')
        lines.append(_tc_element("PartialIDType",
                                 _v(row, f"{prefix}_PartialIDType_tc"),
                                 _v(row, f"{prefix}_PartialIDType_text"),
                                 indent=T9))
        lines.append(f'{T8}</ns3:PartialIdentification>')
    lines.append(f'{T7}</ns3:Party>')


def _build_org_party_xml(lines: list[str], row: dict | Row,
                         prefix: str) -> None:
    """Build an Organization-type Party (Distributor)."""
    pid = _v(row, f"{prefix}_id")
    if not pid:
        return
    T7 = "\t\t\t\t\t\t\t"
    T8 = T7 + "\t"
    T9 = T8 + "\t"
    lines.append(f'{T7}<ns3:Party id="{_esc(pid)}">')
    lines.append(_tc_element("PartyTypeCode",
                             _v(row, f"{prefix}_PartyTypeCode_tc"),
                             _v(row, f"{prefix}_PartyTypeCode_text"),
                             indent=T8))
    lines.append(f'{T8}<ns3:Organization>')
    mc = _v(row, f"{prefix}_DTCCMemberCode")
    amc = _v(row, f"{prefix}_DTCCAssociatedMemberCode")
    if mc:
        lines.append(f'{T9}<ns3:DTCCMemberCode>{_esc(mc)}</ns3:DTCCMemberCode>')
    if amc:
        lines.append(f'{T9}<ns3:DTCCAssociatedMemberCode>{_esc(amc)}</ns3:DTCCAssociatedMemberCode>')
    lines.append(f'{T8}</ns3:Organization>')
    lines.append(f'{T7}</ns3:Party>')


def _build_carrier_party_xml(lines: list[str], row: dict | Row) -> None:
    """Build the Carrier Party element (Organization + Carrier child)."""
    prefix = "Party_Carrier"
    pid = _v(row, f"{prefix}_id")
    if not pid:
        return
    T7 = "\t\t\t\t\t\t\t"
    T8 = T7 + "\t"
    T9 = T8 + "\t"
    lines.append(f'{T7}<ns3:Party id="{_esc(pid)}">')
    lines.append(_tc_element("PartyTypeCode",
                             _v(row, f"{prefix}_PartyTypeCode_tc"),
                             _v(row, f"{prefix}_PartyTypeCode_text"),
                             indent=T8))
    lines.append(f'{T8}<ns3:Organization>')
    mc = _v(row, f"{prefix}_DTCCMemberCode")
    amc = _v(row, f"{prefix}_DTCCAssociatedMemberCode")
    if mc:
        lines.append(f'{T9}<ns3:DTCCMemberCode>{_esc(mc)}</ns3:DTCCMemberCode>')
    if amc:
        lines.append(f'{T9}<ns3:DTCCAssociatedMemberCode>{_esc(amc)}</ns3:DTCCAssociatedMemberCode>')
    lines.append(f'{T8}</ns3:Organization>')
    cc = _v(row, f"{prefix}_CarrierCode")
    if cc:
        lines.append(f'{T8}<ns3:Carrier>')
        lines.append(f'{T9}<ns3:CarrierCode>{_esc(cc)}</ns3:CarrierCode>')
        lines.append(f'{T8}</ns3:Carrier>')
    lines.append(f'{T7}</ns3:Party>')


def _build_relation_xml(lines: list[str], row: dict | Row,
                        prefix: str) -> None:
    """Build a Relation element from the given column prefix."""
    rid = _v(row, f"{prefix}_id")
    if not rid:
        return
    T7 = "\t\t\t\t\t\t\t"
    T8 = T7 + "\t"
    attrs = f'id="{_esc(rid)}"'
    orig = _v(row, f"{prefix}_OriginatingObjectID")
    rel  = _v(row, f"{prefix}_RelatedObjectID")
    if orig:
        attrs += f' OriginatingObjectID="{_esc(orig)}"'
    if rel:
        attrs += f' RelatedObjectID="{_esc(rel)}"'
    lines.append(f'{T7}<ns3:Relation {attrs}>')
    lines.append(_tc_element("OriginatingObjectType",
                             _v(row, f"{prefix}_OriginatingObjectType_tc"),
                             _v(row, f"{prefix}_OriginatingObjectType_text"),
                             indent=T8))
    lines.append(_tc_element("RelatedObjectType",
                             _v(row, f"{prefix}_RelatedObjectType_tc"),
                             _v(row, f"{prefix}_RelatedObjectType_text"),
                             indent=T8))
    lines.append(_tc_element("RelationRoleCode",
                             _v(row, f"{prefix}_RoleCode_tc"),
                             _v(row, f"{prefix}_RoleCode_text"),
                             indent=T8))
    lines.append(f'{T7}</ns3:Relation>')


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------

def _filename_for_row(row: dict | Row) -> str:
    """Derive an output filename from the row's PolNumber."""
    pol = _v(row, "PolNumber") or "UNKNOWN"
    safe_pol = pol.replace("/", "_").replace("\\", "_")
    return f"WDQuote_{safe_pol}.xml"


def generate_all_xmls(df: DataFrame, output_dir: str) -> list[dict]:
    """
    Generate one XML file per DataFrame row and write to *output_dir*.
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
            "pol_number": _v(row, "PolNumber") or "",
            "filename": filename,
            "filepath": filepath,
            "xml_string": xml_str,
        })

    return results
