"""Shared pytest fixtures for PySpark tests."""

from __future__ import annotations

import csv
import os
import sys
import shutil
import tempfile
import uuid

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import get_column_names


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for all tests."""
    python_path = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("XMLGenerator-Tests")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + tempfile.mkdtemp())
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", python_path)
        .config("spark.pyspark.driver.python", python_path)
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def tmp_dir():
    """Per-test temporary directory, cleaned up after test."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Shared test data for the 21208 Withdrawal-Quote schema
# ---------------------------------------------------------------------------

_VALID_ROW_TEMPLATE: dict[str, str] = {
    "SoapEnvelopeNs": "http://www.w3.org/2003/05/soap-envelope",
    "SoapBodyPresent": "1",
    "OperationName": "processValueInquiry21208",
    "OperationNs": "http://service.iwa.dtcc.com/",
    "MessageType": "VI21208_Msg",
    "TXLifeRequest_PrimaryObjectID": "Holding_1",
    "TransRefGUID": "a1b2c3d4-17f1-4884-abcd-c4c769516559",
    "TransType_tc": "212",
    "TransType_text": "Values Inquiry",
    "TransSubType_tc": "21208",
    "TransSubType_text": "Withdrawal Quote",
    "TransExeDate": "2026-03-19",
    "TransExeTime": "13:29:42-05:00",
    "PendingResponseOK_tc": "0",
    "PendingResponseOK_text": "False",
    "Holding_id": "Holding_1",
    "HoldingTypeCode_tc": "2",
    "HoldingTypeCode_text": "Policy",
    "DistributorClientAcctNum": "026243",
    "Policy_CarrierPartyID": "Party_Carrier",
    "PolNumber": "ANN-2026-00001",
    "Policy_CarrierCode": "4500",
    "CusipNum": "10922P215",
    "Annuity_present": "1",
    "ArrMode_tc": "9",
    "ArrMode_text": "Single Pay",
    "ArrType_tc": "1004900058",
    "ArrType_text": "OneTimeWithdrawal",
    "ArrSubType_tc": "4",
    "ArrSubType_text": "Specified Amount",
    "ModalAmt": "2000.00",
    "SourceTransferAmtType_tc": "6",
    "SourceTransferAmtType_text": "ProrataAllFunds",
    "TaxFed_id": "ID_1",
    "TaxFed_AppliesToPartyID": "Party_PrimaryOwner",
    "TaxFed_Place_tc": "1",
    "TaxFed_Place_text": "Federal",
    "TaxFed_Type_tc": "1",
    "TaxFed_Type_text": "Default",
    "TaxState_id": "ID_2",
    "TaxState_AppliesToPartyID": "Party_PrimaryOwner",
    "TaxState_Place_tc": "2",
    "TaxState_Place_text": "State",
    "TaxState_Type_tc": "1",
    "TaxState_Type_text": "Default",
    "OLifEExtension_VendorCode": "87726",
    "OLifEExtension_ExtensionCode": "EXT",
    "AmountQualifier_tc": "2",
    "AmountQualifier_text": "Gross",
    "Party_Agent_id": "Party_Agent",
    "Party_Agent_PartyTypeCode_tc": "1",
    "Party_Agent_PartyTypeCode_text": "Person",
    "Party_Agent_FirstName": "ALEX",
    "Party_Agent_LastName": "ADVISOR",
    "Party_Agent_NIPRNumber": "14740776",
    "Party_Agent_IDPart": "4932",
    "Party_Agent_PartialIDType_tc": "1",
    "Party_Agent_PartialIDType_text": "SSN",
    "Party_Distributor_id": "Party_Distributor",
    "Party_Distributor_PartyTypeCode_tc": "2",
    "Party_Distributor_PartyTypeCode_text": "Organization",
    "Party_Distributor_DTCCMemberCode": "0015",
    "Party_Distributor_DTCCAssociatedMemberCode": "0015",
    "Party_Carrier_id": "Party_Carrier",
    "Party_Carrier_PartyTypeCode_tc": "2",
    "Party_Carrier_PartyTypeCode_text": "Organization",
    "Party_Carrier_DTCCMemberCode": "4602",
    "Party_Carrier_DTCCAssociatedMemberCode": "4602",
    "Party_Carrier_CarrierCode": "4500",
    "Party_PrimaryOwner_id": "Party_PrimaryOwner",
    "Party_PrimaryOwner_PartyTypeCode_tc": "1",
    "Party_PrimaryOwner_PartyTypeCode_text": "Person",
    "Party_PrimaryOwner_FirstName": "CASEY",
    "Party_PrimaryOwner_LastName": "OWNER",
    "Party_PrimaryOwner_IDPart": "3856",
    "Party_PrimaryOwner_PartialIDType_tc": "1",
    "Party_PrimaryOwner_PartialIDType_text": "SSN",
    "Party_PrimaryAnnuitant_id": "Party_PrimaryAnnuitant",
    "Party_PrimaryAnnuitant_PartyTypeCode_tc": "1",
    "Party_PrimaryAnnuitant_PartyTypeCode_text": "Person",
    "Party_PrimaryAnnuitant_FirstName": "CASEY",
    "Party_PrimaryAnnuitant_LastName": "OWNER",
    "Party_PrimaryAnnuitant_IDPart": "3856",
    "Party_PrimaryAnnuitant_PartialIDType_tc": "1",
    "Party_PrimaryAnnuitant_PartialIDType_text": "SSN",
    "Relation_Agent_id": "Agent_Relation",
    "Relation_Agent_OriginatingObjectID": "Holding_1",
    "Relation_Agent_RelatedObjectID": "Party_Agent",
    "Relation_Agent_OriginatingObjectType_tc": "4",
    "Relation_Agent_OriginatingObjectType_text": "Holding",
    "Relation_Agent_RelatedObjectType_tc": "6",
    "Relation_Agent_RelatedObjectType_text": "Party",
    "Relation_Agent_RoleCode_tc": "38",
    "Relation_Agent_RoleCode_text": "Servicing Agent",
    "Relation_Distributor_id": "Distributor_Relation",
    "Relation_Distributor_OriginatingObjectID": "Holding_1",
    "Relation_Distributor_RelatedObjectID": "Party_Distributor",
    "Relation_Distributor_OriginatingObjectType_tc": "4",
    "Relation_Distributor_OriginatingObjectType_text": "Holding",
    "Relation_Distributor_RelatedObjectType_tc": "6",
    "Relation_Distributor_RelatedObjectType_text": "Party",
    "Relation_Distributor_RoleCode_tc": "83",
    "Relation_Distributor_RoleCode_text": "Broker Dealer",
    "Relation_Carrier_id": "Carrier_Relation",
    "Relation_Carrier_OriginatingObjectID": "Holding_1",
    "Relation_Carrier_RelatedObjectID": "Party_Carrier",
    "Relation_Carrier_OriginatingObjectType_tc": "4",
    "Relation_Carrier_OriginatingObjectType_text": "Holding",
    "Relation_Carrier_RelatedObjectType_tc": "6",
    "Relation_Carrier_RelatedObjectType_text": "Party",
    "Relation_Carrier_RoleCode_tc": "87",
    "Relation_Carrier_RoleCode_text": "Carrier",
    "Relation_Owner_id": "Owner_Relation",
    "Relation_Owner_OriginatingObjectID": "Holding_1",
    "Relation_Owner_RelatedObjectID": "Party_PrimaryOwner",
    "Relation_Owner_OriginatingObjectType_tc": "4",
    "Relation_Owner_OriginatingObjectType_text": "Holding",
    "Relation_Owner_RelatedObjectType_tc": "6",
    "Relation_Owner_RelatedObjectType_text": "Party",
    "Relation_Owner_RoleCode_tc": "8",
    "Relation_Owner_RoleCode_text": "Owner",
    "Relation_Annuitant_id": "Annuitant_Relation",
    "Relation_Annuitant_OriginatingObjectID": "Holding_1",
    "Relation_Annuitant_RelatedObjectID": "Party_PrimaryAnnuitant",
    "Relation_Annuitant_OriginatingObjectType_tc": "4",
    "Relation_Annuitant_OriginatingObjectType_text": "Holding",
    "Relation_Annuitant_RelatedObjectType_tc": "6",
    "Relation_Annuitant_RelatedObjectType_text": "Party",
    "Relation_Annuitant_RoleCode_tc": "35",
    "Relation_Annuitant_RoleCode_text": "Annuitant",
    "ExpectedToPass": "1",
    "FailureReason": "",
}


def make_test_row(pol_number: str = "ANN-2026-00001", **overrides) -> dict[str, str]:
    """
    Build a valid 21208 WD-Quote row with all required columns populated.

    Any column may be overridden via keyword arguments.
    """
    columns = get_column_names()
    row = {c: _VALID_ROW_TEMPLATE.get(c, "") for c in columns}
    row["PolNumber"] = pol_number
    row["TransRefGUID"] = str(uuid.uuid4())
    row.update(overrides)
    return row


def write_test_csv(
    path: str,
    rows: list[dict[str, str]] | None = None,
    num_valid: int = 1,
) -> str:
    """
    Write a test CSV with the correct 21208 schema columns.

    If *rows* is provided, those are written directly.
    Otherwise, *num_valid* rows are generated with sequential PolNumbers.
    """
    columns = get_column_names()

    if rows is None:
        rows = []
        for i in range(1, num_valid + 1):
            rows.append(make_test_row(pol_number=f"ANN-2026-{i:05d}"))

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for r in rows:
            writer.writerow({c: r.get(c, "") for c in columns})

    return path
