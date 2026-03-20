"""
Generates a sample CSV file with realistic annuity product data
matching the ACORD Values Inquiry request schema.

Produces 9 data rows (7 valid, 2 intentionally invalid for validation testing).
"""

import csv
import os
import uuid
from datetime import date

from config.schema_config import get_column_names


_VALID_ROWS = [
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy1")),
        "TransType": "212",
        "TransSubType": "21207",
        "TransExeDate": "2026-03-19",
        "TransExeTime": "10:30:00",
        "TransEffDate": "2026-03-19",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "0",
        "TestIndicator": "1",
        "HoldingID": "Holding_1",
        "HoldingTypeCode": "2",
        "PolNumber": "ANN-2026-00001",
        "ProductCode": "VA-100",
        "CarrierCode": "12345",
        "LineOfBusiness": "2",
        "ProductType": "Variable Annuity",
        "PolicyStatus": "1",
    },
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy2")),
        "TransType": "212",
        "TransSubType": "21207",
        "TransExeDate": "2026-03-19",
        "TransExeTime": "10:31:00",
        "TransEffDate": "2026-03-19",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "0",
        "TestIndicator": "1",
        "HoldingID": "Holding_2",
        "HoldingTypeCode": "2",
        "PolNumber": "ANN-2026-00002",
        "ProductCode": "FIA-200",
        "CarrierCode": "12345",
        "LineOfBusiness": "2",
        "ProductType": "Fixed Indexed Annuity",
        "PolicyStatus": "1",
    },
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy3")),
        "TransType": "212",
        "TransSubType": "21201",
        "TransExeDate": "2026-03-18",
        "TransExeTime": "09:15:00",
        "TransEffDate": "2026-03-18",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "1",
        "TestIndicator": "0",
        "HoldingID": "Holding_3",
        "HoldingTypeCode": "2",
        "PolNumber": "ANN-2026-00003",
        "ProductCode": "SPIA-300",
        "CarrierCode": "67890",
        "LineOfBusiness": "2",
        "ProductType": "Single Premium Immediate Annuity",
        "PolicyStatus": "1",
    },
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy4")),
        "TransType": "212",
        "TransSubType": "21203",
        "TransExeDate": "2026-03-17",
        "TransExeTime": "14:00:00",
        "TransEffDate": "",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "0",
        "TestIndicator": "1",
        "HoldingID": "Holding_4",
        "HoldingTypeCode": "2",
        "PolNumber": "ANN-2026-00004",
        "ProductCode": "FA-400",
        "CarrierCode": "67890",
        "LineOfBusiness": "2",
        "ProductType": "Fixed Annuity",
        "PolicyStatus": "11",
    },
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy5")),
        "TransType": "212",
        "TransSubType": "21204",
        "TransExeDate": "2026-03-16",
        "TransExeTime": "08:45:30",
        "TransEffDate": "2026-03-16",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "0",
        "TestIndicator": "1",
        "HoldingID": "Holding_5",
        "HoldingTypeCode": "2",
        "PolNumber": "ANN-2026-00005",
        "ProductCode": "DIA-500",
        "CarrierCode": "11111",
        "LineOfBusiness": "2",
        "ProductType": "Deferred Income Annuity",
        "PolicyStatus": "1",
    },
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy6")),
        "TransType": "212",
        "TransSubType": "21205",
        "TransExeDate": "2026-03-15",
        "TransExeTime": "16:20:00",
        "TransEffDate": "2026-03-15",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "1",
        "TestIndicator": "0",
        "HoldingID": "Holding_6",
        "HoldingTypeCode": "2",
        "PolNumber": "ANN-2026-00006",
        "ProductCode": "RILA-600",
        "CarrierCode": "11111",
        "LineOfBusiness": "2",
        "ProductType": "Registered Index-Linked Annuity",
        "PolicyStatus": "1",
    },
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy7")),
        "TransType": "212",
        "TransSubType": "21206",
        "TransExeDate": "2026-03-14",
        "TransExeTime": "11:00:00",
        "TransEffDate": "2026-03-14",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "0",
        "TestIndicator": "1",
        "HoldingID": "Holding_7",
        "HoldingTypeCode": "2",
        "PolNumber": "ANN-2026-00007",
        "ProductCode": "MVA-700",
        "CarrierCode": "22222",
        "LineOfBusiness": "2",
        "ProductType": "Market Value Adjusted Annuity",
        "PolicyStatus": "1",
    },
]

_INVALID_ROWS = [
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy8")),
        "TransType": "212",
        "TransSubType": "21207",
        "TransExeDate": "03-19-2026",  # INVALID: wrong date format
        "TransExeTime": "10:30:00",
        "TransEffDate": "2026-03-19",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "0",
        "TestIndicator": "1",
        "HoldingID": "Holding_8",
        "HoldingTypeCode": "2",
        "PolNumber": "ANN-2026-00008",
        "ProductCode": "VA-800",
        "CarrierCode": "12345",
        "LineOfBusiness": "2",
        "ProductType": "Variable Annuity",
        "PolicyStatus": "1",
    },
    {
        "TransRefGUID": str(uuid.uuid5(uuid.NAMESPACE_DNS, "policy9")),
        "TransType": "212",
        "TransSubType": "21207",
        "TransExeDate": "2026-03-19",
        "TransExeTime": "10:30:00",
        "TransEffDate": "2026-03-19",
        "InquiryLevel": "",
        "InquiryView": "",
        "NoResponseOK": "0",
        "TestIndicator": "1",
        "HoldingID": "Holding_9",
        "HoldingTypeCode": "2",
        "PolNumber": "",  # INVALID: missing required PolNumber
        "ProductCode": "FIA-900",
        "CarrierCode": "12345",
        "LineOfBusiness": "2",
        "ProductType": "Fixed Indexed Annuity",
        "PolicyStatus": "1",
    },
]


def generate_sample_csv(output_path: str, num_valid: int = 7, num_invalid: int = 2) -> str:
    """
    Write a sample CSV with header + data rows.

    Args:
        output_path: file path for the CSV
        num_valid: number of valid rows to include (max 7)
        num_invalid: number of intentionally invalid rows (max 2)

    Returns:
        The output path written to.
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    columns = get_column_names()
    rows = _VALID_ROWS[:num_valid] + _INVALID_ROWS[:num_invalid]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return output_path


def get_sample_rows(include_invalid: bool = True):
    """Return sample row dicts (for programmatic use without CSV I/O)."""
    rows = list(_VALID_ROWS)
    if include_invalid:
        rows.extend(_INVALID_ROWS)
    return rows
