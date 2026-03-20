"""Tests for modules.pov_validator."""

from __future__ import annotations

import csv
import os
import tempfile

import pytest

from config.pov_record_layouts import RECORD_LAYOUTS
from modules.pov_parser import parse_file
from modules.pov_flattener import flatten_parsed_file, write_csv
from modules.pov_validator import (
    validate_flattened_csv,
    write_validation_report,
    ValidationReport,
)


def _build_line(record_type: str, field_values: dict[str, str] | None = None) -> str:
    layout = RECORD_LAYOUTS[record_type]
    parts: list[str] = []
    for field_name, width in layout:
        if field_values and field_name in field_values:
            val = field_values[field_name]
        else:
            val = ""
        parts.append(val.ljust(width)[:width])
    return "".join(parts)


def _build_pov_content(contracts: list[dict]) -> str:
    lines: list[str] = []
    lines.append(_build_line("100", {
        "Submitters_Code": "C", "Record_Type": "10",
        "Submitting_Participant_Number": "1234",
        "Valuation_Date": "20260319",
    }))
    lines.append(_build_line("120", {
        "Submitters_Code": "C", "Record_Type": "12",
        "Contra_Participant_Number": "5678",
    }))
    for contract in contracts:
        for rt, fv in sorted(contract.items()):
            if isinstance(fv, list):
                for item in fv:
                    lines.append(_build_line(rt, item))
            else:
                lines.append(_build_line(rt, fv))
    return "\n".join(lines)


CONTRACT = {
    "1301": {
        "Submitters_Code": "C", "Record_Type": "13",
        "Sequence_Number": "01",
        "Contract_Number": "VAL-001",
        "CUSIP_Number": "XYZ789012",
        "Contract_Status": "01",
        "Product_Type_Code": "VA",
        "Contract_State": "TX",
    },
    "1302": {
        "Submitters_Code": "C", "Record_Type": "13",
        "Sequence_Number": "02",
        "Contract_Number": "VAL-001",
        "Contract_Value_Amount_1": "0000000250000.00",
        "Contract_Value_Qualifier_1": "GAV",
    },
}

MULTI_FUND_CONTRACT = {
    "1301": {
        "Submitters_Code": "C", "Record_Type": "13",
        "Sequence_Number": "01",
        "Contract_Number": "VAL-002",
        "Contract_Status": "01",
    },
    "1303": [
        {
            "Submitters_Code": "C", "Record_Type": "13",
            "Sequence_Number": "03",
            "Contract_Number": "VAL-002",
            "CUSIP_Fund_ID_Sub_Fund_ID": "FUND-X",
            "Fund_Value": "0000000075000.00",
        },
        {
            "Submitters_Code": "C", "Record_Type": "13",
            "Sequence_Number": "03",
            "Contract_Number": "VAL-002",
            "CUSIP_Fund_ID_Sub_Fund_ID": "FUND-Y",
            "Fund_Value": "0000000025000.00",
        },
    ],
}


def _run_full_pipeline(contracts: list[dict]):
    """Parse → flatten → write CSV → validate; returns (report, paths to clean up)."""
    content = _build_pov_content(contracts)
    fd, pov_path = tempfile.mkstemp(suffix=".txt")
    with os.fdopen(fd, "w") as fh:
        fh.write(content)

    parsed = parse_file(pov_path)
    flattened = flatten_parsed_file(parsed)

    csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
    os.close(csv_fd)
    write_csv(flattened, csv_path)

    report = validate_flattened_csv(csv_path, parsed, flattened)
    return report, pov_path, csv_path


class TestValidateFlattened:
    def test_valid_single_contract(self):
        report, pov_path, csv_path = _run_full_pipeline([CONTRACT])
        try:
            assert report.valid is True
            assert report.mismatch_count == 0
            assert report.total_fields_checked > 0
            assert report.expected_contract_count == 1
            assert report.actual_contract_count == 1
        finally:
            os.unlink(pov_path)
            os.unlink(csv_path)

    def test_valid_two_contracts(self):
        report, pov_path, csv_path = _run_full_pipeline([CONTRACT, MULTI_FUND_CONTRACT])
        try:
            assert report.valid is True
            assert report.actual_contract_count == 2
        finally:
            os.unlink(pov_path)
            os.unlink(csv_path)

    def test_valid_repeating_records(self):
        report, pov_path, csv_path = _run_full_pipeline([MULTI_FUND_CONTRACT])
        try:
            assert report.valid is True
            assert report.mismatch_count == 0
        finally:
            os.unlink(pov_path)
            os.unlink(csv_path)

    def test_tampered_csv_detected(self):
        content = _build_pov_content([CONTRACT])
        fd, pov_path = tempfile.mkstemp(suffix=".txt")
        with os.fdopen(fd, "w") as fh:
            fh.write(content)

        parsed = parse_file(pov_path)
        flattened = flatten_parsed_file(parsed)

        csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
        os.close(csv_fd)
        write_csv(flattened, csv_path)

        # Tamper with the CSV
        with open(csv_path, "r") as fh:
            csv_content = fh.read()
        csv_content = csv_content.replace("XYZ789012", "TAMPERED!")
        with open(csv_path, "w") as fh:
            fh.write(csv_content)

        report = validate_flattened_csv(csv_path, parsed, flattened)
        try:
            assert report.valid is False
            assert report.mismatch_count >= 1
            mismatch_fields = [m.field_name for m in report.mismatches]
            assert "CUSIP_Number" in mismatch_fields
        finally:
            os.unlink(pov_path)
            os.unlink(csv_path)

    def test_missing_csv_file(self):
        content = _build_pov_content([CONTRACT])
        fd, pov_path = tempfile.mkstemp(suffix=".txt")
        with os.fdopen(fd, "w") as fh:
            fh.write(content)

        parsed = parse_file(pov_path)
        flattened = flatten_parsed_file(parsed)

        report = validate_flattened_csv("/nonexistent.csv", parsed, flattened)
        try:
            assert report.valid is False
            assert any("not found" in e for e in report.errors)
        finally:
            os.unlink(pov_path)

    def test_to_summary_dict(self):
        report, pov_path, csv_path = _run_full_pipeline([CONTRACT])
        try:
            summary = report.to_summary_dict()
            assert "valid" in summary
            assert "total_fields_checked" in summary
            assert summary["valid"] is True
        finally:
            os.unlink(pov_path)
            os.unlink(csv_path)


class TestWriteValidationReport:
    def test_report_written(self):
        report, pov_path, csv_path = _run_full_pipeline([CONTRACT])
        rpt_fd, rpt_path = tempfile.mkstemp(suffix=".txt")
        os.close(rpt_fd)
        try:
            write_validation_report(report, rpt_path)
            with open(rpt_path, "r") as fh:
                content = fh.read()
            assert "PASS" in content
            assert "Validation Report" in content
        finally:
            os.unlink(pov_path)
            os.unlink(csv_path)
            os.unlink(rpt_path)

    def test_failed_report_shows_mismatches(self):
        content = _build_pov_content([CONTRACT])
        fd, pov_path = tempfile.mkstemp(suffix=".txt")
        with os.fdopen(fd, "w") as fh:
            fh.write(content)

        parsed = parse_file(pov_path)
        flattened = flatten_parsed_file(parsed)

        csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
        os.close(csv_fd)
        write_csv(flattened, csv_path)

        with open(csv_path, "r") as fh:
            csv_content = fh.read()
        csv_content = csv_content.replace("XYZ789012", "TAMPERED!")
        with open(csv_path, "w") as fh:
            fh.write(csv_content)

        report = validate_flattened_csv(csv_path, parsed, flattened)

        rpt_fd, rpt_path = tempfile.mkstemp(suffix=".txt")
        os.close(rpt_fd)
        try:
            write_validation_report(report, rpt_path)
            with open(rpt_path, "r") as fh:
                content = fh.read()
            assert "FAIL" in content
            assert "MISMATCH" in content.upper()
        finally:
            os.unlink(pov_path)
            os.unlink(csv_path)
            os.unlink(rpt_path)
