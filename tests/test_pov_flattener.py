"""Tests for modules.pov_flattener."""

from __future__ import annotations

import csv
import os
import tempfile

import pytest

from config.pov_record_layouts import RECORD_LAYOUTS
from modules.pov_parser import parse_file
from modules.pov_flattener import flatten_parsed_file, write_csv


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


def _write_temp_file(content: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".txt")
    with os.fdopen(fd, "w") as fh:
        fh.write(content)
    return path


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


# ── Test data ────────────────────────────────────────────────────────
SINGLE_CONTRACT = {
    "1301": {
        "Submitters_Code": "C", "Record_Type": "13",
        "Sequence_Number": "01",
        "Contract_Number": "POL-001",
        "CUSIP_Number": "ABC123456",
        "Contract_Status": "01",
        "Product_Type_Code": "VA",
    },
    "1302": {
        "Submitters_Code": "C", "Record_Type": "13",
        "Sequence_Number": "02",
        "Contract_Number": "POL-001",
        "Contract_Value_Amount_1": "0000000100000.00",
        "Contract_Value_Qualifier_1": "CSV",
    },
}

MULTI_FUND_CONTRACT = {
    "1301": {
        "Submitters_Code": "C", "Record_Type": "13",
        "Sequence_Number": "01",
        "Contract_Number": "POL-002",
        "Contract_Status": "01",
    },
    "1303": [
        {
            "Submitters_Code": "C", "Record_Type": "13",
            "Sequence_Number": "03",
            "Contract_Number": "POL-002",
            "CUSIP_Fund_ID_Sub_Fund_ID": "FUND-A",
            "Fund_Value": "0000000050000.00",
            "Fund_Percentage": "0050.00000",
        },
        {
            "Submitters_Code": "C", "Record_Type": "13",
            "Sequence_Number": "03",
            "Contract_Number": "POL-002",
            "CUSIP_Fund_ID_Sub_Fund_ID": "FUND-B",
            "Fund_Value": "0000000050000.00",
            "Fund_Percentage": "0050.00000",
        },
    ],
}


class TestFlattenParsedFile:
    def test_single_contract(self):
        content = _build_pov_content([SINGLE_CONTRACT])
        path = _write_temp_file(content)
        try:
            parsed = parse_file(path)
            result = flatten_parsed_file(parsed)
            assert result.contract_count == 1
            assert "Contract_Number" in result.header
            assert result.rows[0]["Contract_Number"] == "POL-001"
            assert "1301_CUSIP_Number" in result.header
            assert result.rows[0]["1301_CUSIP_Number"] == "ABC123456"
        finally:
            os.unlink(path)

    def test_two_contracts(self):
        content = _build_pov_content([SINGLE_CONTRACT, {
            "1301": {
                "Submitters_Code": "C", "Record_Type": "13",
                "Sequence_Number": "01",
                "Contract_Number": "POL-003",
                "Contract_Status": "02",
            }
        }])
        path = _write_temp_file(content)
        try:
            parsed = parse_file(path)
            result = flatten_parsed_file(parsed)
            assert result.contract_count == 2
            nums = {r["Contract_Number"] for r in result.rows}
            assert nums == {"POL-001", "POL-003"}
        finally:
            os.unlink(path)

    def test_repeating_records_get_occurrence_suffix(self):
        content = _build_pov_content([MULTI_FUND_CONTRACT])
        path = _write_temp_file(content)
        try:
            parsed = parse_file(path)
            result = flatten_parsed_file(parsed)
            assert result.contract_count == 1

            assert "1303_CUSIP_Fund_ID_Sub_Fund_ID_1" in result.header
            assert "1303_CUSIP_Fund_ID_Sub_Fund_ID_2" in result.header
            assert result.rows[0]["1303_CUSIP_Fund_ID_Sub_Fund_ID_1"] == "FUND-A"
            assert result.rows[0]["1303_CUSIP_Fund_ID_Sub_Fund_ID_2"] == "FUND-B"
        finally:
            os.unlink(path)

    def test_max_occurrences_tracked(self):
        content = _build_pov_content([MULTI_FUND_CONTRACT])
        path = _write_temp_file(content)
        try:
            parsed = parse_file(path)
            result = flatten_parsed_file(parsed)
            assert result.record_type_max_occurrences.get("1303") == 2
        finally:
            os.unlink(path)

    def test_header_excludes_filler(self):
        content = _build_pov_content([SINGLE_CONTRACT])
        path = _write_temp_file(content)
        try:
            parsed = parse_file(path)
            result = flatten_parsed_file(parsed)
            filler_cols = [c for c in result.header if "Filler" in c]
            assert len(filler_cols) == 0
        finally:
            os.unlink(path)


class TestWriteCsv:
    def test_csv_written_correctly(self):
        content = _build_pov_content([SINGLE_CONTRACT])
        path = _write_temp_file(content)
        try:
            parsed = parse_file(path)
            result = flatten_parsed_file(parsed)

            csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
            os.close(csv_fd)
            try:
                write_csv(result, csv_path)

                with open(csv_path, "r") as fh:
                    reader = csv.DictReader(fh)
                    rows = list(reader)

                assert len(rows) == 1
                assert rows[0]["Contract_Number"] == "POL-001"
                assert rows[0]["1301_CUSIP_Number"] == "ABC123456"
            finally:
                os.unlink(csv_path)
        finally:
            os.unlink(path)

    def test_csv_has_all_columns(self):
        content = _build_pov_content([SINGLE_CONTRACT])
        path = _write_temp_file(content)
        try:
            parsed = parse_file(path)
            result = flatten_parsed_file(parsed)

            csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
            os.close(csv_fd)
            try:
                write_csv(result, csv_path)

                with open(csv_path, "r") as fh:
                    reader = csv.DictReader(fh)
                    csv_header = list(reader.fieldnames or [])

                assert csv_header == result.header
            finally:
                os.unlink(csv_path)
        finally:
            os.unlink(path)
