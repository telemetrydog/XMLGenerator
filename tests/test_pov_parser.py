"""Tests for modules.pov_parser."""

from __future__ import annotations

import os
import tempfile

import pytest

from config.pov_record_layouts import get_record_width, RECORD_LAYOUTS
from modules.pov_parser import parse_line, parse_file, ParsedRecord


def _build_line(record_type: str, field_values: dict[str, str] | None = None) -> str:
    """Build a valid fixed-width line for the given record type."""
    layout = RECORD_LAYOUTS[record_type]
    total_width = sum(w for _, w in layout)
    parts: list[str] = []

    for field_name, width in layout:
        if field_values and field_name in field_values:
            val = field_values[field_name]
        else:
            val = ""
        parts.append(val.ljust(width)[:width])

    return "".join(parts)


def _build_pov_file_content(contracts: list[dict]) -> str:
    """
    Build a multi-line POV file with a header and detail records.

    Each item in ``contracts`` is a dict mapping record_type -> field_values.
    """
    lines: list[str] = []

    # File header (100)
    header = _build_line("100", {
        "Submitters_Code": "C",
        "Record_Type": "10",
        "Submitting_Participant_Number": "1234",
        "IPS_Business_Code": "POV",
        "Transmission_Unique_ID": "TX-20260319-001",
        "Total_Count": str(sum(
            sum(1 for rt in c if rt.startswith("13")) for c in contracts
        )),
        "Valuation_Date": "20260319",
        "Test_Indicator": "T",
    })
    lines.append(header)

    # Firm header (120)
    firm = _build_line("120", {
        "Submitters_Code": "C",
        "Record_Type": "12",
        "Contra_Participant_Number": "5678",
        "Associated_Firm_ID": "9012",
    })
    lines.append(firm)

    for contract in contracts:
        for rt, fv in contract.items():
            lines.append(_build_line(rt, fv))

    return "\n".join(lines)


# ── Sample contract data ─────────────────────────────────────────────
CONTRACT_1 = {
    "1301": {
        "Submitters_Code": "C",
        "Record_Type": "13",
        "Sequence_Number": "01",
        "Contract_Number": "ANN-2026-00001",
        "CUSIP_Number": "123456789",
        "Contract_Status": "01",
        "Product_Type_Code": "VA",
        "Contract_State": "NY",
    },
    "1302": {
        "Submitters_Code": "C",
        "Record_Type": "13",
        "Sequence_Number": "02",
        "Contract_Number": "ANN-2026-00001",
        "Contract_Value_Amount_1": "0000000150000.00",
        "Contract_Value_Qualifier_1": "CSV",
    },
}

CONTRACT_2 = {
    "1301": {
        "Submitters_Code": "C",
        "Record_Type": "13",
        "Sequence_Number": "01",
        "Contract_Number": "ANN-2026-00002",
        "CUSIP_Number": "987654321",
        "Contract_Status": "02",
        "Product_Type_Code": "FA",
        "Contract_State": "CA",
    },
}


class TestParseLine:
    def test_parse_valid_1301(self):
        line = _build_line("1301", CONTRACT_1["1301"])
        rec = parse_line(line)
        assert rec is not None
        assert rec.record_type == "1301"
        assert rec.fields["Contract_Number"].strip() == "ANN-2026-00001"
        assert rec.fields["CUSIP_Number"].strip() == "123456789"

    def test_parse_valid_1302(self):
        line = _build_line("1302", CONTRACT_1["1302"])
        rec = parse_line(line)
        assert rec is not None
        assert rec.record_type == "1302"
        assert "150000" in rec.fields["Contract_Value_Amount_1"]

    def test_parse_header_100(self):
        line = _build_line("100", {
            "Submitters_Code": "C",
            "Record_Type": "10",
            "Valuation_Date": "20260319",
        })
        rec = parse_line(line)
        assert rec is not None
        assert rec.record_type == "100"
        assert rec.fields["Valuation_Date"] == "20260319"

    def test_parse_unknown_returns_none(self):
        line = "X99" + "A" * 300
        assert parse_line(line) is None

    def test_line_number_preserved(self):
        line = _build_line("1301", CONTRACT_1["1301"])
        rec = parse_line(line, line_number=42)
        assert rec is not None
        assert rec.line_number == 42


class TestParseFile:
    def _write_temp_file(self, content: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".txt")
        with os.fdopen(fd, "w") as fh:
            fh.write(content)
        return path

    def test_parse_two_contracts(self):
        content = _build_pov_file_content([CONTRACT_1, CONTRACT_2])
        path = self._write_temp_file(content)
        try:
            result = parse_file(path)
            assert result.file_type == "POV"
            assert result.valuation_date == "20260319"
            assert len(result.header_records) == 2  # 100 + 120
            assert len(result.detail_records) == 3  # 1301+1302 + 1301
            assert result.parsed_lines == 5
            assert result.skipped_lines == 0
        finally:
            os.unlink(path)

    def test_contract_numbers_in_details(self):
        content = _build_pov_file_content([CONTRACT_1, CONTRACT_2])
        path = self._write_temp_file(content)
        try:
            result = parse_file(path)
            contract_nums = {
                r.fields["Contract_Number"]
                for r in result.detail_records
            }
            assert "ANN-2026-00001" in contract_nums
            assert "ANN-2026-00002" in contract_nums
        finally:
            os.unlink(path)

    def test_empty_lines_skipped(self):
        content = _build_pov_file_content([CONTRACT_1])
        content = content + "\n\n\n"
        path = self._write_temp_file(content)
        try:
            result = parse_file(path)
            # "\n\n\n" at end produces 2 blank lines (last \n terminates line 2)
            assert result.skipped_lines == 2
        finally:
            os.unlink(path)

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            parse_file("/nonexistent/path/file.txt")

    def test_record_type_counts(self):
        content = _build_pov_file_content([CONTRACT_1, CONTRACT_2])
        path = self._write_temp_file(content)
        try:
            result = parse_file(path)
            counts = result.record_type_counts
            assert counts["100"] == 1
            assert counts["120"] == 1
            assert counts["1301"] == 2
            assert counts["1302"] == 1
        finally:
            os.unlink(path)

    def test_to_summary_dict(self):
        content = _build_pov_file_content([CONTRACT_1])
        path = self._write_temp_file(content)
        try:
            result = parse_file(path)
            summary = result.to_summary_dict()
            assert summary["file_type"] == "POV"
            assert summary["total_lines"] > 0
            assert "record_type_counts" in summary
        finally:
            os.unlink(path)
