"""Tests for dual-format POV file parsing (standard + extended)."""

from __future__ import annotations

import csv
import os
import tempfile
import shutil

import pytest

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.pov_record_layouts import (
    RECORD_LAYOUTS,
    RECORD_TYPE_DESCRIPTIONS,
    FORMAT_STANDARD,
    FORMAT_EXTENDED,
    STANDARD_LINE_WIDTH,
    EXTENDED_LINE_WIDTH,
    _EXTENSION_36,
    detect_file_format,
    detect_record_type,
    extract_valuation_date_from_filename,
    get_record_width,
)
from modules.pov_parser import parse_line, parse_file, ParsedFile, ParsedRecord
from modules.pov_flattener import flatten_parsed_file, write_csv


# ── Helpers ──────────────────────────────────────────────────────────

def _build_line(record_type: str, field_values: dict[str, str] | None = None,
                *, width: int = STANDARD_LINE_WIDTH) -> str:
    """Build a fixed-width line, padded to *width*."""
    layout = RECORD_LAYOUTS[record_type]
    parts: list[str] = []
    for field_name, w in layout:
        val = (field_values or {}).get(field_name, "")
        parts.append(val.ljust(w)[:w])
    line = "".join(parts)
    return line.ljust(width)[:width]


def _build_extension(carrier: str = "4512", fmt: str = "PFF",
                     contra: str = "0255", firm: str = "MML",
                     contract: str = "", seq: str = "01") -> str:
    """Build a 36-byte transmission extension block."""
    ref = contract.rjust(20)
    ext = f"{carrier}{fmt}{contra}{firm}{ref}{seq}"
    return ext.ljust(36)[:36]


def _build_extended_line(record_type: str,
                         field_values: dict[str, str] | None = None,
                         contract_num: str = "") -> str:
    """Build a 336-char extended-format line with extension block."""
    layout_width = get_record_width(record_type)
    if layout_width <= STANDARD_LINE_WIDTH:
        core = _build_line(record_type, field_values, width=STANDARD_LINE_WIDTH)
        ext = _build_extension(contract=contract_num,
                               seq=record_type[-2:] if len(record_type) == 4 else "00")
        return core + ext
    else:
        # Records wider than 300 fill up to 336 from the layout
        return _build_line(record_type, field_values, width=EXTENDED_LINE_WIDTH)


def _write_temp_file(lines: list[str], suffix: str = ".txt") -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w") as fh:
        fh.write("\n".join(lines))
    return path


# ── Standard format sample data ─────────────────────────────────────

STD_FIELDS_100 = {
    "Submitters_Code": "C", "Record_Type": "10",
    "Submitting_Participant_Number": "3418",
    "IPS_Business_Code": "POV",
    "Transmission_Unique_ID": "TX-20260320",
    "Valuation_Date": "20260320",
    "Test_Indicator": "T",
}

STD_FIELDS_120 = {
    "Submitters_Code": "C", "Record_Type": "12",
    "Contra_Participant_Number": "0015",
    "Associated_Firm_ID": "LIFE",
}

STD_FIELDS_1301 = {
    "Submitters_Code": "C", "Record_Type": "13", "Sequence_Number": "01",
    "Contract_Number": "TEST-001",
    "CUSIP_Number": "10922T829",
    "Contract_Status": "VA",
    "Distributors_Account_ID": "ACCT-001",
    "Product_Type_Code": "IVA",
    "Contract_State": "NY",
}

STD_FIELDS_1302 = {
    "Submitters_Code": "C", "Record_Type": "13", "Sequence_Number": "02",
    "Contract_Number": "TEST-001",
    "Contract_Value_Amount_1": "0000000050000000",
    "Contract_Value_Qualifier_1": "CR",
}

STD_FIELDS_1305 = {
    "Submitters_Code": "C", "Record_Type": "13", "Sequence_Number": "05",
    "Contract_Number": "TEST-001",
    "Agent_Role": "PA",
    "Agent_Non_Natural_Name": "SMITH",
}

STD_FIELDS_1309 = {
    "Submitters_Code": "C", "Record_Type": "13", "Sequence_Number": "09",
    "Contract_Number": "TEST-001",
    "Party_Last_Name": "DOE",
    "Party_First_Name": "JOHN",
    "Party_Role": "OK",
}


# ═══════════════════════════════════════════════════════════════════
#  Format Detection
# ═══════════════════════════════════════════════════════════════════

class TestDetectFileFormat:
    def test_standard_with_hdr_line(self):
        lines = [
            "HDR.S46027.E00.C3418.S341803202026POSITIONS AND VALUATIONS".ljust(300),
            _build_line("100", STD_FIELDS_100),
            _build_line("120", STD_FIELDS_120),
            _build_line("1301", STD_FIELDS_1301),
        ]
        path = _write_temp_file(lines)
        try:
            assert detect_file_format(path) == FORMAT_STANDARD
        finally:
            os.unlink(path)

    def test_standard_with_100_header(self):
        lines = [
            _build_line("100", STD_FIELDS_100),
            _build_line("1301", STD_FIELDS_1301),
        ]
        path = _write_temp_file(lines)
        try:
            assert detect_file_format(path) == FORMAT_STANDARD
        finally:
            os.unlink(path)

    def test_extended_336_chars(self):
        lines = [
            _build_extended_line("1301", STD_FIELDS_1301, "TEST-001"),
            _build_extended_line("1302", STD_FIELDS_1302, "TEST-001"),
        ]
        path = _write_temp_file(lines)
        try:
            assert detect_file_format(path) == FORMAT_EXTENDED
        finally:
            os.unlink(path)

    def test_missing_file_defaults_standard(self):
        assert detect_file_format("/nonexistent/file.txt") == FORMAT_STANDARD

    def test_empty_file_defaults_standard(self):
        path = _write_temp_file([""])
        try:
            assert detect_file_format(path) == FORMAT_STANDARD
        finally:
            os.unlink(path)

    def test_standard_300_char_detail_only(self):
        lines = [_build_line("1301", STD_FIELDS_1301, width=300)]
        path = _write_temp_file(lines)
        try:
            assert detect_file_format(path) == FORMAT_STANDARD
        finally:
            os.unlink(path)


# ═══════════════════════════════════════════════════════════════════
#  Valuation Date from Filename
# ═══════════════════════════════════════════════════════════════════

class TestExtractValuationDate:
    def test_yyyymmdd_pattern(self):
        assert extract_valuation_date_from_filename(
            "/data/E9914T.POV.20260320_0305.txt"
        ) == "20260320"

    def test_mmddyyyy_pattern(self):
        assert extract_valuation_date_from_filename(
            "/data/BHFT_DXC_DTCC_POV_03202026_0315.txt"
        ) == "20260320"

    def test_no_date_returns_empty(self):
        assert extract_valuation_date_from_filename("/data/nodate.txt") == ""

    def test_yyyymmdd_in_longer_name(self):
        assert extract_valuation_date_from_filename(
            "/data/E9914T.BHF.DXCA.DTCC.POV.P.DATA.20260320_0305.txt"
        ) == "20260320"


# ═══════════════════════════════════════════════════════════════════
#  Parsing Standard Format
# ═══════════════════════════════════════════════════════════════════

class TestParseStandardFormat:
    def _make_standard_file(self) -> str:
        lines = [
            "HDR.S46027.E00.C3418".ljust(300),
            _build_line("100", STD_FIELDS_100),
            _build_line("120", STD_FIELDS_120),
            _build_line("1301", STD_FIELDS_1301),
            _build_line("1302", STD_FIELDS_1302),
            _build_line("1305", STD_FIELDS_1305),
            _build_line("1309", STD_FIELDS_1309),
            "END.S46027.E00.C3418".ljust(300),
        ]
        return _write_temp_file(lines)

    def test_file_type_is_pov(self):
        path = self._make_standard_file()
        try:
            parsed = parse_file(path)
            assert parsed.file_type == "POV"
        finally:
            os.unlink(path)

    def test_format_is_standard(self):
        path = self._make_standard_file()
        try:
            parsed = parse_file(path)
            assert parsed.file_format == FORMAT_STANDARD
        finally:
            os.unlink(path)

    def test_valuation_date_from_header(self):
        path = self._make_standard_file()
        try:
            parsed = parse_file(path)
            assert parsed.valuation_date == "20260320"
        finally:
            os.unlink(path)

    def test_hdr_end_lines_skipped_no_errors(self):
        path = self._make_standard_file()
        try:
            parsed = parse_file(path)
            assert parsed.skipped_lines == 2  # HDR + END
            assert len(parsed.errors) == 0
        finally:
            os.unlink(path)

    def test_header_and_detail_counts(self):
        path = self._make_standard_file()
        try:
            parsed = parse_file(path)
            assert len(parsed.header_records) == 2  # 100 + 120
            assert len(parsed.detail_records) == 4  # 1301 + 1302 + 1305 + 1309
        finally:
            os.unlink(path)

    def test_contract_number_parsed(self):
        path = self._make_standard_file()
        try:
            parsed = parse_file(path)
            rec_1301 = [r for r in parsed.detail_records if r.record_type == "1301"]
            assert len(rec_1301) == 1
            assert rec_1301[0].fields["Contract_Number"] == "TEST-001"
        finally:
            os.unlink(path)


# ═══════════════════════════════════════════════════════════════════
#  Parsing Extended Format
# ═══════════════════════════════════════════════════════════════════

class TestParseExtendedFormat:
    def _make_extended_file(self, filename: str = "POV_20260320_data.txt") -> str:
        lines = [
            _build_extended_line("1301", STD_FIELDS_1301, "TEST-001"),
            _build_extended_line("1302", STD_FIELDS_1302, "TEST-001"),
            _build_extended_line("1305", STD_FIELDS_1305, "TEST-001"),
            _build_extended_line("1309", STD_FIELDS_1309, "TEST-001"),
        ]
        d = tempfile.mkdtemp()
        path = os.path.join(d, filename)
        with open(path, "w") as fh:
            fh.write("\n".join(lines))
        return path

    def test_format_is_extended(self):
        path = self._make_extended_file()
        try:
            parsed = parse_file(path)
            assert parsed.file_format == FORMAT_EXTENDED
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_file_type_inferred_pov(self):
        path = self._make_extended_file()
        try:
            parsed = parse_file(path)
            assert parsed.file_type == "POV"
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_valuation_date_from_filename(self):
        path = self._make_extended_file("E9914T.BHF.DXCA.DTCC.POV.P.DATA.20260320_0305.txt")
        try:
            parsed = parse_file(path)
            assert parsed.valuation_date == "20260320"
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_no_header_records(self):
        path = self._make_extended_file()
        try:
            parsed = parse_file(path)
            assert len(parsed.header_records) == 0
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_all_lines_parsed(self):
        path = self._make_extended_file()
        try:
            parsed = parse_file(path)
            assert parsed.skipped_lines == 0
            assert parsed.parsed_lines == 4
            assert len(parsed.errors) == 0
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_contract_number_parsed(self):
        path = self._make_extended_file()
        try:
            parsed = parse_file(path)
            rec_1301 = [r for r in parsed.detail_records if r.record_type == "1301"]
            assert len(rec_1301) == 1
            assert rec_1301[0].fields["Contract_Number"] == "TEST-001"
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_cusip_parsed(self):
        path = self._make_extended_file()
        try:
            parsed = parse_file(path)
            rec_1301 = [r for r in parsed.detail_records if r.record_type == "1301"][0]
            assert rec_1301.fields["CUSIP_Number"] == "10922T829"
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_extension_bytes_do_not_corrupt_core_fields(self):
        """Extension bytes at pos 300+ must not pollute core layout fields."""
        path = self._make_extended_file()
        try:
            parsed = parse_file(path)
            rec_1301 = [r for r in parsed.detail_records if r.record_type == "1301"][0]
            # Fields at known positions should have correct data
            assert rec_1301.fields["Contract_Status"] == "VA"
            assert rec_1301.fields["Product_Type_Code"] == "IVA"
            assert rec_1301.fields["Contract_State"] == "NY"
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_summary_dict_includes_format(self):
        path = self._make_extended_file()
        try:
            parsed = parse_file(path)
            summary = parsed.to_summary_dict()
            assert summary["file_format"] == FORMAT_EXTENDED
            assert summary["file_type"] == "POV"
        finally:
            shutil.rmtree(os.path.dirname(path))


# ═══════════════════════════════════════════════════════════════════
#  Cross-Format Parity
# ═══════════════════════════════════════════════════════════════════

class TestCrossFormatParity:
    """Ensure the same contract data produces consistent results in both formats."""

    def _make_both_formats(self):
        std_lines = [
            _build_line("100", STD_FIELDS_100),
            _build_line("120", STD_FIELDS_120),
            _build_line("1301", STD_FIELDS_1301),
            _build_line("1302", STD_FIELDS_1302),
        ]
        ext_lines = [
            _build_extended_line("1301", STD_FIELDS_1301, "TEST-001"),
            _build_extended_line("1302", STD_FIELDS_1302, "TEST-001"),
        ]
        std_path = _write_temp_file(std_lines)
        d = tempfile.mkdtemp()
        ext_path = os.path.join(d, "POV_20260320.txt")
        with open(ext_path, "w") as fh:
            fh.write("\n".join(ext_lines))
        return std_path, ext_path

    def test_same_contract_fields(self):
        std_path, ext_path = self._make_both_formats()
        try:
            std_parsed = parse_file(std_path)
            ext_parsed = parse_file(ext_path)

            std_1301 = [r for r in std_parsed.detail_records if r.record_type == "1301"][0]
            ext_1301 = [r for r in ext_parsed.detail_records if r.record_type == "1301"][0]

            # Core fields must match
            for field in ["Contract_Number", "CUSIP_Number", "Contract_Status",
                          "Product_Type_Code", "Contract_State"]:
                assert std_1301.fields[field] == ext_1301.fields[field], \
                    f"Field {field} mismatch: std={std_1301.fields[field]!r} ext={ext_1301.fields[field]!r}"
        finally:
            os.unlink(std_path)
            shutil.rmtree(os.path.dirname(ext_path))

    def test_same_flatten_contract_count(self):
        std_path, ext_path = self._make_both_formats()
        try:
            std_flat = flatten_parsed_file(parse_file(std_path))
            ext_flat = flatten_parsed_file(parse_file(ext_path))
            assert std_flat.contract_count == ext_flat.contract_count
        finally:
            os.unlink(std_path)
            shutil.rmtree(os.path.dirname(ext_path))

    def test_flattened_csv_round_trip_extended(self):
        d = tempfile.mkdtemp()
        try:
            ext_lines = [
                _build_extended_line("1301", STD_FIELDS_1301, "TEST-001"),
                _build_extended_line("1302", STD_FIELDS_1302, "TEST-001"),
            ]
            pov_path = os.path.join(d, "POV_20260320.txt")
            with open(pov_path, "w") as fh:
                fh.write("\n".join(ext_lines))

            parsed = parse_file(pov_path)
            flattened = flatten_parsed_file(parsed)
            csv_path = os.path.join(d, "out.csv")
            write_csv(flattened, csv_path)

            # Read back and verify
            with open(csv_path) as fh:
                reader = csv.DictReader(fh)
                rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["Contract_Number"] == "TEST-001"
            assert rows[0]["1301_CUSIP_Number"] == "10922T829"
        finally:
            shutil.rmtree(d)


# ═══════════════════════════════════════════════════════════════════
#  Multi-Contract Extended Format
# ═══════════════════════════════════════════════════════════════════

class TestMultiContractExtended:
    def _make_multi_contract_file(self):
        c1_1301 = dict(STD_FIELDS_1301, Contract_Number="MULTI-001")
        c1_1302 = dict(STD_FIELDS_1302, Contract_Number="MULTI-001")
        c2_1301 = dict(STD_FIELDS_1301, Contract_Number="MULTI-002",
                       CUSIP_Number="99999X111", Contract_State="CA")
        c2_1302 = dict(STD_FIELDS_1302, Contract_Number="MULTI-002")

        lines = [
            _build_extended_line("1301", c1_1301, "MULTI-001"),
            _build_extended_line("1302", c1_1302, "MULTI-001"),
            _build_extended_line("1301", c2_1301, "MULTI-002"),
            _build_extended_line("1302", c2_1302, "MULTI-002"),
        ]
        d = tempfile.mkdtemp()
        path = os.path.join(d, "MULTI_20260320.txt")
        with open(path, "w") as fh:
            fh.write("\n".join(lines))
        return path

    def test_two_contracts_parsed(self):
        path = self._make_multi_contract_file()
        try:
            parsed = parse_file(path)
            contracts = {r.fields["Contract_Number"]
                         for r in parsed.detail_records if r.record_type == "1301"}
            assert contracts == {"MULTI-001", "MULTI-002"}
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_two_contracts_flattened(self):
        path = self._make_multi_contract_file()
        try:
            parsed = parse_file(path)
            flattened = flatten_parsed_file(parsed)
            assert flattened.contract_count == 2
        finally:
            shutil.rmtree(os.path.dirname(path))

    def test_contract_states_distinct(self):
        path = self._make_multi_contract_file()
        try:
            parsed = parse_file(path)
            flattened = flatten_parsed_file(parsed)
            rows_by_cn = {r["Contract_Number"]: r for r in flattened.rows}
            assert rows_by_cn["MULTI-001"]["1301_Contract_State"] == "NY"
            assert rows_by_cn["MULTI-002"]["1301_Contract_State"] == "CA"
        finally:
            shutil.rmtree(os.path.dirname(path))


# ═══════════════════════════════════════════════════════════════════
#  Real File Integration (run only when files exist)
# ═══════════════════════════════════════════════════════════════════

_REAL_OLD = "/Workspace/Users/edward.m.ruiz@brighthousefinancial.com/XMLGenerator/run_output/data/POV/BHFT_DXC_DTCC_POV_03202026_0315.txt"
_REAL_NEW = "/Workspace/Users/edward.m.ruiz@brighthousefinancial.com/XMLGenerator/run_output/data/POV/E9914T.BHF.DXCA.DTCC.POV.P.DATA.20260320_0305.txt"


@pytest.mark.skipif(not os.path.isfile(_REAL_OLD), reason="real old POV not present")
class TestRealStandardFile:
    def test_format_detected(self):
        assert detect_file_format(_REAL_OLD) == FORMAT_STANDARD

    def test_parses_without_errors(self):
        parsed = parse_file(_REAL_OLD)
        assert len(parsed.errors) == 0

    def test_has_headers(self):
        parsed = parse_file(_REAL_OLD)
        assert len(parsed.header_records) > 0

    def test_file_type_pov(self):
        parsed = parse_file(_REAL_OLD)
        assert parsed.file_type == "POV"


@pytest.mark.skipif(not os.path.isfile(_REAL_NEW), reason="real new POV not present")
class TestRealExtendedFile:
    def test_format_detected(self):
        assert detect_file_format(_REAL_NEW) == FORMAT_EXTENDED

    def test_parses_without_errors(self):
        parsed = parse_file(_REAL_NEW)
        assert len(parsed.errors) == 0

    def test_no_headers(self):
        parsed = parse_file(_REAL_NEW)
        assert len(parsed.header_records) == 0

    def test_file_type_pov(self):
        parsed = parse_file(_REAL_NEW)
        assert parsed.file_type == "POV"

    def test_valuation_date_extracted(self):
        parsed = parse_file(_REAL_NEW)
        assert parsed.valuation_date == "20260320"

    def test_detail_count_matches_line_count(self):
        parsed = parse_file(_REAL_NEW)
        assert len(parsed.detail_records) == parsed.total_lines

    def test_flatten_produces_contracts(self):
        parsed = parse_file(_REAL_NEW)
        flattened = flatten_parsed_file(parsed)
        assert flattened.contract_count > 0
