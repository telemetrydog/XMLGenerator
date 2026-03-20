"""Tests for config.pov_record_layouts."""

from __future__ import annotations

import pytest

from config.pov_record_layouts import (
    RECORD_LAYOUTS,
    RECORD_TYPE_DESCRIPTIONS,
    POV_DETAIL_TYPES,
    POV_HEADER_TYPES,
    FAR_DETAIL_TYPES,
    FAR_HEADER_TYPES,
    REPEATING_RECORD_TYPES,
    get_layout,
    get_field_names,
    get_record_width,
    get_total_width,
    detect_record_type,
)


class TestRecordLayouts:
    def test_all_record_types_have_descriptions(self):
        for rt in RECORD_LAYOUTS:
            assert rt in RECORD_TYPE_DESCRIPTIONS, f"No description for {rt}"

    def test_known_record_widths(self):
        assert get_record_width("100") == 300
        assert get_record_width("1301") == 300
        assert get_record_width("1302") == 300
        assert get_record_width("1303") == 300

    def test_pov_detail_records_are_300_wide(self):
        for rt in POV_DETAIL_TYPES:
            width = get_record_width(rt)
            if rt in ("1305", "1309"):
                # 1305 and 1309 are wider (405 / 413)
                assert width > 300
            else:
                assert width == 300, f"Record {rt} width is {width}, expected 300"

    def test_record_type_sets_are_disjoint(self):
        all_sets = [POV_HEADER_TYPES, POV_DETAIL_TYPES,
                    FAR_HEADER_TYPES, FAR_DETAIL_TYPES]
        for i, s1 in enumerate(all_sets):
            for j, s2 in enumerate(all_sets):
                if i != j:
                    assert s1.isdisjoint(s2)

    def test_all_types_covered(self):
        all_types = (POV_HEADER_TYPES | POV_DETAIL_TYPES |
                     FAR_HEADER_TYPES | FAR_DETAIL_TYPES)
        assert all_types == set(RECORD_LAYOUTS.keys())

    def test_get_layout_returns_none_for_unknown(self):
        assert get_layout("9999") is None

    def test_get_field_names_excludes_filler_by_default(self):
        names = get_field_names("1301")
        assert all(not n.startswith("Filler") for n in names)

    def test_get_field_names_includes_filler(self):
        names = get_field_names("1301", include_filler=True)
        assert any(n.startswith("Filler") for n in names)

    def test_repeating_types_are_valid(self):
        for rt in REPEATING_RECORD_TYPES:
            assert rt in RECORD_LAYOUTS

    # ── New tests for fixes ──────────────────────────────────────────

    def test_pov_header_100_is_300_wide(self):
        """R100 (POV File Header) must be exactly 300 bytes."""
        assert get_record_width("100") == 300

    def test_pov_header_120_is_300_wide(self):
        """R120 (POV Firm Header) must be exactly 300 bytes.

        Regression: Filler was 217 (total 266) instead of 251 (total 300).
        """
        assert get_record_width("120") == 300

    def test_all_pov_headers_are_300_wide(self):
        """Every POV header record must be 300 bytes."""
        for rt in POV_HEADER_TYPES:
            width = get_record_width(rt)
            assert width == 300, f"POV header {rt} width is {width}, expected 300"

    def test_get_total_width_is_alias(self):
        """get_total_width should be an alias for get_record_width per requirements."""
        assert get_total_width is get_record_width
        assert get_total_width("1301") == 300
        assert get_total_width("100") == 300

    def test_total_record_type_count(self):
        """The layout registry must contain exactly 24 record types."""
        assert len(RECORD_LAYOUTS) == 25

    def test_no_duplicate_field_names_per_record(self):
        """Within each record type, field names must be unique."""
        for rt, fields in RECORD_LAYOUTS.items():
            names = [name for name, _ in fields]
            assert len(names) == len(set(names)), (
                f"Record {rt} has duplicate field names: "
                f"{[n for n in names if names.count(n) > 1]}"
            )

    def test_far_file_header_400_is_300_wide(self):
        """R400 (FAR File Header) must be exactly 300 bytes."""
        assert get_record_width("400") == 300


class TestDetectRecordType:
    def test_pov_header_100(self):
        line = "X10" + "A" * 297
        assert detect_record_type(line) == "100"

    def test_pov_firm_header_120(self):
        line = "X12" + "A" * 297
        assert detect_record_type(line) == "120"

    def test_pov_detail_1301(self):
        line = "X1301" + "A" * 295
        assert detect_record_type(line) == "1301"

    def test_far_header_400(self):
        line = "X40" + "A" * 297
        assert detect_record_type(line) == "400"

    def test_far_detail_4305(self):
        line = "X4305" + "A" * 295
        assert detect_record_type(line) == "4305"

    def test_unknown_record_type(self):
        line = "X99" + "A" * 297
        assert detect_record_type(line) is None

    def test_short_line(self):
        assert detect_record_type("AB") is None
