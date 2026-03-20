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
