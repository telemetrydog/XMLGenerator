"""
Flattener for parsed DTCC POV/FAR records.

Takes the hierarchical, multi-record-per-contract structure produced by
``pov_parser.parse_file`` and pivots it into one wide row per contract,
suitable for CSV export.

Repeating record types (e.g. multiple 1303 Fund Detail records for one
contract) are handled by appending an occurrence index suffix to each
column name (e.g. ``1303_Fund_Value_1``, ``1303_Fund_Value_2``).

Public API
----------
- ``flatten_parsed_file``  – flatten a ``ParsedFile`` into rows + header
- ``write_csv``            – write flattened rows to a CSV file
- ``FlattenedResult``      – dataclass wrapping the output
"""

from __future__ import annotations

import csv
import os
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from config.pov_record_layouts import (
    REPEATING_RECORD_TYPES,
    RECORD_TYPE_DESCRIPTIONS,
    get_field_names,
)
from modules.pov_parser import ParsedFile, ParsedRecord


@dataclass
class FlattenedResult:
    """Result of flattening a parsed POV/FAR file."""
    header: list[str]
    rows: list[dict[str, str]]
    contract_count: int
    record_type_max_occurrences: dict[str, int] = field(default_factory=dict)
    source_filepath: str = ""

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "source_filepath": self.source_filepath,
            "contract_count": self.contract_count,
            "column_count": len(self.header),
            "record_type_max_occurrences": self.record_type_max_occurrences,
        }


def _group_by_contract(records: list[ParsedRecord]) -> OrderedDict[str, list[ParsedRecord]]:
    """Group detail records by Contract_Number, preserving insertion order."""
    grouped: OrderedDict[str, list[ParsedRecord]] = OrderedDict()
    for rec in records:
        contract_num = rec.fields.get("Contract_Number", "").strip()
        if not contract_num:
            contract_num = "__NO_CONTRACT__"
        grouped.setdefault(contract_num, []).append(rec)
    return grouped


def _make_column_name(record_type: str, field_name: str,
                      occurrence: int | None = None) -> str:
    """
    Build a flattened column name.

    Format: ``{record_type}_{field_name}`` for singular records, or
    ``{record_type}_{field_name}_{occurrence}`` for repeating records.
    """
    base = f"{record_type}_{field_name}"
    if occurrence is not None:
        return f"{base}_{occurrence}"
    return base


def flatten_parsed_file(parsed: ParsedFile) -> FlattenedResult:
    """
    Flatten a ``ParsedFile`` into a list of wide row dicts, one per contract.

    Steps:
        1. Group detail records by contract number.
        2. For each contract, count occurrences of each record type.
        3. Build a global header (union of all columns across contracts).
        4. Fill each contract's row dict, using occurrence suffixes for
           repeating record types.
    """
    grouped = _group_by_contract(parsed.detail_records)

    # ── Pass 1: determine max occurrences per record type ─────────────
    rt_max: dict[str, int] = {}
    per_contract_rt_counts: dict[str, dict[str, int]] = {}

    for contract_num, records in grouped.items():
        counts: dict[str, int] = {}
        for rec in records:
            counts[rec.record_type] = counts.get(rec.record_type, 0) + 1
        per_contract_rt_counts[contract_num] = counts
        for rt, cnt in counts.items():
            rt_max[rt] = max(rt_max.get(rt, 0), cnt)

    # ── Pass 2: build ordered header ─────────────────────────────────
    all_rt_sorted = sorted(rt_max.keys())
    header: list[str] = ["Contract_Number"]

    for rt in all_rt_sorted:
        field_names = get_field_names(rt, include_filler=False)
        skip = {"Submitters_Code", "System_Code", "Record_Type",
                "Sequence_Number", "Contract_Number"}
        data_fields = [f for f in field_names if f not in skip]

        max_occ = rt_max[rt]
        if rt in REPEATING_RECORD_TYPES and max_occ > 1:
            for occ in range(1, max_occ + 1):
                for fn in data_fields:
                    header.append(_make_column_name(rt, fn, occ))
        else:
            for fn in data_fields:
                header.append(_make_column_name(rt, fn))

    # ── Pass 3: build rows ───────────────────────────────────────────
    rows: list[dict[str, str]] = []

    for contract_num, records in grouped.items():
        row: dict[str, str] = {col: "" for col in header}
        row["Contract_Number"] = contract_num

        rt_occ_counter: dict[str, int] = {}

        for rec in records:
            rt = rec.record_type
            rt_occ_counter[rt] = rt_occ_counter.get(rt, 0) + 1
            occ = rt_occ_counter[rt]

            field_names = get_field_names(rt, include_filler=False)
            skip = {"Submitters_Code", "System_Code", "Record_Type",
                    "Sequence_Number", "Contract_Number"}
            data_fields = [f for f in field_names if f not in skip]

            for fn in data_fields:
                value = rec.fields.get(fn, "")
                if rt in REPEATING_RECORD_TYPES and rt_max[rt] > 1:
                    col = _make_column_name(rt, fn, occ)
                else:
                    col = _make_column_name(rt, fn)
                if col in row:
                    row[col] = value

        rows.append(row)

    return FlattenedResult(
        header=header,
        rows=rows,
        contract_count=len(rows),
        record_type_max_occurrences=rt_max,
        source_filepath=parsed.filepath,
    )


def write_csv(result: FlattenedResult, output_path: str) -> str:
    """Write the flattened result to a CSV file. Returns the output path."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=result.header)
        writer.writeheader()
        writer.writerows(result.rows)

    return output_path
