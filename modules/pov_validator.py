"""
Validator for DTCC POV flattened CSV output.

Reads the CSV produced by ``pov_flattener.write_csv`` and cross-checks
it against the original ``ParsedFile`` to ensure data integrity.

Validation checks
-----------------
1. **Contract count** – CSV row count matches the number of unique
   contracts in the parsed file.
2. **Field fidelity** – every non-filler field value in the original
   parsed records appears in the corresponding CSV column/row.
3. **No data loss** – all record types present for a contract in the
   original are represented in the CSV row.
4. **Structural integrity** – CSV header matches the expected flattened
   column set.

Public API
----------
- ``validate_flattened_csv``  – run all checks, return ``ValidationReport``
- ``ValidationReport``        – dataclass with pass/fail and details
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field
from typing import Any

from config.pov_record_layouts import (
    REPEATING_RECORD_TYPES,
    get_field_names,
)
from modules.pov_parser import ParsedFile
from modules.pov_flattener import FlattenedResult, _make_column_name


@dataclass
class FieldMismatch:
    """Details of a single field-level mismatch."""
    contract_number: str
    record_type: str
    field_name: str
    expected: str
    actual: str
    occurrence: int | None = None


@dataclass
class ValidationReport:
    """Aggregate validation result."""
    valid: bool
    csv_path: str
    expected_contract_count: int
    actual_contract_count: int
    total_fields_checked: int
    mismatches: list[FieldMismatch] = field(default_factory=list)
    missing_record_types: list[dict[str, str]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def mismatch_count(self) -> int:
        return len(self.mismatches)

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "csv_path": self.csv_path,
            "expected_contracts": self.expected_contract_count,
            "actual_contracts": self.actual_contract_count,
            "total_fields_checked": self.total_fields_checked,
            "mismatch_count": self.mismatch_count,
            "missing_record_type_count": len(self.missing_record_types),
            "error_count": len(self.errors),
        }


def validate_flattened_csv(
    csv_path: str,
    parsed: ParsedFile,
    flattened: FlattenedResult,
) -> ValidationReport:
    """
    Validate a flattened CSV against the original parsed data.

    Returns a ``ValidationReport`` with detailed mismatch information.
    """
    report = ValidationReport(
        valid=True,
        csv_path=csv_path,
        expected_contract_count=flattened.contract_count,
        actual_contract_count=0,
        total_fields_checked=0,
    )

    if not os.path.isfile(csv_path):
        report.valid = False
        report.errors.append(f"CSV file not found: {csv_path}")
        return report

    # ── Read CSV into a dict keyed by Contract_Number ────────────────
    csv_rows: dict[str, dict[str, str]] = {}
    with open(csv_path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        csv_header = list(reader.fieldnames or [])
        for row in reader:
            cn = row.get("Contract_Number", "").strip()
            csv_rows[cn] = row

    report.actual_contract_count = len(csv_rows)

    # ── Check 1: contract count ──────────────────────────────────────
    if report.actual_contract_count != report.expected_contract_count:
        report.valid = False
        report.errors.append(
            f"Contract count mismatch: expected {report.expected_contract_count}, "
            f"got {report.actual_contract_count}"
        )

    # ── Check 2: header matches expected columns ─────────────────────
    expected_header = set(flattened.header)
    actual_header = set(csv_header)
    if expected_header != actual_header:
        missing_cols = expected_header - actual_header
        extra_cols = actual_header - expected_header
        if missing_cols:
            report.valid = False
            report.errors.append(f"Missing CSV columns: {sorted(missing_cols)}")
        if extra_cols:
            report.errors.append(f"Extra CSV columns: {sorted(extra_cols)}")

    # ── Check 3: field-level fidelity ────────────────────────────────
    # Group original parsed records by contract
    from collections import OrderedDict
    contract_records: OrderedDict[str, list] = OrderedDict()
    for rec in parsed.detail_records:
        cn = rec.fields.get("Contract_Number", "").strip() or "__NO_CONTRACT__"
        contract_records.setdefault(cn, []).append(rec)

    skip_fields = {"Submitters_Code", "System_Code", "Record_Type",
                   "Sequence_Number", "Contract_Number"}

    rt_max = flattened.record_type_max_occurrences

    for contract_num, records in contract_records.items():
        csv_row = csv_rows.get(contract_num)
        if csv_row is None:
            report.valid = False
            report.errors.append(
                f"Contract '{contract_num}' from parsed data not found in CSV"
            )
            continue

        # Track record type occurrences for this contract
        rt_occ_counter: dict[str, int] = {}

        for rec in records:
            rt = rec.record_type
            rt_occ_counter[rt] = rt_occ_counter.get(rt, 0) + 1
            occ = rt_occ_counter[rt]

            data_fields = [f for f in get_field_names(rt, include_filler=False)
                           if f not in skip_fields]

            for fn in data_fields:
                expected_val = rec.fields.get(fn, "").strip()
                if rt in REPEATING_RECORD_TYPES and rt_max.get(rt, 1) > 1:
                    col = _make_column_name(rt, fn, occ)
                else:
                    col = _make_column_name(rt, fn)

                actual_val = csv_row.get(col, "").strip()
                report.total_fields_checked += 1

                if expected_val != actual_val:
                    report.valid = False
                    report.mismatches.append(FieldMismatch(
                        contract_number=contract_num,
                        record_type=rt,
                        field_name=fn,
                        expected=expected_val,
                        actual=actual_val,
                        occurrence=occ if rt in REPEATING_RECORD_TYPES else None,
                    ))

    # ── Check 4: record type coverage ────────────────────────────────
    for contract_num, records in contract_records.items():
        present_rts = {rec.record_type for rec in records}
        csv_row = csv_rows.get(contract_num, {})

        for rt in present_rts:
            data_fields = [f for f in get_field_names(rt, include_filler=False)
                           if f not in skip_fields]
            if not data_fields:
                continue

            sample_col = _make_column_name(
                rt, data_fields[0],
                1 if (rt in REPEATING_RECORD_TYPES and rt_max.get(rt, 1) > 1) else None,
            )
            if sample_col not in csv_row:
                report.missing_record_types.append({
                    "contract_number": contract_num,
                    "record_type": rt,
                })

    return report


def write_validation_report(report: ValidationReport, output_path: str) -> str:
    """Write a human-readable validation report to a text file."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    lines = [
        "DTCC POV Flattening Validation Report",
        "=" * 50,
        f"CSV File:           {report.csv_path}",
        f"Overall Result:     {'PASS' if report.valid else 'FAIL'}",
        f"Expected Contracts: {report.expected_contract_count}",
        f"Actual Contracts:   {report.actual_contract_count}",
        f"Fields Checked:     {report.total_fields_checked}",
        f"Mismatches:         {report.mismatch_count}",
        "",
    ]

    if report.errors:
        lines.append("ERRORS:")
        for err in report.errors:
            lines.append(f"  - {err}")
        lines.append("")

    if report.mismatches:
        lines.append("FIELD MISMATCHES (first 50):")
        for mm in report.mismatches[:50]:
            occ_str = f" (occ {mm.occurrence})" if mm.occurrence else ""
            lines.append(
                f"  Contract={mm.contract_number}  RT={mm.record_type}{occ_str}  "
                f"Field={mm.field_name}  Expected={mm.expected!r}  Actual={mm.actual!r}"
            )
        if len(report.mismatches) > 50:
            lines.append(f"  ... and {len(report.mismatches) - 50} more")
        lines.append("")

    if report.missing_record_types:
        lines.append("MISSING RECORD TYPES:")
        for item in report.missing_record_types:
            lines.append(
                f"  Contract={item['contract_number']}  RT={item['record_type']}"
            )
        lines.append("")

    content = "\n".join(lines)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(content)

    return output_path
