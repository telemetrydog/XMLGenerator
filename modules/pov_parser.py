"""
Fixed-width parser for DTCC POV (Positions & Valuations) and
FAR (Financial Activity Reporting) files.

Reads a text file line-by-line, detects each line's record type, and
extracts fields according to the byte-width layout defined in
``config.pov_record_layouts``.

Supports two file formats:
    - **Standard**: original format with HDR/END text lines, 100/120
      header records, and 300-character lines.
    - **Extended**: production format with no header/trailer lines,
      336-character padded lines, and a 36-byte transmission extension
      on records whose layout is ≤ 300 characters.

Format is auto-detected.  The public API is unchanged.

Public API
----------
- ``parse_line``      – parse a single fixed-width line
- ``parse_file``      – parse an entire POV/FAR file (auto-detects format)
- ``ParsedFile``      – dataclass holding the full parse result
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from config.pov_record_layouts import (
    RECORD_LAYOUTS,
    RECORD_TYPE_DESCRIPTIONS,
    POV_HEADER_TYPES,
    POV_DETAIL_TYPES,
    FAR_HEADER_TYPES,
    FORMAT_STANDARD,
    FORMAT_EXTENDED,
    detect_file_format,
    detect_record_type,
    extract_valuation_date_from_filename,
)


@dataclass
class ParsedRecord:
    """A single parsed fixed-width line."""
    line_number: int
    record_type: str
    record_description: str
    fields: dict[str, str]
    raw_line: str


@dataclass
class ParsedFile:
    """Result of parsing an entire POV/FAR file."""
    filepath: str
    total_lines: int
    parsed_lines: int
    skipped_lines: int
    header_records: list[ParsedRecord] = field(default_factory=list)
    detail_records: list[ParsedRecord] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    file_type: str = ""
    valuation_date: str = ""
    file_format: str = FORMAT_STANDARD

    @property
    def record_type_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for rec in self.header_records + self.detail_records:
            counts[rec.record_type] = counts.get(rec.record_type, 0) + 1
        return counts

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "filepath": self.filepath,
            "file_type": self.file_type,
            "file_format": self.file_format,
            "valuation_date": self.valuation_date,
            "total_lines": self.total_lines,
            "parsed_lines": self.parsed_lines,
            "skipped_lines": self.skipped_lines,
            "error_count": len(self.errors),
            "record_type_counts": self.record_type_counts,
        }


def parse_line(line: str, line_number: int = 0) -> ParsedRecord | None:
    """
    Parse a single fixed-width line into a ``ParsedRecord``.

    Returns ``None`` if the record type cannot be detected or is unknown.
    """
    record_type = detect_record_type(line)
    if record_type is None:
        return None

    layout = RECORD_LAYOUTS[record_type]
    fields: dict[str, str] = {}
    pos = 0

    for field_name, width in layout:
        raw_value = line[pos:pos + width] if pos + width <= len(line) else line[pos:]
        fields[field_name] = raw_value.strip()
        pos += width

    return ParsedRecord(
        line_number=line_number,
        record_type=record_type,
        record_description=RECORD_TYPE_DESCRIPTIONS.get(record_type, "Unknown"),
        fields=fields,
        raw_line=line,
    )


def parse_file(filepath: str) -> ParsedFile:
    """
    Parse an entire DTCC POV or FAR fixed-width file.

    The file format (standard vs extended) is auto-detected:

    - **Standard** files have an ``HDR`` text line, ``100``/``120`` header
      records, ``300``-character lines, and an ``END`` trailer line.
    - **Extended** files contain only detail records (``13xx``), every
      line is ``336`` characters, and there are no header or trailer lines.

    For extended files the ``file_type`` is inferred from the presence of
    detail record types, and ``valuation_date`` is extracted from the
    filename when possible.
    """
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"POV file not found: {filepath}")

    file_format = detect_file_format(filepath)

    result = ParsedFile(filepath=filepath, total_lines=0, parsed_lines=0,
                        skipped_lines=0, file_format=file_format)

    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        for line_num, raw_line in enumerate(fh, start=1):
            line = raw_line.rstrip("\n\r")
            result.total_lines += 1

            if not line.strip():
                result.skipped_lines += 1
                continue

            record = parse_line(line, line_number=line_num)
            if record is None:
                result.skipped_lines += 1
                # In standard format, HDR/END lines are expected non-records
                if file_format == FORMAT_STANDARD:
                    if not (line.startswith("HDR") or line.startswith("END")):
                        result.errors.append(
                            f"Line {line_num}: unrecognised record type "
                            f"(first 5 chars: {line[:5]!r})"
                        )
                else:
                    result.errors.append(
                        f"Line {line_num}: unrecognised record type "
                        f"(first 5 chars: {line[:5]!r})"
                    )
                continue

            result.parsed_lines += 1

            if record.record_type in POV_HEADER_TYPES | FAR_HEADER_TYPES:
                result.header_records.append(record)
                if record.record_type == "100":
                    result.file_type = "POV"
                    result.valuation_date = record.fields.get("Valuation_Date", "")
                elif record.record_type == "400":
                    result.file_type = "FAR"
            else:
                result.detail_records.append(record)

    # ── Extended format: infer metadata from content / filename ───────
    if file_format == FORMAT_EXTENDED:
        if not result.file_type:
            has_pov = any(r.record_type in POV_DETAIL_TYPES
                         for r in result.detail_records[:50])
            has_far = any(r.record_type.startswith("43")
                         for r in result.detail_records[:50])
            if has_pov:
                result.file_type = "POV"
            elif has_far:
                result.file_type = "FAR"

        if not result.valuation_date:
            result.valuation_date = extract_valuation_date_from_filename(filepath)

    return result
