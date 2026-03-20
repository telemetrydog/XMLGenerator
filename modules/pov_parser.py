"""
Fixed-width parser for DTCC POV (Positions & Valuations) and
FAR (Financial Activity Reporting) files.

Reads a text file line-by-line, detects each line's record type, and
extracts fields according to the byte-width layout defined in
``config.pov_record_layouts``.

Public API
----------
- ``parse_line``      – parse a single fixed-width line
- ``parse_file``      – parse an entire POV/FAR file
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
    FAR_HEADER_TYPES,
    detect_record_type,
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

    Each line is classified by record type and parsed into structured
    field dictionaries.  Header records (100/120 for POV, 400/420 for
    FAR) are separated from detail records.
    """
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"POV file not found: {filepath}")

    result = ParsedFile(filepath=filepath, total_lines=0, parsed_lines=0,
                        skipped_lines=0)

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

    return result
