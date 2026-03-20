"""
Loads the Withdrawal-Quote sample CSV (WD_quote_samples.csv) and
optionally copies it to the pipeline data directory.

This replaces the former synthetic-data generator; the CSV is
now the authoritative source of test rows.
"""

from __future__ import annotations

import csv
import os
import shutil


def load_csv_rows(csv_path: str) -> list[dict]:
    """
    Read all data rows from the Withdrawal-Quote CSV.

    Returns:
        List of OrderedDicts keyed by column header.
    """
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def prepare_csv(source_csv: str, dest_csv: str) -> str:
    """
    Copy the source CSV into the pipeline data directory.

    If *source_csv* and *dest_csv* point to the same file the copy is
    skipped.  Returns the destination path.
    """
    os.makedirs(os.path.dirname(dest_csv) or ".", exist_ok=True)
    src = os.path.realpath(source_csv)
    dst = os.path.realpath(dest_csv)
    if src != dst:
        shutil.copy2(source_csv, dest_csv)
    return dest_csv


def get_sample_rows(csv_path: str, include_invalid: bool = True) -> list[dict]:
    """
    Return sample row dicts from the CSV.

    If *include_invalid* is False, only rows with ExpectedToPass == '1' are
    returned.
    """
    rows = load_csv_rows(csv_path)
    if not include_invalid:
        rows = [r for r in rows if r.get("ExpectedToPass") == "1"]
    return rows
