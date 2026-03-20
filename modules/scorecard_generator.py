"""
Generates validation scorecards and sorts XML files into
success/ and unsuccessful/ folders based on validation or analysis results.

Supports two modes:
  - Basic: from ValidationResult (original pipeline)
  - Enhanced: from AnalysisResult (external XML analysis with schema diffs)
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from modules.xml_validator import ValidationResult


SCORECARD_SCHEMA = StructType([
    StructField("PolNumber", StringType(), False),
    StructField("FileName", StringType(), False),
    StructField("Status", StringType(), False),
    StructField("ErrorDetails", StringType(), True),
    StructField("Timestamp", StringType(), False),
])

ENHANCED_SCORECARD_SCHEMA = StructType([
    StructField("PolNumber", StringType(), False),
    StructField("FileName", StringType(), False),
    StructField("Status", StringType(), False),
    StructField("ErrorDetails", StringType(), True),
    StructField("MatchedFields", StringType(), True),
    StructField("MatchedCount", StringType(), True),
    StructField("MissingFields", StringType(), True),
    StructField("MissingCount", StringType(), True),
    StructField("CustomFields", StringType(), True),
    StructField("CustomCount", StringType(), True),
    StructField("ConformancePct", FloatType(), True),
    StructField("Timestamp", StringType(), False),
])


def generate_scorecard(
    spark: SparkSession,
    validation_results: list[ValidationResult],
    xml_results: list[dict],
) -> DataFrame:
    """
    Create a basic PySpark DataFrame scorecard from validation results.

    Columns: PolNumber, FileName, Status (PASS/FAIL), ErrorDetails, Timestamp.
    Uses positional pairing: validation_results[i] corresponds to xml_results[i].
    """
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for i, vr in enumerate(validation_results):
        xml_info = xml_results[i] if i < len(xml_results) else {}
        filename = xml_info.get("filename", f"ValuesInquiry_{vr.policy_number}.xml")

        rows.append((
            vr.policy_number,
            filename,
            "PASS" if vr.valid else "FAIL",
            "; ".join(vr.errors) if vr.errors else None,
            now,
        ))

    return spark.createDataFrame(rows, schema=SCORECARD_SCHEMA)


def generate_enhanced_scorecard(
    spark: SparkSession,
    analysis_results: list,
) -> DataFrame:
    """
    Create an enhanced PySpark DataFrame scorecard from AnalysisResult objects.

    Includes schema-difference columns so you can see which fields are standard
    (matched), which our schema expects but the XML omits (missing), and which
    the XML contains beyond our schema (custom / carrier-specific).
    """
    from modules.xml_analyzer import AnalysisResult

    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for ar in analysis_results:
        rows.append((
            ar.policy_number,
            ar.filename,
            ar.status,
            "; ".join(ar.validation.errors) if ar.validation.errors else None,
            "; ".join(ar.matched_fields) if ar.matched_fields else None,
            str(len(ar.matched_fields)),
            "; ".join(ar.missing_fields) if ar.missing_fields else None,
            str(len(ar.missing_fields)),
            "; ".join(ar.custom_fields) if ar.custom_fields else None,
            str(len(ar.custom_fields)),
            ar.conformance_pct,
            now,
        ))

    return spark.createDataFrame(rows, schema=ENHANCED_SCORECARD_SCHEMA)


def save_scorecard(df: DataFrame, output_path: str) -> str:
    """Write the scorecard DataFrame to a single CSV file."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(output_path, index=False)
    return output_path


def sort_xml_files(
    validation_results: list[ValidationResult],
    xml_results: list[dict],
    success_dir: str,
    fail_dir: str,
) -> dict[str, list[str]]:
    """
    Copy XML files to success/ or unsuccessful/ based on validation.

    Uses positional pairing: validation_results[i] corresponds to xml_results[i].
    Returns dict with 'success' and 'unsuccessful' keys listing file paths.
    """
    os.makedirs(success_dir, exist_ok=True)
    os.makedirs(fail_dir, exist_ok=True)

    sorted_files: dict[str, list[str]] = {"success": [], "unsuccessful": []}

    for i, vr in enumerate(validation_results):
        if i >= len(xml_results):
            break

        xml_info = xml_results[i]
        src = xml_info["filepath"]
        if not os.path.exists(src):
            continue

        if vr.valid:
            dest = os.path.join(success_dir, xml_info["filename"])
            shutil.copy2(src, dest)
            sorted_files["success"].append(dest)
        else:
            dest = os.path.join(fail_dir, xml_info["filename"])
            shutil.copy2(src, dest)
            sorted_files["unsuccessful"].append(dest)

    return sorted_files


def sort_analyzed_files(
    analysis_results: list,
    success_dir: str,
    fail_dir: str,
) -> dict[str, list[str]]:
    """
    Copy analyzed XML files to success/ or unsuccessful/ based on validation.

    Returns dict with 'success' and 'unsuccessful' keys listing file paths.
    """
    os.makedirs(success_dir, exist_ok=True)
    os.makedirs(fail_dir, exist_ok=True)

    sorted_files: dict[str, list[str]] = {"success": [], "unsuccessful": []}

    for ar in analysis_results:
        src = ar.filepath
        if not src or not os.path.exists(src):
            continue

        if ar.validation.valid:
            dest = os.path.join(success_dir, ar.filename)
            shutil.copy2(src, dest)
            sorted_files["success"].append(dest)
        else:
            dest = os.path.join(fail_dir, ar.filename)
            shutil.copy2(src, dest)
            sorted_files["unsuccessful"].append(dest)

    return sorted_files
