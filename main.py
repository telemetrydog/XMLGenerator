"""
End-to-end orchestrator for the DTCC Values Inquiry XML Generator pipeline.

Steps:
    1. Generate DDL from ACORD schema config
    2. Generate sample CSV with annuity product data
    3. Create Spark table from DDL
    4. Load CSV into the table
    5. Generate ACORD-compliant XML per row
    6. Validate each XML against the data dictionary
    7. Generate a scorecard DataFrame
    8. Sort XMLs into success/ and unsuccessful/ folders
    9. Files named with policy number

Works locally (PySpark local mode) and in Databricks.
"""

from __future__ import annotations

import argparse
import os
import sys

if sys.version_info < (3, 10):
    raise RuntimeError(
        f"Python >= 3.10 is required (3.12+ recommended for Databricks serverless). "
        f"Current version: {sys.version}"
    )

from pyspark.sql import SparkSession

from config.schema_config import get_spark_schema
from modules.ddl_generator import generate_ddl, save_ddl
from modules.csv_generator import generate_sample_csv
from modules.table_manager import create_table, load_csv, read_table
from modules.xml_generator import generate_all_xmls
from modules.xml_validator import validate_all
from modules.xml_analyzer import analyze_xml_file, analyze_xml_directory
from modules.scorecard_generator import (
    generate_scorecard, generate_enhanced_scorecard,
    save_scorecard, sort_xml_files, sort_analyzed_files,
)


def _is_databricks() -> bool:
    """Detect whether we're running inside Databricks."""
    try:
        # dbutils is injected by the Databricks runtime
        import IPython
        ip = IPython.get_ipython()
        return ip is not None and hasattr(ip, "user_ns") and "dbutils" in ip.user_ns
    except Exception:
        return False


def _get_spark(app_name: str = "XMLGenerator") -> SparkSession:
    """Return existing Databricks session or create a local one."""
    if _is_databricks():
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    python_path = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.pyspark.python", python_path)
        .config("spark.pyspark.driver.python", python_path)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def run_pipeline(
    base_dir: str = ".",
    table_name: str = "values_inquiry",
    database: str | None = None,
    num_valid: int = 7,
    num_invalid: int = 2,
) -> dict:
    """
    Execute the full pipeline end-to-end.

    Args:
        base_dir: root output directory
        table_name: name for the Spark SQL table
        database: optional database/schema prefix
        num_valid: number of valid sample rows
        num_invalid: number of intentionally invalid sample rows

    Returns:
        Summary dict with paths and counts.
    """
    output_dir = os.path.join(base_dir, "output")
    data_dir = os.path.join(base_dir, "data")
    xml_dir = os.path.join(output_dir, "xml")
    success_dir = os.path.join(output_dir, "success")
    fail_dir = os.path.join(output_dir, "unsuccessful")

    for d in [output_dir, data_dir, xml_dir, success_dir, fail_dir]:
        os.makedirs(d, exist_ok=True)

    spark = _get_spark()

    # Step 1 - DDL
    print("[1/8] Generating DDL...")
    ddl = generate_ddl(table_name, database)
    ddl_path = save_ddl(ddl, os.path.join(output_dir, "ddl.sql"))
    print(f"       DDL saved to {ddl_path}")

    # Step 2 - CSV
    print("[2/8] Generating sample CSV...")
    csv_path = os.path.join(data_dir, "values_inquiry_sample.csv")
    generate_sample_csv(csv_path, num_valid=num_valid, num_invalid=num_invalid)
    total_rows = num_valid + num_invalid
    print(f"       CSV with {total_rows} rows saved to {csv_path}")

    # Steps 3 & 4 - Table creation and CSV load
    print("[3/8] Creating table and loading CSV...")
    ddl_parquet = ddl.replace("USING DELTA", "USING PARQUET")
    df = load_csv(spark, csv_path, table_name)
    print(f"       Loaded {df.count()} rows into table '{table_name}'")

    # Step 5 - XML generation
    print("[4/8] Generating XML files...")
    xml_results = generate_all_xmls(df, xml_dir)
    print(f"       Generated {len(xml_results)} XML files in {xml_dir}")

    # Step 6 - Validation
    print("[5/8] Validating XMLs...")
    validation_results = validate_all(xml_results)
    passed = sum(1 for r in validation_results if r.valid)
    failed = len(validation_results) - passed
    print(f"       Results: {passed} PASS, {failed} FAIL")

    # Step 7 - Scorecard
    print("[6/8] Generating scorecard...")
    sc_df = generate_scorecard(spark, validation_results, xml_results)
    sc_path = save_scorecard(sc_df, os.path.join(output_dir, "scorecard.csv"))
    print(f"       Scorecard saved to {sc_path}")

    # Step 8 - Sort into folders
    print("[7/8] Sorting XMLs into success/unsuccessful...")
    sorted_files = sort_xml_files(validation_results, xml_results, success_dir, fail_dir)
    print(f"       Success: {len(sorted_files['success'])} files")
    print(f"       Unsuccessful: {len(sorted_files['unsuccessful'])} files")

    print("[8/8] Pipeline complete!")

    summary = {
        "ddl_path": ddl_path,
        "csv_path": csv_path,
        "table_name": table_name,
        "total_rows": total_rows,
        "xml_count": len(xml_results),
        "passed": passed,
        "failed": failed,
        "scorecard_path": sc_path,
        "success_count": len(sorted_files["success"]),
        "unsuccessful_count": len(sorted_files["unsuccessful"]),
        "success_dir": success_dir,
        "unsuccessful_dir": fail_dir,
    }

    return summary


def analyze_external(
    input_path: str,
    base_dir: str = ".",
) -> dict:
    """
    Analyze one or more external DTCC Values Inquiry XMLs against our schema.

    Args:
        input_path: path to a single .xml file or a directory of .xml files
        base_dir: root output directory for scorecard and sorted files

    Returns:
        Summary dict with analysis results.
    """
    output_dir = os.path.join(base_dir, "output")
    success_dir = os.path.join(output_dir, "success")
    fail_dir = os.path.join(output_dir, "unsuccessful")

    for d in [output_dir, success_dir, fail_dir]:
        os.makedirs(d, exist_ok=True)

    spark = _get_spark()

    print(f"[1/4] Analyzing XML(s) from: {input_path}")
    if os.path.isdir(input_path):
        results = analyze_xml_directory(input_path)
    elif os.path.isfile(input_path):
        results = [analyze_xml_file(input_path)]
    else:
        raise FileNotFoundError(f"Input path not found: {input_path}")

    print(f"       Analyzed {len(results)} XML file(s)")

    for ar in results:
        print(f"\n  --- {ar.filename} ---")
        print(f"  Policy:      {ar.policy_number}")
        print(f"  Status:      {ar.status}")
        print(f"  Conformance: {ar.conformance_pct}%")
        print(f"  Matched:     {len(ar.matched_fields)} fields  {ar.matched_fields}")
        print(f"  Missing:     {len(ar.missing_fields)} fields  {ar.missing_fields}")
        print(f"  Custom:      {len(ar.custom_fields)} fields  {ar.custom_fields}")
        if ar.validation.errors:
            print(f"  Errors:      {ar.validation.errors}")

    print(f"\n[2/4] Generating enhanced scorecard...")
    sc_df = generate_enhanced_scorecard(spark, results)
    sc_path = save_scorecard(sc_df, os.path.join(output_dir, "analysis_scorecard.csv"))
    print(f"       Scorecard saved to {sc_path}")

    print("[3/4] Sorting XMLs into success/unsuccessful...")
    sorted_files = sort_analyzed_files(results, success_dir, fail_dir)
    print(f"       Success: {len(sorted_files['success'])} files")
    print(f"       Unsuccessful: {len(sorted_files['unsuccessful'])} files")

    passed = sum(1 for r in results if r.validation.valid)
    failed = len(results) - passed
    print(f"[4/4] Analysis complete!")

    return {
        "input_path": input_path,
        "files_analyzed": len(results),
        "passed": passed,
        "failed": failed,
        "scorecard_path": sc_path,
        "success_count": len(sorted_files["success"]),
        "unsuccessful_count": len(sorted_files["unsuccessful"]),
    }


def main():
    parser = argparse.ArgumentParser(description="DTCC Values Inquiry XML Generator & Analyzer")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    gen_parser = subparsers.add_parser("generate", help="Generate sample XMLs and run pipeline")
    gen_parser.add_argument("--base-dir", default=".", help="Base output directory")
    gen_parser.add_argument("--table-name", default="values_inquiry", help="Spark table name")
    gen_parser.add_argument("--database", default=None, help="Database/schema prefix")
    gen_parser.add_argument("--num-valid", type=int, default=7, help="Number of valid sample rows")
    gen_parser.add_argument("--num-invalid", type=int, default=2, help="Number of invalid sample rows")

    analyze_parser = subparsers.add_parser("analyze", help="Analyze external DTCC XML(s) against our schema")
    analyze_parser.add_argument("input", help="Path to a .xml file or directory of .xml files")
    analyze_parser.add_argument("--base-dir", default=".", help="Base output directory")

    args = parser.parse_args()

    if args.command == "analyze":
        summary = analyze_external(
            input_path=args.input,
            base_dir=args.base_dir,
        )
    elif args.command == "generate":
        summary = run_pipeline(
            base_dir=args.base_dir,
            table_name=args.table_name,
            database=args.database,
            num_valid=args.num_valid,
            num_invalid=args.num_invalid,
        )
    else:
        summary = run_pipeline()

    print("\n=== Summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
