"""
End-to-end orchestrator for the DTCC IFW 21208 Withdrawal-Quote
XML Generator pipeline.

Steps:
    1. Generate DDL from ACORD 212/21208 schema config
    2. Copy / locate WD_quote_samples CSV
    3. Create Spark table from CSV
    4. Generate SOAP-wrapped ACORD 21208 XML per row
    5. Validate each XML against the DTCC IFW Data Dictionary
    6. (Optional) Analyze reference XML for conformance
    7. Generate a scorecard DataFrame
    8. Sort XMLs into success/ and unsuccessful/ folders

Works locally (PySpark local mode) and in Databricks.
"""

from __future__ import annotations

import argparse
import os
import sys

if sys.version_info < (3, 10):
    raise RuntimeError(
        f"Python >= 3.10 is required. Current version: {sys.version}"
    )

from pyspark.sql import SparkSession

from modules.ddl_generator import generate_ddl, save_ddl
from modules.csv_generator import prepare_csv, load_csv_rows
from modules.table_manager import load_csv
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
    csv_source: str = "",
    table_name: str = "wd_quote_21208",
    database: str | None = None,
    reference_xml: str = "",
) -> dict:
    """
    Execute the full pipeline end-to-end.

    Args:
        base_dir:       root output directory
        csv_source:     path to WD_quote_samples.csv (if empty, looks in
                        base_dir/data/WD_quote_samples.csv)
        table_name:     name for the Spark SQL table
        database:       optional database/schema prefix
        reference_xml:  path to a known-good reference XML for analysis

    Returns:
        Summary dict with paths and counts.
    """
    output_dir  = os.path.join(base_dir, "output")
    data_dir    = os.path.join(base_dir, "data")
    xml_dir     = os.path.join(output_dir, "xml")
    success_dir = os.path.join(output_dir, "success")
    fail_dir    = os.path.join(output_dir, "unsuccessful")

    for d in [output_dir, data_dir, xml_dir, success_dir, fail_dir]:
        os.makedirs(d, exist_ok=True)

    spark = _get_spark()

    # Resolve CSV source
    if not csv_source:
        csv_source = os.path.join(data_dir, "WD_quote_samples.csv")
    if not os.path.isfile(csv_source):
        raise FileNotFoundError(f"CSV source not found: {csv_source}")

    # Step 1 – DDL
    print("[1/8] Generating DDL...")
    ddl = generate_ddl(table_name, database)
    ddl_path = save_ddl(ddl, os.path.join(output_dir, "ddl.sql"))
    print(f"       DDL saved to {ddl_path}")

    # Step 2 – Prepare CSV
    print("[2/8] Preparing CSV data...")
    dest_csv = os.path.join(data_dir, "WD_quote_samples.csv")
    prepare_csv(csv_source, dest_csv)
    csv_rows = load_csv_rows(dest_csv)
    total_rows = len(csv_rows)
    print(f"       CSV with {total_rows} rows ready at {dest_csv}")

    # Step 3 – Load into Spark
    print("[3/8] Loading CSV into Spark table...")
    df = load_csv(spark, dest_csv, table_name)
    print(f"       Loaded {df.count()} rows into table '{table_name}'")

    # Step 4 – XML generation
    print("[4/8] Generating SOAP-wrapped 21208 XML files...")
    xml_results = generate_all_xmls(df, xml_dir)
    print(f"       Generated {len(xml_results)} XML files in {xml_dir}")

    # Step 5 – Validation
    print("[5/8] Validating XMLs against DTCC IFW Data Dictionary...")
    validation_results = validate_all(xml_results)
    passed = sum(1 for r in validation_results if r.valid)
    failed = len(validation_results) - passed
    print(f"       Results: {passed} PASS, {failed} FAIL")

    # Step 6 – Reference XML analysis (optional)
    ref_analysis = None
    if reference_xml and os.path.isfile(reference_xml):
        print(f"[6/8] Analyzing reference XML: {os.path.basename(reference_xml)}...")
        ref_analysis = analyze_xml_file(reference_xml)
        print(f"       Reference status={ref_analysis.status}  "
              f"conformance={ref_analysis.conformance_pct}%")
    else:
        print("[6/8] No reference XML provided – skipping.")

    # Step 7 – Scorecard
    print("[7/8] Generating scorecard...")
    sc_df = generate_scorecard(spark, validation_results, xml_results,
                               csv_rows=csv_rows)
    sc_path = save_scorecard(sc_df, os.path.join(output_dir, "scorecard.csv"))
    print(f"       Scorecard saved to {sc_path}")

    # Step 8 – Sort
    print("[8/8] Sorting XMLs into success/unsuccessful...")
    sorted_files = sort_xml_files(validation_results, xml_results,
                                  success_dir, fail_dir)
    print(f"       Success: {len(sorted_files['success'])} files")
    print(f"       Unsuccessful: {len(sorted_files['unsuccessful'])} files")

    print("\nPipeline complete!")

    return {
        "ddl_path": ddl_path,
        "csv_path": dest_csv,
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
        "reference_analysis": ref_analysis.to_dict() if ref_analysis else None,
    }


def analyze_external(
    input_path: str,
    base_dir: str = ".",
) -> dict:
    """
    Analyze one or more external DTCC 21208 XMLs against our schema.
    """
    output_dir  = os.path.join(base_dir, "output")
    success_dir = os.path.join(output_dir, "success")
    fail_dir    = os.path.join(output_dir, "unsuccessful")
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
        print(f"  Matched:     {len(ar.matched_fields)} fields")
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

    passed_count = sum(1 for r in results if r.validation.valid)
    failed_count = len(results) - passed_count
    print("[4/4] Analysis complete!")

    return {
        "input_path": input_path,
        "files_analyzed": len(results),
        "passed": passed_count,
        "failed": failed_count,
        "scorecard_path": sc_path,
        "success_count": len(sorted_files["success"]),
        "unsuccessful_count": len(sorted_files["unsuccessful"]),
    }


def main():
    parser = argparse.ArgumentParser(
        description="DTCC IFW 21208 Withdrawal-Quote XML Generator & Analyzer"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    gen_parser = subparsers.add_parser(
        "generate", help="Generate 21208 WD-Quote XMLs from CSV"
    )
    gen_parser.add_argument("--base-dir", default=".",
                            help="Base output directory")
    gen_parser.add_argument("--csv-source", default="",
                            help="Path to WD_quote_samples.csv")
    gen_parser.add_argument("--table-name", default="wd_quote_21208",
                            help="Spark table name")
    gen_parser.add_argument("--database", default=None,
                            help="Database/schema prefix")
    gen_parser.add_argument("--reference-xml", default="",
                            help="Path to known-good reference XML")

    analyze_parser = subparsers.add_parser(
        "analyze", help="Analyze external DTCC 21208 XML(s)"
    )
    analyze_parser.add_argument("input_path",
                                help="Path to XML file or directory")
    analyze_parser.add_argument("--base-dir", default=".",
                                help="Base output directory")

    args = parser.parse_args()

    if args.command == "generate":
        run_pipeline(
            base_dir=args.base_dir,
            csv_source=args.csv_source,
            table_name=args.table_name,
            database=args.database,
            reference_xml=args.reference_xml,
        )
    elif args.command == "analyze":
        analyze_external(args.input_path, base_dir=args.base_dir)
    else:
        run_pipeline()


if __name__ == "__main__":
    main()
