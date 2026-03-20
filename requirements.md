# DTCC Values Inquiry XML Generator -- Requirements

## Overview

A modular PySpark application that generates ACORD-compliant DTCC Values Inquiry
(TransType 212) request XMLs from tabular data, validates them against the
schema/data dictionary, produces a scorecard, and sorts output files into
success/unsuccessful folders. Structured for Databricks serverless deployment.

## Architecture

The project follows a pipeline pattern where each step is an independent module
orchestrated by a main runner. All modules use PySpark DataFrames for Databricks
compatibility.

```
SchemaConfig ──> DDLGenerator ──┐
                                ├──> TableManager ──> XMLGenerator ──> XMLValidator ──> ScorecardGenerator
SchemaConfig ──> CSVGenerator ──┘                                                          │           │
                                                                                      success/   unsuccessful/
```

## Runtime Requirements

| Requirement       | Value                                                       |
|-------------------|-------------------------------------------------------------|
| Python            | >= 3.10 (3.12+ recommended; Databricks serverless v3-v5 uses 3.12.3) |
| PySpark           | >= 3.5.0                                                    |
| pandas            | >= 2.2.0                                                    |
| pyarrow           | >= 15.0.0                                                   |
| lxml              | >= 4.9.0 (optional; reserved for future XSD-based validation) |
| pytest            | >= 7.0.0 (dev/test only)                                    |
| Java              | JDK 17+ (required by PySpark locally; Databricks provides JDK 21) |

A runtime guard in `main.py` raises `RuntimeError` if the Python version is
below 3.10.

All modules include `from __future__ import annotations` to ensure type-hint
syntax (e.g. `str | None`, `list[str]`) is compatible with PySpark worker
processes, which may run on different Python minor versions.

Timestamps use `datetime.now(timezone.utc)` instead of the deprecated
`datetime.utcnow()`, which is removed in Python 3.12+.

## Project Structure

```
XMLGenerator/
├── config/
│   ├── __init__.py
│   └── schema_config.py        # ACORD schema definition, field types, validation rules
├── modules/
│   ├── __init__.py
│   ├── ddl_generator.py        # Step 1: DDL from schema config
│   ├── csv_generator.py        # Step 2: Sample CSV with 9 annuity rows
│   ├── table_manager.py        # Steps 3-4: Create table + load CSV
│   ├── xml_generator.py        # Step 5: Table rows -> XML files
│   ├── xml_validator.py        # Step 6: Validate XML against data dictionary
│   └── scorecard_generator.py  # Steps 7-9: Scorecard + sort into folders
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Shared SparkSession fixture
│   ├── test_ddl_generator.py   # 10 tests
│   ├── test_csv_generator.py   # 11 tests
│   ├── test_table_manager.py   #  6 tests
│   ├── test_xml_generator.py   # 13 tests
│   ├── test_xml_validator.py   #  9 tests
│   └── test_scorecard_generator.py  # 9 tests
├── output/
│   ├── ddl.sql                 # Generated DDL
│   ├── scorecard.csv           # Validation scorecard
│   ├── xml/                    # All generated XMLs (staging)
│   ├── success/                # Valid XMLs
│   └── unsuccessful/           # Invalid XMLs
├── data/
│   └── values_inquiry_sample.csv
├── main.py                     # End-to-end orchestrator
├── requirements.txt
└── requirements.md             # This file
```

## Schema Design

Based on the publicly available ACORD TXLifeRequest_Type (TransType 212) for
Values Inquiry. The schema is defined once in `config/schema_config.py` as a
list of field-descriptor dictionaries (`FIELD_DEFINITIONS`). Every other module
reads from this single source of truth.

### Field Definitions

Each entry in `FIELD_DEFINITIONS` contains:

| Key               | Purpose                                                        |
|-------------------|----------------------------------------------------------------|
| `column_name`     | Spark DataFrame / table column name                            |
| `spark_type`      | `"STRING"` or `"DATE"` (mapped to PySpark types via `SPARK_TYPE_MAP`) |
| `xml_path`        | Full XPath from `TXLifeRequest` root to the element            |
| `xml_tag`         | Element local name in the XML                                  |
| `required`        | Whether the field is mandatory                                 |
| `max_length`      | Maximum string length (None if unlimited)                      |
| `regex`           | Validation regex pattern (None if unconstrained)               |
| `allowed_values`  | List of valid values (None if unconstrained)                   |
| `tc_code`         | ACORD type-code attribute value (None if element has no `tc`)  |
| `tc_description`  | Human-readable label for the `tc` code                         |
| `tc_description_map` | Dict mapping multiple `tc` codes to labels (e.g. PolicyStatus) |
| `is_attribute`    | True when the column maps to an XML attribute rather than element text |
| `attribute_name`  | Name of the attribute (e.g. `"id"` for Holding)               |
| `description`     | Column comment used in DDL generation                          |

### Fields (18 total)

**Transaction-level (10 fields)**

| Column         | Type   | Required | Validation                           |
|----------------|--------|----------|--------------------------------------|
| TransRefGUID   | STRING | Yes      | UUID format, max 36 chars            |
| TransType      | STRING | Yes      | Must be `"212"`, `tc="212"`          |
| TransSubType   | STRING | No       | One of 21201-21207                   |
| TransExeDate   | DATE   | Yes      | YYYY-MM-DD format                    |
| TransExeTime   | STRING | Yes      | HH:mm:ss format, max 8 chars        |
| TransEffDate   | DATE   | No       | YYYY-MM-DD format                    |
| InquiryLevel   | STRING | No       | Max 10 chars                         |
| InquiryView    | STRING | No       | Max 10 chars                         |
| NoResponseOK   | STRING | No       | `"0"` or `"1"`                       |
| TestIndicator  | STRING | No       | `"0"` or `"1"`                       |

**OLifE > Holding (2 fields)**

| Column          | Type   | Required | Validation                          |
|-----------------|--------|----------|-------------------------------------|
| HoldingID       | STRING | Yes      | Maps to Holding `id` attribute      |
| HoldingTypeCode | STRING | Yes      | Must be `"2"` (Policy), `tc="2"`    |

**OLifE > Holding > Policy (6 fields)**

| Column          | Type   | Required | Validation                          |
|-----------------|--------|----------|-------------------------------------|
| PolNumber       | STRING | Yes      | Alphanumeric + hyphens, max 30      |
| ProductCode     | STRING | No       | Max 20 chars                        |
| CarrierCode     | STRING | Yes      | Digits only, max 10 chars           |
| LineOfBusiness  | STRING | No       | `"1"` (Life), `"2"` (Annuity), `"3"` (Health) |
| ProductType     | STRING | No       | Free-text, max 50 chars             |
| PolicyStatus    | STRING | No       | `"1"`-`"12"` with label map (Active, Inactive, etc.) |

### Helper Functions

- `get_spark_schema()` -- returns a `StructType` for PySpark DataFrame/table creation
- `get_column_names()` -- returns ordered list of column names
- `get_required_fields()` -- returns field definitions where `required=True`
- `get_field_by_column(name)` -- look up a single field definition
- `get_xml_path_groups()` -- group fields by XML parent path (for tree construction)

## Module Specifications

### 1. DDL Generator (`modules/ddl_generator.py`)

**Functions:**

- `generate_ddl(table_name, database=None) -> str`
  Produces a Spark SQL `CREATE TABLE IF NOT EXISTS ... USING DELTA` statement
  with `NOT NULL` constraints on required columns and `COMMENT` on every column.
- `save_ddl(ddl, output_path) -> str`
  Writes DDL to a `.sql` file, creating parent directories as needed.

### 2. CSV Generator (`modules/csv_generator.py`)

**Functions:**

- `generate_sample_csv(output_path, num_valid=7, num_invalid=2) -> str`
  Writes a CSV with 1 header row + `num_valid + num_invalid` data rows.
- `get_sample_rows(include_invalid=True) -> list[dict]`
  Returns sample row dicts for programmatic use without file I/O.

**Sample data:**

7 valid rows covering annuity product types: Variable Annuity, Fixed Indexed
Annuity, Single Premium Immediate Annuity (SPIA), Fixed Annuity, Deferred
Income Annuity (DIA), Registered Index-Linked Annuity (RILA), and Market Value
Adjusted Annuity (MVA). Policy numbers follow the pattern `ANN-2026-NNNNN`.
TransRefGUIDs are deterministic (UUID5 from policy seed).

2 intentionally invalid rows:
- Row 8: `TransExeDate` in wrong format (`"03-19-2026"` instead of `"2026-03-19"`)
- Row 9: `PolNumber` is empty (violates required constraint)

### 3. Table Manager (`modules/table_manager.py`)

**Functions:**

- `create_table(spark, ddl) -> None`
  Executes a DDL statement via `spark.sql()`.
- `load_csv(spark, csv_path, table_name, schema=None, database=None) -> DataFrame`
  Reads CSV with permissive mode so invalid rows are ingested (bad dates become
  `null`). Drops and recreates the table on each load.
- `read_table(spark, table_name, database=None) -> DataFrame`
  Reads the full table contents.

### 4. XML Generator (`modules/xml_generator.py`)

**Functions:**

- `generate_xml_from_row(row) -> str`
  Converts a single Spark `Row` into a well-formed ACORD TXLifeRequest XML
  string with XML declaration, ACORD namespace
  (`http://ACORD.org/Standards/Life/2`), and proper `tc` attributes.
- `generate_all_xmls(df, output_dir) -> list[dict]`
  Iterates over all DataFrame rows, generates one XML per row, writes to disk.
  Returns a list of result dicts with keys: `pol_number`, `filename`,
  `filepath`, `xml_string`.

**XML structure produced:**

```xml
<?xml version='1.0' encoding='utf-8'?>
<ACORD:TXLife xmlns:ACORD="http://ACORD.org/Standards/Life/2">
    <ACORD:TXLifeRequest PrimaryObjectID="Holding_1">
        <ACORD:TransRefGUID>...</ACORD:TransRefGUID>
        <ACORD:TransType tc="212">Values Inquiry</ACORD:TransType>
        <ACORD:TransSubType>21207</ACORD:TransSubType>
        <ACORD:TransExeDate>2026-03-19</ACORD:TransExeDate>
        <ACORD:TransExeTime>10:30:00</ACORD:TransExeTime>
        ...
        <ACORD:OLifE>
            <ACORD:Holding id="Holding_1">
                <ACORD:HoldingTypeCode tc="2">Policy</ACORD:HoldingTypeCode>
                <ACORD:Policy>
                    <ACORD:PolNumber>ANN-2026-00001</ACORD:PolNumber>
                    <ACORD:ProductCode>VA-100</ACORD:ProductCode>
                    <ACORD:CarrierCode>12345</ACORD:CarrierCode>
                    <ACORD:LineOfBusiness tc="2">Annuity</ACORD:LineOfBusiness>
                    <ACORD:ProductType>Variable Annuity</ACORD:ProductType>
                    <ACORD:PolicyStatus tc="1">Active</ACORD:PolicyStatus>
                </ACORD:Policy>
            </ACORD:Holding>
        </ACORD:OLifE>
    </ACORD:TXLifeRequest>
</ACORD:TXLife>
```

**File naming:** `ValuesInquiry_{PolNumber}.xml` (e.g. `ValuesInquiry_ANN-2026-00001.xml`).
Rows with missing PolNumber produce `ValuesInquiry_UNKNOWN.xml`.

### 5. XML Validator (`modules/xml_validator.py`)

**Classes:**

- `ValidationResult` (dataclass)
  Fields: `policy_number: str`, `valid: bool`, `errors: list[str]`.
  Method: `to_dict()` for serialization.

**Functions:**

- `validate_xml(xml_string) -> ValidationResult`
  Validates a single XML string. Checks:
  - XML well-formedness (parse error detection)
  - Root element is `TXLife` in the ACORD namespace
  - All required fields are present and non-empty
  - String lengths within `max_length` bounds
  - Values in `allowed_values` sets (where defined)
  - Regex pattern matching (date formats, UUID format, carrier code digits)
  - For `tc`-attributed elements, validates the `tc` attribute value

- `validate_all(xml_results) -> list[ValidationResult]`
  Batch validation using positional pairing with `xml_results`.

The validator uses a hardcoded XPath map (`_XPATH_MAP`) to locate each field
in the XML tree, handling both element text and attribute values (e.g. Holding `id`).

### 6. Scorecard Generator (`modules/scorecard_generator.py`)

**Functions:**

- `generate_scorecard(spark, validation_results, xml_results) -> DataFrame`
  Creates a PySpark DataFrame with schema:
  `PolNumber | FileName | Status (PASS/FAIL) | ErrorDetails | Timestamp`.
  Uses positional pairing between validation results and XML results.

- `save_scorecard(df, output_path) -> str`
  Converts to pandas and writes a single CSV file.

- `sort_xml_files(validation_results, xml_results, success_dir, fail_dir) -> dict`
  Copies each XML to either `success/` or `unsuccessful/` based on its
  validation status. Returns `{"success": [...paths], "unsuccessful": [...paths]}`.
  Uses positional pairing to correctly match results even when PolNumber is
  missing.

### 7. Main Orchestrator (`main.py`)

**Functions:**

- `run_pipeline(base_dir, table_name, database, num_valid, num_invalid) -> dict`
  Executes all 8 pipeline steps and returns a summary dict.
- `main()`
  CLI entry point with argparse (args: `--base-dir`, `--table-name`,
  `--database`, `--num-valid`, `--num-invalid`).

**Databricks detection:**

`_is_databricks()` checks for `dbutils` in the IPython namespace.
`_get_spark()` reuses the active Databricks session when detected, or creates a
local `local[*]` session otherwise. The local session explicitly sets
`PYSPARK_PYTHON` and `spark.pyspark.python` to `sys.executable` to prevent
PySpark workers from picking up an incompatible system Python.

## Testing

58 tests across 6 test files, all executed via `pytest`.

| Test File                    | Tests | Covers                                    |
|------------------------------|-------|-------------------------------------------|
| test_ddl_generator.py        | 10    | Column presence, NOT NULL, DELTA format, comments, database prefix, file save |
| test_csv_generator.py        | 11    | Row count, header match, TransType=212, invalid row content, unique policy numbers |
| test_table_manager.py        |  6    | Table creation, CSV load, column order, null handling for bad dates, read-back |
| test_xml_generator.py        | 13    | Well-formedness, namespace, tc attributes, PrimaryObjectID, XML declaration, file naming, batch generation |
| test_xml_validator.py        |  9    | Valid pass, malformed XML, missing required fields, wrong root, batch mixed valid/invalid |
| test_scorecard_generator.py  |  9    | Row count, schema match, PASS/FAIL presence, error details, CSV save, file sorting, accounting |

**Test infrastructure:**

- `conftest.py` provides a session-scoped `SparkSession` fixture with
  `PYSPARK_PYTHON` pinned to `sys.executable`
- Each test that loads CSV data uses a unique table name (UUID-based) to avoid
  `TABLE_OR_VIEW_ALREADY_EXISTS` errors across the shared Spark session
- Temp directories are created per-test and cleaned up via the `tmp_dir` fixture

## Implementation Notes (Deviations from Original Plan)

The following adjustments were made during implementation:

1. **Positional pairing in scorecard/sort** -- The original plan assumed keying
   scorecard and sort logic on `pol_number`. This fails when PolNumber is empty
   (the invalid test row), because the XML result stores `pol_number=""` while
   the validator assigns `policy_number="UNKNOWN"`. Both `generate_scorecard`
   and `sort_xml_files` were changed to use index-based positional pairing
   (`validation_results[i]` corresponds to `xml_results[i]`).

2. **PySpark worker Python version** -- PySpark 4.x on macOS defaulted to
   Xcode's Python 3.9 for worker processes, which cannot parse `str | None`
   type hints. Fixed by:
   - Adding `from __future__ import annotations` to all modules
   - Setting `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` env vars
   - Configuring `spark.pyspark.python` / `spark.pyspark.driver.python` in
     SparkSession builder

3. **Python 3.12 upgrade** -- Upgraded from Python 3.11.4 to 3.12 in a
   dedicated conda environment (`xmlgen`) to match Databricks serverless
   environment v3-v5 (Python 3.12.3). This required:
   - Replacing `datetime.utcnow()` with `datetime.now(timezone.utc)` (3.12
     deprecation)
   - Upgrading pandas to >= 2.2.0 and adding pyarrow as an explicit dependency

4. **`output/xml/` staging directory** -- An intermediate `output/xml/`
   directory was added as the initial write target for all generated XMLs before
   they are copied into `success/` or `unsuccessful/`. This was not in the
   original plan but provides a clean audit trail of all generated files
   regardless of validation outcome.

5. **Delta vs Parquet** -- DDL is generated with `USING DELTA` for Databricks.
   Locally, Spark's `saveAsTable` writes Parquet by default (Delta requires
   `delta-spark`). The pipeline works in both modes transparently.

6. **Additional dependencies** -- `pandas` and `pyarrow` were added to
   `requirements.txt` (not in the original plan) because `save_scorecard` uses
   `df.toPandas()` and PySpark 4.x requires pyarrow for DataFrame operations.

## Databricks Usage

**As a script:**

```python
from main import run_pipeline
summary = run_pipeline(
    base_dir="/dbfs/mnt/your-mount",  # or a Unity Catalog volume path
    table_name="values_inquiry",
    database="insurance_db",
)
```

**As a CLI:**

```bash
conda activate xmlgen
python main.py --base-dir /dbfs/mnt/data --table-name values_inquiry --database insurance_db
```

All file paths are configurable. The pipeline auto-detects Databricks and reuses
the existing SparkSession.
