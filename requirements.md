# DTCC IFW 21208 Withdrawal-Quote XML Generator & POV Flattener -- Requirements

## Overview

A modular PySpark application that:

1. **Generates** SOAP-wrapped, ACORD-compliant DTCC Withdrawal-Quote
   (TransType 212 / TransSubType 21208) request XMLs from tabular CSV data.
2. **Validates** generated XMLs against the DTCC IFW Data Dictionary.
3. **Analyzes** external XML files for schema conformance.
4. **Produces scorecards** and sorts output files into success/unsuccessful
   folders.
5. **Parses & flattens** DTCC POV (Positions & Valuations) fixed-width files
   into CSV, with automated validation of data integrity.

Structured for Databricks serverless deployment.

## Architecture

The project follows a pipeline pattern with three entry points:
`run_pipeline` (generate XMLs from CSV), `analyze_external` (analyze
third-party XMLs), and `flatten_pov` (parse & flatten DTCC POV files).
The XML pipelines use PySpark DataFrames for Databricks compatibility; the
POV pipeline is pure-Python for lightweight execution.

```
                                  ┌──────────────────────────────────────────────────────────┐
                                  │                     run_pipeline                         │
                                  │                                                          │
SchemaConfig ──> DDLGenerator ──┐ │                                                          │
                                ├─┴─> TableManager ──> XMLGenerator ──> XMLValidator ──┐     │
CSV Source ───> CSVGenerator ───┘                                                      │     │
                                                                                       v     │
                                                                              ScorecardGenerator
                                                                                  │         │
                                                                             success/  unsuccessful/
                                  └──────────────────────────────────────────────────────────┘

                                  ┌──────────────────────────────────────────────────────────┐
                                  │                    analyze_external                      │
                                  │                                                          │
External XMLs ──> XMLAnalyzer ──> XMLValidator ──> ScorecardGenerator (enhanced)             │
                                                        │         │                          │
                                                   success/  unsuccessful/                   │
                                  └──────────────────────────────────────────────────────────┘

                                  ┌──────────────────────────────────────────────────────────┐
                                  │                      flatten_pov                         │
                                  │                                                          │
POV Fixed-Width File ──> POVParser ──> POVFlattener ──> POVValidator                         │
                                             │                │                              │
                                        CSV output     validation report                     │
                                  └──────────────────────────────────────────────────────────┘
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
│   ├── schema_config.py        # ACORD 21208 schema definition, namespaces, field types, validation rules
│   └── pov_record_layouts.py   # DTCC POV/FAR fixed-width record type definitions (24 types)
├── modules/
│   ├── __init__.py
│   ├── ddl_generator.py        # Step 1: DDL from schema config
│   ├── csv_generator.py        # Step 2: Load & prepare external CSV
│   ├── table_manager.py        # Step 3: Create table + load CSV into Spark
│   ├── xml_generator.py        # Step 4: Table rows -> SOAP-wrapped XML files
│   ├── xml_validator.py        # Step 5: Validate XML against data dictionary
│   ├── xml_analyzer.py         # External XML analysis (schema conformance)
│   ├── scorecard_generator.py  # Steps 7-8: Scorecard + sort into folders
│   ├── pov_parser.py           # POV: Parse fixed-width file into structured records
│   ├── pov_flattener.py        # POV: Flatten hierarchical records into wide CSV rows
│   └── pov_validator.py        # POV: Validate CSV fidelity against original parse
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Shared SparkSession fixture + test data helpers
│   ├── test_ddl_generator.py   #  9 tests
│   ├── test_csv_generator.py   # 10 tests
│   ├── test_table_manager.py   #  6 tests
│   ├── test_xml_generator.py   # 17 tests
│   ├── test_xml_validator.py   #  8 tests
│   ├── test_xml_analyzer.py    # 15 tests
│   ├── test_scorecard_generator.py  # 14 tests
│   ├── test_pov_record_layouts.py   # 14 tests
│   ├── test_pov_parser.py      # 11 tests
│   ├── test_pov_flattener.py   #  7 tests
│   └── test_pov_validator.py   #  8 tests (+ 2 sub-tests)
├── dtcc_ifw_21208_upload_package/
│   ├── README.txt
│   ├── required_files_list.txt
│   └── schema_usage_notes.txt
├── run_output/                 # Runtime output root (configurable via base_dir)
│   ├── output/
│   │   ├── ddl.sql             # Generated DDL
│   │   ├── scorecard.csv       # Validation scorecard
│   │   ├── analysis_scorecard.csv  # Enhanced scorecard (analyze mode)
│   │   ├── xml/                # All generated XMLs (staging)
│   │   ├── success/            # Valid XMLs
│   │   └── unsuccessful/       # Invalid XMLs
│   └── data/
│       └── WD_quote_samples.csv
├── output/pov/                 # POV output directory (auto-created)
│   ├── *_flattened.csv         # Flattened CSV output
│   └── *_validation.txt        # Validation report
├── main.py                     # End-to-end orchestrator (generate + analyze + flatten-pov)
├── requirements.txt
└── requirements.md             # This file
```

## Schema Design

Based on the ACORD TXLifeRequest (TransType 212 / TransSubType 21208) for
Withdrawal Quote, the DTCC IFW WSDL (`processValueInquiry21208` operation),
and the IFW Web Services Data Dictionary.

The schema is defined once in `config/schema_config.py` as a list of
field-descriptor dictionaries (`FIELD_DEFINITIONS`). Every other module reads
from this single source of truth.

### Namespace Constants

| Constant              | Value                                              |
|-----------------------|----------------------------------------------------|
| `SOAP_NAMESPACE`      | `http://www.w3.org/2003/05/soap-envelope`          |
| `OPERATION_NAMESPACE` | `http://service.iwa.dtcc.com/`                     |
| `ACORD_NAMESPACE`     | `http://ACORD.org/Standards/Life/2`                |
| `ACORD_NS_PREFIX`     | `ns3`                                              |
| `OPERATION_NS_PREFIX` | `ns2`                                              |
| `SOAP_NS_PREFIX`      | `soap`                                             |
| `OPERATION_NAME`      | `processValueInquiry21208`                         |
| `MESSAGE_TYPE`        | `VI21208_Msg`                                      |
| `TRANS_TYPE_VALUES_INQUIRY` | `212`                                        |
| `TRANS_SUBTYPE_WD_QUOTE`   | `21208`                                      |

### Field Definitions

Each entry in `FIELD_DEFINITIONS` (135 total: 133 data + 2 metadata) contains:

| Key               | Purpose                                                        |
|-------------------|----------------------------------------------------------------|
| `column_name`     | CSV header / Spark DataFrame column name                       |
| `spark_type`      | Always `"STRING"` (all CSV data is text)                       |
| `group`           | Logical XML section (one of 19 group constants)                |
| `xml_tag`         | Element local name in the XML tree                             |
| `required`        | Whether the field is mandatory per DTCC IFW Data Dictionary    |
| `conditional`     | Human-readable condition (empty string if always required)     |
| `tc_code`         | Fixed tc attribute value (empty string if not fixed)           |
| `description`     | Human-readable field purpose (used in DDL COMMENT)             |
| `max_length`      | Maximum string length (0 = unlimited)                          |
| `regex`           | Validation regex pattern (empty string = none)                 |
| `allowed_values`  | List of valid tc / text values (empty list = any)              |

### Field Groups (19 groups, 135 fields)

**SOAP / Message Wrapper (`soap`) -- 5 fields**

| Column              | Required | Description                            |
|---------------------|----------|----------------------------------------|
| SoapEnvelopeNs      | Yes      | SOAP 1.2 envelope namespace URI        |
| SoapBodyPresent     | Yes      | SOAP Body presence flag (1=yes)        |
| OperationName       | Yes      | WSDL operation name                    |
| OperationNs         | Yes      | WSDL operation namespace               |
| MessageType         | Yes      | IFW message wrapper element name       |

**TXLifeRequest (`txlife_request`) -- 10 fields**

| Column                      | Required | Validation                              |
|-----------------------------|----------|-----------------------------------------|
| TXLifeRequest_PrimaryObjectID | Yes    | Max 50 chars                            |
| TransRefGUID                | Yes      | UUID format, max 36 chars               |
| TransType_tc                | Yes      | Must be `"212"`, tc attribute            |
| TransType_text              | Yes      | Must be `"Values Inquiry"`              |
| TransSubType_tc             | Yes      | Must be `"21208"`, tc attribute          |
| TransSubType_text           | Yes      | Must be `"Withdrawal Quote"`            |
| TransExeDate                | Yes      | YYYY-MM-DD format                       |
| TransExeTime                | Yes      | Time with timezone, max 20 chars        |
| PendingResponseOK_tc        | Yes      | `"0"` or `"1"`, tc attribute             |
| PendingResponseOK_text      | Yes      | `"False"` or `"True"`                   |

**OLifE > Holding (`holding`) -- 4 fields**

| Column                   | Required | Validation                             |
|--------------------------|----------|----------------------------------------|
| Holding_id               | Yes      | Holding element `id` attribute         |
| HoldingTypeCode_tc       | Yes      | Must be `"2"` (Policy), tc attribute   |
| HoldingTypeCode_text     | Yes      | Must be `"Policy"`                     |
| DistributorClientAcctNum | No       | Conditional per carrier, max 30 chars  |

**OLifE > Holding > Policy (`policy`) -- 5 fields**

| Column               | Required | Validation                              |
|----------------------|----------|-----------------------------------------|
| Policy_CarrierPartyID | Yes     | Policy `CarrierPartyID` attribute       |
| PolNumber            | Yes      | Alphanumeric + hyphens, max 30 chars    |
| Policy_CarrierCode   | Yes      | Digits only, max 10 chars               |
| CusipNum             | No       | Conditional per carrier, max 15 chars   |
| Annuity_present      | Yes      | `"0"` or `"1"` (empty Annuity element)  |

**OLifE > Holding > Arrangement (`arrangement`) -- 9 fields**

| Column                      | Required | Validation                          |
|-----------------------------|----------|-------------------------------------|
| ArrMode_tc / ArrMode_text   | Yes      | Arrangement mode tc + text          |
| ArrType_tc / ArrType_text   | Yes      | Arrangement type tc + text          |
| ArrSubType_tc / ArrSubType_text | Yes  | Arrangement sub-type tc + text      |
| ModalAmt                    | No       | Required when ArrSubType tc=4       |
| SourceTransferAmtType_tc / _text | Yes | Source transfer amount type         |

**TaxWithholding -- Federal (`tax_fed`) -- 6 fields**

| Column                  | Required | Validation                             |
|-------------------------|----------|----------------------------------------|
| TaxFed_id               | Yes      | Federal TaxWithholding `id` attribute  |
| TaxFed_AppliesToPartyID | Yes      | `AppliesToPartyID` attribute           |
| TaxFed_Place_tc / _text | Yes      | tc=`"1"` (Federal), text=`"Federal"`   |
| TaxFed_Type_tc / _text  | Yes      | Federal TaxWithholdingType             |

**TaxWithholding -- State (`tax_state`) -- 6 fields**

| Column                    | Required | Validation                            |
|---------------------------|----------|---------------------------------------|
| TaxState_id               | Yes      | State TaxWithholding `id` attribute   |
| TaxState_AppliesToPartyID | Yes      | `AppliesToPartyID` attribute          |
| TaxState_Place_tc / _text | Yes      | tc=`"2"` (State), text=`"State"`      |
| TaxState_Type_tc / _text  | Yes      | State TaxWithholdingType              |

**OLifEExtension -- DTCC IFW (`olife_extension`) -- 4 fields**

| Column                        | Required | Validation                         |
|-------------------------------|----------|------------------------------------|
| OLifEExtension_VendorCode     | No       | Conditional: when AmountQualifier present |
| OLifEExtension_ExtensionCode  | No       | Conditional: when AmountQualifier present |
| AmountQualifier_tc / _text    | No       | Conditional: when ArrSubType tc=4  |

**Party -- Agent (`party_agent`) -- 9 fields**

| Column                           | Required | Validation                      |
|----------------------------------|----------|---------------------------------|
| Party_Agent_id                   | Yes      | Party `id` attribute            |
| Party_Agent_PartyTypeCode_tc / _text | Yes  | `"1"` (Person) or `"2"` (Org)   |
| Party_Agent_FirstName            | Yes      | Max 50 chars                    |
| Party_Agent_LastName             | Yes      | Max 50 chars                    |
| Party_Agent_NIPRNumber           | No       | Conditional per carrier         |
| Party_Agent_IDPart               | Yes      | Partial ID (last 4 SSN)         |
| Party_Agent_PartialIDType_tc / _text | Yes  | PartialIDType tc + text         |

**Party -- Distributor (`party_distributor`) -- 5 fields**

| Column                                  | Required | Validation                |
|-----------------------------------------|----------|---------------------------|
| Party_Distributor_id                    | Yes      | Party `id` attribute      |
| Party_Distributor_PartyTypeCode_tc / _text | Yes   | `"2"` (Organization)      |
| Party_Distributor_DTCCMemberCode        | Yes      | Max 20 chars              |
| Party_Distributor_DTCCAssociatedMemberCode | No    | Conditional per carrier   |

**Party -- Carrier (`party_carrier`) -- 6 fields**

| Column                                  | Required | Validation                |
|-----------------------------------------|----------|---------------------------|
| Party_Carrier_id                        | Yes      | Party `id` attribute      |
| Party_Carrier_PartyTypeCode_tc / _text  | Yes      | `"2"` (Organization)      |
| Party_Carrier_DTCCMemberCode            | Yes      | Max 20 chars              |
| Party_Carrier_DTCCAssociatedMemberCode  | No       | Conditional per carrier   |
| Party_Carrier_CarrierCode               | Yes      | Digits only, max 10 chars |

**Party -- Primary Owner (`party_owner`) -- 8 fields**

| Column                                      | Required | Validation            |
|---------------------------------------------|----------|-----------------------|
| Party_PrimaryOwner_id                       | Yes      | Party `id` attribute  |
| Party_PrimaryOwner_PartyTypeCode_tc / _text | Yes      | Person or Org         |
| Party_PrimaryOwner_FirstName                | Yes      | Max 50 chars          |
| Party_PrimaryOwner_LastName                 | Yes      | Max 50 chars          |
| Party_PrimaryOwner_IDPart                   | Yes      | Partial ID            |
| Party_PrimaryOwner_PartialIDType_tc / _text | Yes      | PartialIDType         |

**Party -- Primary Annuitant (`party_annuitant`) -- 8 fields**

| Column                                         | Required | Validation         |
|------------------------------------------------|----------|--------------------|
| Party_PrimaryAnnuitant_id                      | No*      | Conditional        |
| Party_PrimaryAnnuitant_PartyTypeCode_tc / _text | No*     | Person or Org      |
| Party_PrimaryAnnuitant_FirstName               | No*      | Max 50 chars       |
| Party_PrimaryAnnuitant_LastName                | No*      | Max 50 chars       |
| Party_PrimaryAnnuitant_IDPart                  | No*      | Partial ID         |
| Party_PrimaryAnnuitant_PartialIDType_tc / _text | No*     | PartialIDType      |

\* Required when Annuitant Party is present in the transaction.

**Relation -- Agent / Distributor / Carrier / Owner / Annuitant (9 fields each, 45 total)**

Each Relation block follows the same structure:

| Column (prefix = `Relation_{Role}`)     | Required | Validation                |
|-----------------------------------------|----------|---------------------------|
| {prefix}_id                             | Yes/No*  | Relation `id` attribute   |
| {prefix}_OriginatingObjectID            | Yes/No*  | Points to Holding         |
| {prefix}_RelatedObjectID                | Yes/No*  | Points to Party           |
| {prefix}_OriginatingObjectType_tc / _text | Yes/No* | tc=`"4"` (Holding)       |
| {prefix}_RelatedObjectType_tc / _text   | Yes/No*  | tc=`"6"` (Party)          |
| {prefix}_RoleCode_tc / _text            | Yes/No*  | Role-specific tc code     |

Relation role codes: Agent=`"37"`, Distributor=`"1811900001"`,
Carrier=`"87"`, Owner=`"8"`, Annuitant=`"35"`.

\* Agent, Distributor, Carrier, and Owner relations are required. Annuitant
relation is conditional (required when Annuitant Party is present).

**Test Metadata (`metadata`) -- 2 fields**

| Column          | Required | Description                              |
|-----------------|----------|------------------------------------------|
| ExpectedToPass  | No       | Test expectation flag (`"1"`=pass, `"0"`=fail) |
| FailureReason   | No       | Expected failure reason for test rows    |

### Helper Functions

- `get_spark_schema()` -- returns a `StructType` (all STRING, all nullable)
- `get_column_names()` -- returns ordered list of column names
- `get_required_fields()` -- returns field definitions where `required=True`
- `get_fields_by_group(group)` -- returns field definitions for a given group
- `get_field_by_column(name)` -- look up a single field definition
- `get_data_fields()` -- returns field definitions excluding test-metadata columns

## Module Specifications

### 1. DDL Generator (`modules/ddl_generator.py`)

**Functions:**

- `generate_ddl(table_name, database=None) -> str`
  Produces a Spark SQL `CREATE TABLE IF NOT EXISTS ... USING DELTA` statement.
  All columns are `STRING` with `COMMENT` on every column.
- `save_ddl(ddl, output_path) -> str`
  Writes DDL to a `.sql` file, creating parent directories as needed.

### 2. CSV Generator (`modules/csv_generator.py`)

Loads the Withdrawal-Quote sample CSV (`WD_quote_samples.csv`) and optionally
copies it to the pipeline data directory. This replaces the former
synthetic-data generator; the CSV is now the authoritative source of test rows.

**Functions:**

- `load_csv_rows(csv_path) -> list[dict]`
  Reads all data rows from the CSV via `csv.DictReader`.
- `prepare_csv(source_csv, dest_csv) -> str`
  Copies the source CSV to the pipeline data directory. Skips if source and
  destination are the same file. Returns the destination path.
- `get_sample_rows(csv_path, include_invalid=True) -> list[dict]`
  Returns sample row dicts. If `include_invalid=False`, filters to rows where
  `ExpectedToPass == '1'`.

### 3. Table Manager (`modules/table_manager.py`)

All columns are STRING (matching the ACORD 21208 flat-CSV format).

**Functions:**

- `create_table(spark, ddl) -> None`
  Executes a DDL statement via `spark.sql()`.
- `load_csv(spark, csv_path, table_name, schema=None, database=None) -> DataFrame`
  Reads CSV with `PERMISSIVE` mode and registers as a temp view. Only persists
  to a managed table when `database` is explicitly given (skipped on serverless
  where the default catalog may not be writable).
- `read_table(spark, table_name, database=None) -> DataFrame`
  Reads the full table contents.

### 4. XML Generator (`modules/xml_generator.py`)

Generates SOAP-wrapped ACORD-compliant TXLifeRequest XML using a
string-template approach (Python's ElementTree reserves the `ns\d+` prefix
pattern, so direct construction is impractical).

**Functions:**

- `generate_xml_from_row(row) -> str`
  Converts a single Spark `Row` or dict into a fully SOAP-wrapped ACORD 21208
  XML string with proper namespace prefixes (`soap`, `ns2`, `ns3`) and `tc`
  attributes.
- `generate_all_xmls(df, output_dir) -> list[dict]`
  Iterates over all DataFrame rows, generates one XML per row, writes to disk.
  Returns a list of result dicts with keys: `pol_number`, `filename`,
  `filepath`, `xml_string`.

**Private helpers:**

- `_build_tax_xml(lines, row, prefix)` -- builds TaxWithholding child (Federal/State)
- `_build_person_party_xml(lines, row, prefix, *, producer_field)` -- Person-type Party
- `_build_org_party_xml(lines, row, prefix)` -- Organization-type Party (Distributor)
- `_build_carrier_party_xml(lines, row)` -- Carrier Party (Org + Carrier child)
- `_build_relation_xml(lines, row, prefix)` -- Relation element

**XML structure produced:**

```xml
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
  <soap:Body>
    <ns2:processValueInquiry21208
         xmlns:ns2="http://service.iwa.dtcc.com/"
         xmlns:ns3="http://ACORD.org/Standards/Life/2" ...>
      <ns3:VI21208_Msg>
        <ns3:TXLife>
          <ns3:TXLifeRequest PrimaryObjectID="Holding_1">
            <ns3:TransRefGUID>...</ns3:TransRefGUID>
            <ns3:TransType tc="212">Values Inquiry</ns3:TransType>
            <ns3:TransSubType tc="21208">Withdrawal Quote</ns3:TransSubType>
            <ns3:TransExeDate>2026-03-20</ns3:TransExeDate>
            <ns3:TransExeTime>10:30:00-05:00</ns3:TransExeTime>
            <ns3:PendingResponseOK tc="0">False</ns3:PendingResponseOK>
            <ns3:OLifE>
              <ns3:Holding id="Holding_1">
                <ns3:HoldingTypeCode tc="2">Policy</ns3:HoldingTypeCode>
                <ns3:Policy CarrierPartyID="Party_Carrier">
                  <ns3:PolNumber>ANN-2026-00001</ns3:PolNumber>
                  <ns3:CarrierCode>12345</ns3:CarrierCode>
                  <ns3:Annuity/>
                </ns3:Policy>
                <ns3:Arrangement>
                  <ns3:ArrMode tc="...">...</ns3:ArrMode>
                  <ns3:ArrType tc="...">...</ns3:ArrType>
                  <ns3:ArrSubType tc="...">...</ns3:ArrSubType>
                  <ns3:SourceTransferAmtType tc="...">...</ns3:SourceTransferAmtType>
                  <ns3:TaxWithholding id="TaxFed" AppliesToPartyID="...">
                    <ns3:TaxWithholdingPlace tc="1">Federal</ns3:TaxWithholdingPlace>
                    <ns3:TaxWithholdingType tc="...">...</ns3:TaxWithholdingType>
                  </ns3:TaxWithholding>
                  <ns3:TaxWithholding id="TaxState" AppliesToPartyID="...">
                    <ns3:TaxWithholdingPlace tc="2">State</ns3:TaxWithholdingPlace>
                    <ns3:TaxWithholdingType tc="...">...</ns3:TaxWithholdingType>
                  </ns3:TaxWithholding>
                  <ns3:OLifEExtension VendorCode="..." ExtensionCode="...">
                    <ns3:AmountQualifier tc="...">...</ns3:AmountQualifier>
                  </ns3:OLifEExtension>
                </ns3:Arrangement>
              </ns3:Holding>
              <ns3:Party id="Party_Agent">...</ns3:Party>
              <ns3:Party id="Party_Distributor">...</ns3:Party>
              <ns3:Party id="Party_Carrier">...</ns3:Party>
              <ns3:Party id="Party_PrimaryOwner">...</ns3:Party>
              <ns3:Relation id="Agent_Relation">...</ns3:Relation>
              <ns3:Relation id="Distributor_Relation">...</ns3:Relation>
              <ns3:Relation id="Carrier_Relation">...</ns3:Relation>
              <ns3:Relation id="Owner_Relation">...</ns3:Relation>
            </ns3:OLifE>
          </ns3:TXLifeRequest>
        </ns3:TXLife>
      </ns3:VI21208_Msg>
    </ns2:processValueInquiry21208>
  </soap:Body>
</soap:Envelope>
```

**File naming:** `WDQuote_{PolNumber}.xml` (e.g. `WDQuote_ANN-2026-00001.xml`).
Rows with missing PolNumber produce `WDQuote_UNKNOWN.xml`.

### 5. XML Validator (`modules/xml_validator.py`)

Handles both SOAP-wrapped and bare TXLife root elements.

**Classes:**

- `ValidationResult` (dataclass)
  Fields: `policy_number: str`, `valid: bool`, `errors: list[str]`.
  Method: `to_dict()` for serialization.

**Functions:**

- `validate_xml(xml_string) -> ValidationResult`
  Validates a single XML string. Checks:
  - XML well-formedness (parse error detection)
  - SOAP Envelope presence
  - Operation element and message wrapper presence
  - All required fields are present and non-empty
  - String lengths within `max_length` bounds
  - Values in `allowed_values` sets (where defined)
  - Regex pattern matching (date formats, UUID format, carrier code digits)
  - For `tc`-attributed elements, validates the `tc` attribute value
  - TaxWithholding fields via dynamic lookup by `TaxWithholdingPlace` tc value
  - Party elements (`Party_Agent`, `Party_Distributor`, `Party_Carrier`,
    `Party_PrimaryOwner`) are present
  - Relation elements (`Agent_Relation`, `Distributor_Relation`,
    `Carrier_Relation`, `Owner_Relation`) are present
  - Conditional rules (e.g. `ModalAmt` required when `ArrSubType tc=4`)

- `validate_all(xml_results) -> list[ValidationResult]`
  Batch validation using positional pairing with `xml_results`.

The validator uses a static XPath map (`_XPATH`) to locate each field in the
XML tree, with special handling for TaxWithholding fields (dynamic lookup by
`TaxWithholdingPlace` tc value via `_TAX_MAP`). Party and Relation groups are
skipped in the static map and validated separately by `_validate_parties()` and
`_validate_relations()`.

### 6. XML Analyzer (`modules/xml_analyzer.py`)

Analyzes external DTCC 21208 XMLs against our schema for conformance.

**Classes:**

- `AnalysisResult` (dataclass)
  Fields: `policy_number`, `filename`, `filepath`, `validation`,
  `matched_fields`, `missing_fields`, `custom_fields`, `conformance_pct`.
  Property: `status` ("PASS" or "FAIL" based on `validation.valid`).
  Method: `to_dict()` for serialization.

**Functions:**

- `analyze_xml(xml_string, filename, filepath) -> AnalysisResult`
  Validates the XML, then compares all element local-names against
  `FIELD_DEFINITIONS` to classify fields as matched, missing, or custom
  (present in XML but not in our schema).
- `analyze_xml_file(filepath) -> AnalysisResult`
  Reads an XML file from disk and analyzes it.
- `analyze_xml_directory(directory) -> list[AnalysisResult]`
  Analyzes all `.xml` files in a directory.

Conformance percentage = (matched schema fields / total schema fields) × 100.

### 7. Scorecard Generator (`modules/scorecard_generator.py`)

Supports two modes: basic (from `ValidationResult`) and enhanced (from
`AnalysisResult` with schema diffs).

**Schemas:**

- `SCORECARD_SCHEMA`: `PolNumber | FileName | Status | ErrorDetails | ExpectedResult | Timestamp`
- `ENHANCED_SCORECARD_SCHEMA`: `PolNumber | FileName | Status | ErrorDetails | MatchedFields | MatchedCount | MissingFields | MissingCount | CustomFields | CustomCount | ConformancePct | Timestamp`

**Functions:**

- `generate_scorecard(spark, validation_results, xml_results, csv_rows=None) -> DataFrame`
  Creates a basic PySpark DataFrame scorecard. When `csv_rows` is provided,
  includes `ExpectedToPass` / `FailureReason` in the `ExpectedResult` column
  for comparison.
- `generate_enhanced_scorecard(spark, analysis_results) -> DataFrame`
  Creates an enhanced scorecard from `AnalysisResult` objects with conformance
  metrics.
- `save_scorecard(df, output_path) -> str`
  Converts to pandas and writes a single CSV file.
- `sort_xml_files(validation_results, xml_results, success_dir, fail_dir) -> dict`
  Copies each XML to `success/` or `unsuccessful/` based on validation status.
  Uses positional pairing.
- `sort_analyzed_files(analysis_results, success_dir, fail_dir) -> dict`
  Copies analyzed XML files based on their validation status.

### 8. Main Orchestrator (`main.py`)

Three entry points for the DTCC toolkit.

**`run_pipeline(base_dir, csv_source, table_name, database, reference_xml) -> dict`**

Executes the 8-step generation pipeline:

1. Generate DDL from ACORD 21208 schema config
2. Copy / locate WD_quote_samples CSV
3. Load CSV into Spark table (temp view)
4. Generate SOAP-wrapped ACORD 21208 XML per row
5. Validate each XML against the DTCC IFW Data Dictionary
6. (Optional) Analyze reference XML for conformance
7. Generate scorecard
8. Sort XMLs into success/unsuccessful folders

Returns a summary dict with paths, counts, and optional reference analysis.

**`analyze_external(input_path, base_dir) -> dict`**

Analyzes one or more external DTCC 21208 XMLs:

1. Analyze XML file(s) via `XMLAnalyzer`
2. Generate enhanced scorecard
3. Sort into success/unsuccessful folders
4. Return summary dict

**`flatten_pov(input_path, output_csv, base_dir) -> dict`**

Parses and flattens a DTCC POV/FAR fixed-width file:

1. Parse the fixed-width file via `pov_parser`
2. Flatten hierarchical records into wide CSV via `pov_flattener`
3. Write flattened data to CSV
4. Validate CSV against original parsed data via `pov_validator`
5. Write human-readable validation report

Returns a summary dict with paths, counts, and validation status.

**`main()`**

CLI entry point with subcommands:
- `generate` -- args: `--base-dir`, `--csv-source`, `--table-name`, `--database`,
  `--reference-xml`
- `analyze` -- args: `input_path`, `--base-dir`
- `flatten-pov` -- args: `input_path`, `--output-csv`, `--base-dir`

**Databricks detection:**

`_is_databricks()` checks for `dbutils` in the IPython namespace.
`_get_spark()` reuses the active Databricks session when detected, or creates a
local `local[*]` session otherwise. The local session explicitly sets
`PYSPARK_PYTHON` and `spark.pyspark.python` to `sys.executable` to prevent
PySpark workers from picking up an incompatible system Python.

---

## DTCC POV File Flattener

### Overview

Parses DTCC Positions & Valuations (POV) and Financial Activity Reporting (FAR)
fixed-width text files, flattens the hierarchical multi-record-per-contract
structure into one wide CSV row per contract, and validates the output against
the original parsed data.

### POV Record Layouts (`config/pov_record_layouts.py`)

Defines the byte-level field layouts for 24 DTCC record types:

| Category | Record Types | Description |
|----------|-------------|-------------|
| POV Headers | 100, 120 | File header, firm header |
| POV Detail | 1301-1315 | Contract-level position and valuation data |
| FAR Headers | 400, 420 | FAR file header, firm header |
| FAR Detail | 4301-4309 | Financial activity detail records |

Each record type is defined as an ordered list of `(field_name, width)` tuples.
Standard detail records are 300 bytes wide (exceptions: 1305, 1309 are wider).

**Key sets:**

- `REPEATING_RECORD_TYPES` -- record types that may appear multiple times per
  contract (e.g. `1303` Fund Detail, `1304` Loan Detail)
- `POV_HEADER_TYPES` / `FAR_HEADER_TYPES` -- header vs. detail classification
- `POV_DETAIL_TYPES` / `FAR_DETAIL_TYPES` -- detail record sets

**Helper functions:**

- `get_layout(record_type)` -- retrieve the field spec for a record type
- `get_field_names(record_type, include_filler)` -- get field names
- `get_total_width(record_type)` -- calculate expected line width
- `detect_record_type(line)` -- identify a record type from a raw line

### POV Parser (`modules/pov_parser.py`)

Reads a fixed-width POV/FAR file line-by-line, detects each line's record type,
and extracts fields by byte position.

**Classes:**

- `ParsedRecord` (dataclass) -- a single parsed line with `line_number`,
  `record_type`, `record_description`, `fields` (dict), and `raw_line`.
- `ParsedFile` (dataclass) -- full parse result with `header_records`,
  `detail_records`, `errors`, `total_lines`, `parsed_lines`, `skipped_lines`,
  `file_type`, `valuation_date`. Method: `to_summary_dict()`.

**Functions:**

- `parse_line(line, line_number) -> ParsedRecord | None`
  Detects record type and extracts fields. Returns `None` for unrecognized lines.
- `parse_file(filepath) -> ParsedFile`
  Parses an entire file. Separates header and detail records. Auto-detects
  file type (POV vs. FAR) and extracts the valuation date from the file header.

### POV Flattener (`modules/pov_flattener.py`)

Transforms hierarchical parsed records into a flat CSV structure.

**Classes:**

- `FlattenedResult` (dataclass) -- `header` (column names), `rows` (list of
  dicts), `contract_count`, `record_type_max_occurrences` (per repeating type),
  `source_filepath`.

**Functions:**

- `flatten_parsed_file(parsed) -> FlattenedResult`
  Groups detail records by `Contract_Number`. For each contract, merges all
  record types into a single wide row. Column names follow the pattern
  `{record_type}_{field_name}` (e.g. `1301_Contract_Number`). Repeating
  record types get an occurrence suffix (e.g. `1303_Fund_Value_1`,
  `1303_Fund_Value_2`). Filler fields are excluded from the output.

- `write_csv(result, output_path) -> str`
  Writes the flattened data to a CSV file with the generated header.

### POV Validator (`modules/pov_validator.py`)

Cross-validates the flattened CSV against the original parsed data.

**Classes:**

- `FieldMismatch` (dataclass) -- details of a single value mismatch:
  `contract_number`, `record_type`, `field_name`, `expected`, `actual`,
  `occurrence`.
- `ValidationReport` (dataclass) -- full report: `valid` (bool), `csv_path`,
  `expected_contract_count`, `actual_contract_count`, `total_fields_checked`,
  `mismatches`, `missing_record_types`, `errors`. Properties: `mismatch_count`,
  `is_valid`. Method: `to_summary_dict()`.

**Functions:**

- `validate_flattened_csv(csv_path, parsed, flattened) -> ValidationReport`
  Reads the CSV and compares every non-filler field against the original
  parsed records. Checks contract count, field values, and record type coverage.
- `write_validation_report(report, output_path) -> str`
  Writes a human-readable text report with pass/fail status, summary stats,
  and mismatch details.

## Testing

121 tests across 11 test files, all executed via `pytest`.

| Test File                    | Tests | Covers                                    |
|------------------------------|-------|-------------------------------------------|
| test_ddl_generator.py        |  9    | Column presence, DELTA format, STRING types, comments, database prefix, file save |
| test_csv_generator.py        | 10    | Row loading, header match, CSV copy/prepare, ExpectedToPass filtering |
| test_table_manager.py        |  6    | Table creation, CSV load, column order, empty fields, read-back |
| test_xml_generator.py        | 17    | SOAP envelope/body/operation, namespaces, tc attributes, Party elements, file naming, batch generation |
| test_xml_validator.py        |  8    | Valid pass, malformed XML, missing PolNumber, batch pass/fail, policy number tracking |
| test_xml_analyzer.py         | 15    | Conformance scoring, matched/missing/custom fields, directory analysis, invalid XML handling |
| test_scorecard_generator.py  | 14    | Row count, schema match, PASS/FAIL, enhanced scorecard, error details, CSV save, file sorting, analysis sorting |
| test_pov_record_layouts.py   | 14    | Record descriptions, known widths, 300-byte rule, disjoint sets, detect_record_type, field name helpers |
| test_pov_parser.py           | 11    | Line parsing (1301, 1302, 100), unknown records, multi-contract files, empty lines, file-not-found, summary dict |
| test_pov_flattener.py        |  7    | Single/multi-contract flattening, repeating records with occurrence suffixes, filler exclusion, CSV write |
| test_pov_validator.py        | 10    | Valid single/multi-contract validation, repeating records, tampered CSV detection, missing file, report writing |

**Test infrastructure:**

- `conftest.py` provides a session-scoped `SparkSession` fixture with
  `PYSPARK_PYTHON` pinned to `sys.executable`
- `conftest.py` also provides shared test data helpers (`make_test_row`,
  `write_test_csv`) that generate valid 21208 WD-Quote CSV data with all 135
  schema columns correctly populated
- Each test that loads CSV data uses a unique table name (UUID-based) to avoid
  `TABLE_OR_VIEW_ALREADY_EXISTS` errors across the shared Spark session
- Temp directories are created per-test and cleaned up via the `tmp_dir` fixture

## Implementation Notes (Deviations from Original Plan)

The following adjustments were made during implementation:

1. **SOAP wrapping** -- The XML output is a full SOAP 1.2 envelope wrapping the
   `processValueInquiry21208` operation, not a bare ACORD `TXLife` document.
   This was required by the DTCC IFW web service interface.

2. **String-template XML generation** -- Python's `xml.etree.ElementTree`
   reserves the `ns\d+` prefix pattern, making it impossible to produce the
   required `ns2:` / `ns3:` prefixes. The generator uses string concatenation
   with `xml.sax.saxutils.escape` for value safety.

3. **Schema expansion** -- The schema grew from 18 fields to 135 fields to cover
   the full DTCC IFW 21208 Withdrawal-Quote request including Arrangement,
   TaxWithholding (Federal + State), OLifEExtension, five Party types (Agent,
   Distributor, Carrier, Owner, Annuitant), and five Relation types.

4. **CSV-as-source** -- The CSV generator was rewritten from a synthetic-data
   generator to a simple file loader. The authoritative test data now lives in
   an external CSV (`WD_quote_samples.csv`) with `ExpectedToPass` / `FailureReason`
   metadata columns for scorecard comparison.

5. **Positional pairing in scorecard/sort** -- The original plan assumed keying
   scorecard and sort logic on `pol_number`. This fails when PolNumber is empty
   (invalid test rows). Both `generate_scorecard` and `sort_xml_files` use
   index-based positional pairing.

6. **PySpark worker Python version** -- PySpark 4.x on macOS defaulted to
   Xcode's Python 3.9 for worker processes, which cannot parse `str | None`
   type hints. Fixed by:
   - Adding `from __future__ import annotations` to all modules
   - Setting `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` env vars
   - Configuring `spark.pyspark.python` / `spark.pyspark.driver.python` in
     SparkSession builder

7. **Python 3.12 upgrade** -- Upgraded to 3.12 to match Databricks serverless
   (Python 3.12.3). Required `datetime.now(timezone.utc)` instead of
   `datetime.utcnow()` and pandas >= 2.2.0 with pyarrow.

8. **`output/xml/` staging directory** -- An intermediate `output/xml/`
   directory serves as the initial write target for all generated XMLs before
   they are copied into `success/` or `unsuccessful/`.

9. **External XML analysis** -- The `analyze_external` entry point and
   `xml_analyzer` module were added to support analyzing third-party XMLs
   against our schema for conformance scoring.

10. **Temp views over persistent tables** -- On serverless, the default catalog
    may not be writable. `load_csv` registers a temp view by default and only
    persists to a managed table when `database` is explicitly provided.

11. **Serverless CSV loading** -- The notebook overrides `load_csv` with a
    pandas-based implementation (`_load_csv_serverless`) to work around
    serverless Spark's inability to read local workspace CSV files directly.

12. **POV record layout definitions** -- The 24 DTCC POV/FAR record types were
    manually transcribed from the canonical DTCC I&RS field spacing definitions
    (public `pov.json` via `back9ins/dtcc` GitHub repo). Records 1311 and 1315
    required corrections during initial implementation: `1311`'s Filler field
    was adjusted from 50 to 54 bytes, and `1315`'s entire field list was revised
    to match the canonical source and achieve the correct 300-byte total.

13. **Hierarchical-to-flat transformation** -- The POV flattener handles
    repeating record types (e.g. multiple `1303` Fund Detail records per
    contract) by appending occurrence indices to column names (e.g.
    `1303_Fund_Value_1`, `1303_Fund_Value_2`). The maximum occurrence count
    across all contracts determines the number of columns generated.

14. **POV pipeline is pure Python** -- Unlike the XML pipeline which requires
    PySpark, the POV `flatten-pov` command runs without Spark. This makes it
    lightweight and usable in any Python 3.10+ environment.

15. **Test data helpers** -- `conftest.py` was extended with `make_test_row()`
    and `write_test_csv()` to generate valid 21208 WD-Quote test data on the
    fly. This replaced the old `generate_sample_csv()` function that was
    removed when the CSV generator was redesigned as a simple file loader.

16. **Test suite alignment** -- Test files for the table manager, XML
    generator, XML validator, CSV generator, DDL generator, XML analyzer, and
    scorecard generator were updated to work with the current 135-field 21208
    WD-Quote schema and SOAP-wrapped XML format. The `ns3` namespace prefix
    issue (reserved by Python's ElementTree) was resolved by using a safe
    `acord` prefix in test XML construction.

## Databricks Usage

**As a notebook (serverless):**

```python
from main import run_pipeline
summary = run_pipeline(
    base_dir="/Workspace/Users/you@company.com/XMLGenerator/run_output",
    csv_source="/Workspace/Users/you@company.com/XMLGenerator/run_output/data/WD_quote_samples.csv",
    table_name="wd_quote_21208",
)
```

**Analyze external XMLs:**

```python
from main import analyze_external
summary = analyze_external(
    input_path="/Workspace/Users/you@company.com/external_xmls/",
    base_dir="/Workspace/Users/you@company.com/XMLGenerator/run_output",
)
```

**Flatten DTCC POV file:**

```python
from main import flatten_pov
summary = flatten_pov(
    input_path="/Workspace/Users/you@company.com/data/pov_file.txt",
    base_dir="/Workspace/Users/you@company.com/XMLGenerator/output/pov",
)
```

**As a CLI:**

```bash
python main.py generate --base-dir ./run_output --csv-source ./data/WD_quote_samples.csv
python main.py analyze ./external_xmls/ --base-dir ./run_output
python main.py flatten-pov ./data/pov_file.txt --base-dir ./output/pov
```

All file paths are configurable. The XML pipelines auto-detect Databricks and
reuse the existing SparkSession. The POV pipeline does not require Spark.
