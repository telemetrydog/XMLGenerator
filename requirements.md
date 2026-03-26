    The run folder structure is:

    ```
    run_output/runs/run_YYYYMMDD_HHMMSS_<distributor>/
    ├── pre_generation_validation_<ts>.csv  ← Steps 3b + 3c (combined)
    ├── data/
    │   └── WD_quote_samples.csv           ← Step 4 working CSV
    └── output/
        ├── ddl.sql
        ├── scorecard.csv
        ├── xml/                           ← All generated XMLs (staging)
        ├── success/                       ← Valid XMLs
        └── unsuccessful/                  ← Invalid XMLs
    ```

    Step 3b creates the validation CSV with 18 base columns.  Step 3c
    enriches it in-place by adding `Failure_Reasons`,
    `Predicted_Error_Codes`, and populating the `Result` column
    (`PASS` or predicted carrier error codes).  The final file contains
    ALL contracts — both passing and failing — so the user has the full
    picture in a single file.

22. **Pre-generation failures analysis (Step 3c)** -- The
    `enrich_validation_df(val_df)` function scans every contract in the
    Step 3b validation DataFrame for missing mandatory fields and
    enriches it with failure analysis columns.  Rather than producing a
    separate failures CSV, Step 3c **overwrites** the Step 3b validation
    CSV so the complete audit is in one file.

    **Columns added by Step 3c:**

    | Column | Description |
    |--------|-------------|
    | `Result` | `PASS` if no issues; otherwise the predicted carrier error codes |
    | `Failure_Reasons` | Semicolon-delimited human-readable list of issues (empty if passing) |
    | `Predicted_Error_Codes` | Semicolon-delimited DTCC error codes (empty if passing) |

    **Mandatory field checks:**

    | Check | Validation Column | Predicted Error Code |
    |-------|-------------------|----------------------|
    | Agent NPN/NIPR missing | `NIPRNumber` | 2108-C075 |
    | Owner first name missing | `OwnFirstName` | 2108-C075 |
    | Owner last name missing | `OwnLastName` | 2108-C075 |
    | Owner SSN (last 4) missing | `OwnSSN` | 2108-C075 |
    | Annuitant SSN (last 4) missing | `AnnSSN` | 2108-C075 |
    | Withdrawal value ≤ 0 | `Withdrawal_Value` | 1004900735-X173 |

    **Blanket warnings** (not per-contract failures):
    - `AgtSSN` is empty for ALL contracts because the BHF POV file does
      not supply agent SSN data. This is flagged as a warning in the
      output summary but not included in per-contract failure rows
      (since it would mark every contract as failed).

    **Carrier error code mapping** (derived from actual carrier responses):

    | Carrier Code | Description | Root Cause |
    |-------------|-------------|------------|
    | 2108-C075 | "Not Present" | NIPR or owner/annuitant identity fields missing |
    | 100-BA11 | "Not Present" (×2) | Agent partial ID missing (blanket POV gap) |
    | 1004900735-X173 | "REM RIDER AMT IS 0" | Rider Free quote but carrier-side rider amount is $0 |
    | 2109-None | "RESULTINFO_DIST_MISMATCH" | Distributor does not match policy record (carrier data) |
    | 3013-None | "InternalProcessingError" | Carrier-side transient error |

    The enriched CSV retains all 18 original validation columns plus
    `Failure_Reasons`, `Predicted_Error_Codes`, and `Result`.
    Every contract is included — filter on `Result != 'PASS'` to see
    only failures.  The output summary prints:
    - Pass/fail counts
    - Blanket warnings (e.g. AgtSSN)
    - Breakdown by failure reason with counts
    - Breakdown by predicted carrier error code
    - Count of contracts with multiple overlapping issues


19. **POV-to-XML notebook pipeline** -- The main Databricks notebook was
    reorganized into a six-step pipeline that starts from a POV file:

    1. **Flatten POV** -- Parse a DTCC POV fixed-width file via `flatten_pov`
       into a wide CSV (one row per contract, columns prefixed by record type).
    2. **Explore Output** -- Load and inspect the flattened CSV to identify
       available fields (contract number, CUSIP, agent info, owner info, etc.).
    3. **Map to XML Schema** -- A `POV_TO_XML` dictionary maps POV columns
       to their corresponding ACORD 21208 XML columns.  The remaining ~124
       XML fields are filled with configurable static defaults (SOAP wrapper,
       tc codes, relation IDs, arrangement parameters, tax withholding).
       Agent NPN is extracted from the shifted `Agent_Last_Name[21:31]` field;
       owner SSN (last 4) from the shifted `Party_Last_Name[2:22]` field.

       **Withdrawal type classification** assigns each contract to one of
       five categories (priority order): **RMD** (1302 `RA` qualifier > 0),
       **Rider Free** (1315 Service Feature with `Program_Type=R`,
       `SF_Type` in {204, 336, 215}, `Value > 0`), **Interest Only**
       (1302 `SC` surrender charge > 0), **Surrender Free** (1302 `TW`
       qualifier > 0), or **Partial Withdrawal** (default).  Within each
       type a 50/50 random split assigns Percent (10%) vs Amount (10% of
       contract value) to drive `ArrSubType` and `ModalAmt`.

    3b. **Validation CSV** -- Creates a timestamped run folder
       (`run_YYYYMMDD_HHMMSS_<distributor>`) and builds an 18-column
       pre-generation validation CSV inside it.  The modular
       `build_validation_df(xml_df, pov_df_src)` function extracts key
       fields for data-quality audit before XML generation.
    3c. **Failures Analysis** -- Enriches the Step 3b validation CSV
       in-place with `Failure_Reasons`, `Predicted_Error_Codes`, and
       `Result` (PASS or predicted codes).  Scans for missing mandatory
       fields (NIPR, owner name, owner SSN, annuitant SSN, withdrawal
       value).  No separate file — the single validation CSV contains
       the full picture for both passing and failing contracts.
    4. **Generate XMLs** -- The mapped CSV is fed into `run_pipeline` to
       produce SOAP-wrapped ACORD 21208 XMLs with full validation and
       scorecard generation.  Reuses the run folder from Step 3b.

    A synthetic POV test file (`sample_pov_file.txt`) is generated in Step 1
    for testing; users replace `POV_INPUT_PATH` with their actual file.


## Databricks Usage

**As a notebook (serverless) -- POV-to-XML pipeline:**

The notebook follows a 6-step workflow:

1. Set `POV_INPUT_PATH` to your DTCC POV file
2. Run all cells sequentially (Setup → Flatten → Explore → Map → Validate → Failures → Generate)
3. Adjust configurable defaults in Step 3 (carrier code, DTCC member codes,
   withdrawal amount, arrangement type, tax withholding)
4. Review the validation CSV after Step 3c — it contains both the base
   validation fields and the failure analysis (filter `Result != 'PASS'`
   to see contracts that will likely be rejected)
5. Generated XMLs land in `run_output/runs/run_YYYYMMDD_HHMMSS_<distributor>/output/success/`
