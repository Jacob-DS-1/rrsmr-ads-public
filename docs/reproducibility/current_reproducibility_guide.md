# Current Reproducibility Guide

This guide records the current reproducibility status of the Rolls-Royce SMR ADS public release. The repository aim is to provide a tidy, auditable, reproducible workflow for modelling the impact of Rolls-Royce SMRs on the GB National Grid.

## Current status

The current public-release workflow is reproducible from a fresh clone plus the public Zenodo source-input bundle.

Validated current state:

- a fresh clone works after the Zenodo source inputs are restored;
- source inputs are restored at external_data/source_inputs;
- the complete workflow runs from the restored source-input paths;
- audit_reproducible_outputs.py reports Overall status: pass;
- pytest reports 136 passed;
- dashboard data builds successfully;
- the Streamlit dashboard runs locally from repo-generated dashboard outputs.

A preprocessing runner is available for rebuilding or verifying shared cleaned preprocessing inputs.

Objective 3 is covered by repository-path runners.

The audit helper now checks the main Objective 1, Objective 2, Objective 3, Owner 5 sensitivity, and figure-output contracts.

## Core conventions

The current workflow uses:

- timestamp_utc as the canonical time column;
- hourly UTC time resolution;
- GB system boundary, excluding Northern Ireland;
- MW for hourly power values;
- Parquet directory-style datasets for large processed model outputs;
- Electric Engagement and Holistic Transition as the two FES scenarios.

## Important paths

Main configuration:

    config/paths.yaml

Source-input restore configuration:

    config/source_data_bundle.json

Restored public source inputs:

    external_data/source_inputs/objective1_raw
    external_data/source_inputs/ERA5

Objective 1 runner:

    scripts/run_objective1_generation.sh

Objective 2 runner:

    scripts/run_objective2_demand.sh

Shared preprocessing restore/check helper:

    scripts/restore_objective1_preprocessing_inputs.sh

Complete workflow runner:

    scripts/run_complete_model.sh

Objective 1 final processed output:

    data/processed/objective1_generation/generation_future_hourly_2030_2045

Objective 2 final processed output:

    data/processed/objective2_demand/demand_future_hourly_2030_2045

Objective 3 expected SMR fleet output:

    data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045

Objective 3 expected grid master output:

    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045

## Fresh clone / clean checkout checks

Start from a clean clone and confirm the environment:

    git status --short
    python --version
    python -m pytest -q tests/test_prepare_source_inputs.py

The most recent known full test status after final public-release validation was:

    136 passed

## Restoring source inputs from Zenodo

Restore the public source-input bundle:

    python scripts/prepare_source_inputs.py \
      --archive "https://zenodo.org/records/20073518/files/rrsmr-source-inputs-final-delivery.zip?download=1" \
      --expected-sha256 39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c \
      --dest external_data/source_inputs

The restored archive provides the source directories used by preprocessing and ERA5 resource processing:

    external_data/source_inputs/objective1_raw
    external_data/source_inputs/ERA5

external_data/ is intentionally ignored and should not be committed.

## Complete model workflow

Run the full validated workflow:

    bash scripts/run_complete_model.sh \
      --clean \
      --kernel-name rrsmr-ads-fresh \
      --source-dir external_data/source_inputs/objective1_raw \
      --era5-source-dir external_data/source_inputs/ERA5

The complete runner orchestrates preprocessing, Objective 1 generation, Objective 2 demand, Objective 3 SMR fleet, Objective 3 integration, Objective 3 system-impact metrics, Objective 3 final QA/dashboard sensitivity outputs, and the reproducible output audit.

The complete runner finishes by calling:

    python scripts/audit_reproducible_outputs.py

Expected audit status:

    Overall status: pass

## Objective 1 generation output

Clean rerun through the complete workflow produces:

    data/processed/objective1_generation/generation_future_hourly_2030_2045

Expected reference contract:

- rows: 841536
- date range: 2030-01-01 00:00 UTC to 2045-12-31 23:00 UTC
- years: 2030-2045
- scenarios: Electric Engagement, Holistic Transition
- weather years: 2010, 2014, 2015
- weather-year roles: low_wind, average_wind, high_wind
- required/model missing values: 0
- negative MW values: 0
- content_sha256: a8feb70e7b7a27ec2c7087fed6486f133f109f949227429a4c2ca8a6a2073d99

Objective 1 key contract:

- unique generated-output key: timestamp_utc + year + fes_scenario + weather_year
- Objective 3 semantic generation key: timestamp_utc + year + fes_scenario + weather_year_role
- timestamp_utc + year + fes_scenario alone is intentionally not unique
- expected row count: 140256 hours x 2 FES scenarios x 3 weather years = 841536 rows

## Objective 2 demand output

Clean rerun through the complete workflow produces:

    data/processed/objective2_demand/demand_future_hourly_2030_2045

Expected reference contract:

- rows: 841536
- date range: 2030-01-01 00:00 UTC to 2045-12-31 23:00 UTC
- years: 2030-2045
- scenarios: Electric Engagement, Holistic Transition
- climate members: member_06, member_12, member_13
- duplicate keys: 0
- required NaNs: 0
- content_sha256: 0ea51272fb868017b33a3d3feea15221693066f950027b8a78944cc545537bc8

## Objective 3 current reproducibility status

Objective 3 is included in the complete public-release workflow.

The current workflow includes:

- SMR fleet generation;
- demand/generation/SMR grid integration;
- system-impact metrics;
- final visualisation and QA package;
- dashboard-ready data build.

Objective 3 should be described as the improved current repository workflow rather than a byte-for-byte reproduction of the original local handoff.

Current SMR cases:

- base case: staggered commissioning of 3 x 470 MWe Wylfa SMRs;
- sensitivity case: simultaneous commissioning.

Current SMR assumptions are reconciled in:

    config/smr_assumptions.csv

## Output audit helper

After the complete workflow has generated outputs, run:

    python scripts/audit_reproducible_outputs.py

For a fresh clone or CI smoke check where generated outputs are intentionally absent, run:

    python scripts/audit_reproducible_outputs.py --allow-missing --json

Use strict file-hash checking only when specifically comparing byte-level output:

    python scripts/audit_reproducible_outputs.py --strict-file-hash

## Dashboard

Build dashboard data:

    python dashboard/objective3_smr_integration/scripts/build_dashboard_data.py

Run the dashboard locally:

    python -m streamlit run dashboard/objective3_smr_integration/app.py

The dashboard reads generated data from:

    outputs/dashboard/objective3_smr_integration/data/

## Generated artifact hygiene

Generated data and outputs should remain ignored, except selected .gitkeep files.

Before opening a PR or publishing the release, check for forbidden tracked artifacts:

    git ls-files | grep -Ei '\.(parquet|pkl|joblib|pyc|png|jpe?g)$|__pycache__|\.egg-info|\.DS_Store|\.Rhistory' || true

Expected result: no output.

Also check repository status:

    git status --short

Expected result before committing documentation-only changes: modified documentation/config files only, with no generated data or output artifacts.

## Current practical validation order

1. Restore the public Zenodo source inputs.
2. Run the complete workflow with --clean.
3. Run the reproducible output audit.
4. Run the full pytest suite.
5. Build dashboard data.
6. Run the dashboard locally.
7. Check forbidden tracked artifacts.
8. Check notebook outputs are clear.
9. Commit only source, configuration, documentation, tests, notebooks, and scripts.

Do not claim Objective 3 outputs are byte-for-byte unchanged from the original local handoff; the repo version is the improved current workflow with explicit SMR availability assumptions.
