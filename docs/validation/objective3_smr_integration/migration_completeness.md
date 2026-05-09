# Objective 3 migration completeness record

## Purpose

This document records the final migration state for Objective 3 SMR integration after the scaffold, Owner 3, Owner 4 summary evidence, Owner 5, and SMR assumptions reconciliation PRs.

This is a migration completeness record only. It does not introduce new modelling logic.

## Current migrated source files

| File | Role |
|---|---|
| `src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py` | Owner 3 data integration script migrated to repo-relative paths |
| `src/rrsmr_ads/objective3_smr_integration/owner5_visualisations_final_qa.py` | Owner 5 visualisation/final-QA script migrated to repo-relative paths |
| `src/rrsmr_ads/objective3_smr_integration/__init__.py` | Objective 3 package marker |

No Owner 4 source script or notebook was present in the inspected Objective 3 source inventory. Owner 4 was therefore migrated as summary evidence only.

## Current migrated validation evidence

Owner 3 evidence:

- `owner3_data_integration_README.md`
- `input_alignment_qa_log.md`
- `grid_master_schema.csv`
- `integration_summary.csv`
- `duplicate_key_check.csv`
- `missing_value_check.csv`
- `owner3_decision_register.csv`
- `row_count_reconciliation.csv`
- `scenario_coverage_check.csv`
- `timestamp_coverage_check.csv`

Owner 4 evidence:

- `owner4_system_impact_metrics_README.md`
- `owner4_system_impact_summary_annual_2030_2045.csv`
- `owner4_system_impact_summary_period_2030_2045.csv`

Owner 5 evidence:

- `objective3_master_README.md`
- `owner5_QA_reconciliation_report.md`
- `owner5_input_manifest.csv`
- `owner5_qa_checks.csv`
- `owner5_sensitivity_definitions.csv`
- `owner5_system_impact_summary_annual_sensitivity_2030_2045.csv`
- `owner5_system_impact_summary_period_sensitivity_2030_2045.csv`

Assumptions reconciliation:

- `config/smr_assumptions.csv`
- `docs/decisions/objective3_smr_assumptions_reconciliation.md`

## Final Objective 3 assumptions state

The reconciled SMR assumptions are:

- 3 Rolls-Royce SMRs at Wylfa
- 470 MWe per unit
- net delivery factor: 1.0
- forced outage rate: 0.02
- planned outage window: 18 days every 24 months
- base staggered commissioning:
  - unit 1: 2035-01-01
  - unit 2: 2036-01-01
  - unit 3: 2037-01-01
- Owner 5 stress-test simultaneous commissioning:
  - all units online from 2035-01-01

## Files intentionally not migrated

The following generated/local artefacts remain intentionally uncommitted:

- hourly Objective 3 Parquet outputs
- generated Objective 3 PNG figures
- raw data
- local model artefacts
- `.DS_Store`
- `.Rhistory`
- bytecode/cache files

Specific known generated Objective 3 outputs excluded from Git include:

- `system_impact_metrics_hourly_2030_2045.parquet`
- `grid_master_hourly_2030_2045.parquet`
- `smr_hourly_library_2030_2045.parquet`
- `smr_hourly_fleet_scenarios.parquet`
- Owner 5 generated hourly sensitivity Parquet outputs
- Owner 5 generated PNG figures

## Completion status

Objective 3 migration is complete for the inspected source inventory:

- scaffold/contract: migrated
- Owner 3 data integration: migrated
- Owner 4 summary evidence: migrated
- Owner 5 visualisation/final QA: migrated
- SMR assumptions conflict: reconciled and documented
- static tests: present
- generated artefacts: intentionally excluded

## Update: received Owner/Part 1, 2, and 4 source notebooks

Additional Objective 3 source files were later received and migrated as notebook source evidence:

    notebooks/objective3_smr_integration/task1_smr_unit_library.ipynb
    notebooks/objective3_smr_integration/task2_smr_fleet_scenarios.ipynb
    notebooks/objective3_smr_integration/task4_system_impact_metrics.ipynb

See docs/validation/objective3_smr_integration/missing_source_inventory.md for source-population paths, SHA-256 fingerprints, and the SMR assumptions decision.

This resolves the earlier source-availability gap for Owner/Part 1, Owner/Part 2, and Owner/Part 4 at the evidence/source migration level.

Objective 3 should still not be described as fully rerunnable until these notebooks are adapted to repository paths or converted into scripts and covered by a runner/audit.
