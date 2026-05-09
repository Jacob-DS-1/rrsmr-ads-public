# Objective 3 Owner 5 Final QA and Visualisation Runner

This document records the repository-path-aware runner for Objective 3 Owner/Part 5 final QA and visualisation packaging.

The runner is:

    scripts/run_objective3_final_qa.sh

## Required upstream outputs

Run these first if the system-impact outputs are missing:

    scripts/run_objective3_integration.sh

    scripts/run_objective3_system_impact.sh --clean

Owner 5 consumes repo-generated Owner/Part 4 outputs:

    data/processed/objective3_smr_integration/system_impact_hourly_2030_2045
    data/processed/objective3_smr_integration/system_impact_summary_annual_2030_2045.csv
    data/processed/objective3_smr_integration/system_impact_summary_period_2030_2045.csv

## Command

    scripts/run_objective3_final_qa.sh --clean

## Generated outputs

Owner 5 writes final QA and packaging evidence under:

    docs/validation/objective3_smr_integration

It writes generated final visualisation/package outputs under ignored output folders:

    outputs/objective3_smr_integration
    outputs/figures/objective3_smr_integration

Generated PNG and Parquet outputs should not be committed.

## Scope

This runner covers Objective 3 Owner/Part 5 visualisation and final QA packaging. It uses repo-generated Part 4 system-impact metrics rather than extracting the old Owner 4 ZIP handoff.
