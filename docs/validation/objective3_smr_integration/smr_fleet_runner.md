# Objective 3 SMR Fleet Runner

This document records the repository-path-aware implementation of Objective 3 Owner/Part 1 and Owner/Part 2 SMR fleet generation.

The runner is:

    scripts/run_objective3_smr_fleet.sh

It executes:

    src/rrsmr_ads/objective3_smr_integration/smr_fleet.py

## Inputs

    config/smr_assumptions.csv

The repository assumptions file is the reconciled source of truth. It includes:

    staggered_commissioning
    simultaneous_commissioning

The original received Part 1 assumptions file contained only the staggered base case, so it is not used by this runner.

## Generated outputs

Canonical generated outputs are written under:

    data/processed/objective3_smr_integration/

The unit-level output is:

    data/processed/objective3_smr_integration/smr_hourly_library_2030_2045

The canonical fleet-level output is:

    data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045

A legacy compatibility copy is also written:

    data/processed/objective3_smr_integration/smr_hourly_fleet_scenarios.parquet

These are generated artifacts and should not be committed.

## Modelling choices

The runner uses hourly UTC timestamps from 2030-01-01 00:00 UTC to 2045-12-31 23:00 UTC.

Unit delivered output is calculated as:

    nameplate_mwe * net_delivery_factor when the unit is available

for hours where the unit is commissioned and available.

Planned outages are represented deterministically as `planned_outage_window` days every `planned_outage_frequency_months` after each unit's commissioning date. Forced outages are also applied deterministically by selecting a reproducible set of otherwise available hours within each year according to `forced_outage_rate`.

Forced outages are applied deterministically from `forced_outage_rate` by selecting a reproducible set of otherwise available hours within each year. This respects the forced-outage assumption without introducing non-deterministic generated outputs.

## Expected output contract

Unit-level output:

    rows: 841536
    unique key: timestamp_utc + smr_case + unit_id
    cases: staggered_commissioning, simultaneous_commissioning
    units: unit_1, unit_2, unit_3

Fleet-level output:

    rows: 561024
    unique key: timestamp_utc + fes_scenario + smr_case
    FES scenarios: Electric Engagement, Holistic Transition
    cases: staggered_commissioning, simultaneous_commissioning

The fleet output includes both legacy/source-style columns and canonical Objective 3 columns:

    unit_1_mw
    unit_2_mw
    unit_3_mw
    total_fleet_mw
    unit1_delivered_mw
    unit2_delivered_mw
    unit3_delivered_mw
    smr_total_delivered_mw

## Run command

    scripts/run_objective3_smr_fleet.sh --clean

