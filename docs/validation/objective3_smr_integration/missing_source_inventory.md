# Objective 3 Missing Source Inventory

This document records the Objective 3 source files received after the initial migration.

Generated parquet, image, cache, and operating-system artifacts remain excluded from version control.

## Migrated source notebooks

| Component | Source population path | Repository path | Source SHA-256 | Notes |
|---|---|---|---|---|
| Owner/Part 1 | `files/part1 SMR Logic & Unit Library Generation/object3_part1.ipynb` | `notebooks/objective3_smr_integration/task1_smr_unit_library.ipynb` | `03a4119a84fa7b42d33f7ff8d0ad279a2c74663230409c96d4ab185d161227b5` | SMR assumptions and unit-level hourly SMR library generation source notebook. |
| Owner/Part 2 | `files/part2 SMR Fleet Scenario Expansion/object3_part2.ipynb` | `notebooks/objective3_smr_integration/task2_smr_fleet_scenarios.ipynb` | `9b994ba580dc7fa38776d5c3ed0bb86d5cebc04247a3ebd593f3eb2723980e96` | Fleet-level SMR scenario expansion source notebook. |
| Owner/Part 4 | `files/part4 System Impact Metrics Calculation/object3_part4.ipynb` | `notebooks/objective3_smr_integration/task4_system_impact_metrics.ipynb` | `fae6dd8aa044546fe068e3a20d54266061dc3857f70e656189145b63580e1fac` | System impact metrics calculation source notebook. |

## SMR assumptions decision

The received Part 1 source folder also contained:

    files/part1 SMR Logic & Unit Library Generation/smr_assumptions.csv

Source SHA-256:

    8e88d84139e208d11a2e412d64fa727bf6e55099cbb27409e788cc655d1f7741

This file was inspected but not copied over `config/smr_assumptions.csv`.

Reason: the repository config is the reconciled source of truth. It preserves the original staggered commissioning base case and also includes the simultaneous commissioning stress-test case used by the migrated Owner 5 sensitivity logic. The received Part 1 assumptions file contains only the original staggered base case and lacks the repository-only reconciliation fields `unit_name` and `notes`.

## Current migration interpretation

Owner/Part 1, Owner/Part 2, and Owner/Part 4 source logic is now available as notebooks and has been migrated as source evidence.

Owner/Part 3 and Owner/Part 5 source logic had already been migrated as Python scripts.

This resolves the earlier source-availability gap for Owner/Part 1, Owner/Part 2, and Owner/Part 4 at the evidence/source migration level.

This PR does not claim that Objective 3 is fully rerunnable from repository paths yet. The next step is to adapt the migrated notebooks or convert them into repository-path-aware scripts, then add an Objective 3 runner and output audit.

## Notebook path observation

The received notebooks use local relative filenames such as `smr_assumptions.csv`, `smr_hourly_library_2030_2045.parquet`, `smr_hourly_fleet_scenarios.parquet`, and `grid_master_hourly_2030_2045.parquet`. These paths are preserved as source evidence in this PR. A later implementation PR should replace them with repository paths from `config/paths.yaml` or another explicit project-root resolver.
