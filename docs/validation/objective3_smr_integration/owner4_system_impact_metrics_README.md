# Objective 3 Owner 4 system impact summary evidence

## Purpose

This folder contains the migrated Owner 4 summary evidence for Objective 3 system impact metrics.

Owner 4 calculated the system-impact summary metrics from the integrated Objective 3 hourly grid master and SMR outputs. The available source inventory contains the delivered hourly Parquet output and two small CSV summary tables. This migration commits only the small CSV summary evidence.

## Migrated files

| File | Description | Classification |
|---|---|---|
| `owner4_system_impact_summary_annual_2030_2045.csv` | Annual Owner 4 system-impact summary by year, FES scenario, climate member, weather-year role, and SMR case | validation evidence |
| `owner4_system_impact_summary_period_2030_2045.csv` | Whole-period Owner 4 system-impact summary by FES scenario and SMR case | validation evidence |

## Source inventory

Source directory inspected:

`local Objective 3 source inventory / part4 System Impact Metrics Calculation`

The source inventory contained:

- `system_impact_metrics_hourly_2030_2045.parquet`
- `system_impact_summary_annual_2030_2045.csv`
- `system_impact_summary_period_2030_2045.csv`

No Owner 4 source script or notebook was present in the inspected source inventory.

## Files intentionally not committed

`system_impact_metrics_hourly_2030_2045.parquet` is a generated hourly output and is intentionally not committed.

This PR does not recreate Owner 4 modelling logic. It is a validation-evidence migration only.

## Evidence summary

Annual summary:

- rows: 96
- years: 2030-2045
- FES scenarios: Electric Engagement, Holistic Transition
- climate members: member_06, member_12, member_13
- weather-year role: average_wind
- SMR case: staggered_commissioning
- missing values in required columns: 0

Period summary:

- rows: 2
- FES scenarios: Electric Engagement, Holistic Transition
- SMR case: staggered_commissioning
- missing values in required columns: 0

## Scope note

This is a migration PR only.

Included:

- Owner 4 annual summary CSV evidence
- Owner 4 period summary CSV evidence
- README documenting the source inventory and exclusions
- static tests covering schema, coverage, and missing-value expectations

Not included:

- no Objective 3 modelling implementation
- no Owner 4 script recreation
- no hourly Parquet output
- no PNG figures
- no raw data or local artefacts
