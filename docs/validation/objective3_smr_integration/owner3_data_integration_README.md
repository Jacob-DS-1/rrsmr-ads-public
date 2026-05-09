# Objective 3 Owner 3 - Data Integration & Consistency QA

## Purpose

This folder contains the Owner 3 delivery package for Objective 3. Owner 3 is
responsible for validating alignment across Objective 1 supply, Objective 2
demand, and the Owner 2 SMR fleet table, then building the integrated hourly
master dataset for 2030-2045.

## Current Project Status

Owner 3 integration is now complete using the team-agreed rule to retain both
uncertainty dimensions:

```text
timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case
```

This means Objective 2 climate-member demand uncertainty and Objective 1
supply weather-year uncertainty are both carried into the final master table.

## Inputs Used

- Objective 1 final supply:
  `data/processed/objective1_generation/generation_future_hourly_2030_2045`
- Objective 2 final demand:
  `data/processed/objective2_demand/demand_future_hourly_2030_2045`
- Owner 2 SMR fleet:
  `data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045`

Owner 2's supplied column names were adapted as follows:

| Supplied column | Owner 3 standard column |
|---|---|
| `unit_1_mw` | `unit1_delivered_mw` |
| `unit_2_mw` | `unit2_delivered_mw` |
| `unit_3_mw` | `unit3_delivered_mw` |
| `total_fleet_mw` | `smr_total_delivered_mw` |

## Folder Structure

```text
Owner 3 Data Integration/
├── Code/
│   └── owner3_data_integration.py
├── Inputs/
│   └── input_manifest.csv
├── Outputs/
│   ├── grid_master_hourly_2030_2045.parquet
│   ├── grid_master_hourly_2030_2045_partitioned/
│   ├── grid_master_schema.csv
│   └── integration_summary.csv
├── QA/
│   ├── input_alignment_qa_log.md
│   ├── owner3_decision_register.csv
│   ├── duplicate_key_check.csv
│   ├── missing_value_check.csv
│   ├── row_count_reconciliation.csv
│   ├── scenario_coverage_check.csv
│   └── timestamp_coverage_check.csv
└── owner3_data_integration_README.md
```

## Reproducible Run Command

The final integration was generated with:

```bash
python src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py --integrate
```

Use the project runtime environment defined by `requirements.txt`.

## Integration Logic

Source keys:

```text
demand:     timestamp_utc + year + fes_scenario + climate_member
generation: timestamp_utc + year + fes_scenario + weather_year_role
smr:        timestamp_utc + year + fes_scenario + smr_case
```

Owner 3 first builds a full scenario skeleton:

```text
timestamps
× fes_scenario
× climate_member
× weather_year_role
× smr_case
```

Then it left-joins demand, generation, and SMR fleet data onto that skeleton
using each source table's native key.

## Final Output

Main output:

```text
data/processed/objective3_smr_integration/grid_master_hourly_2030_2045
```

Performance-oriented partitioned output:

```text
data/processed/objective3_smr_integration/grid_master_hourly_2030_2045_partitioned/
```

The partitioned dataset is partitioned by:

```text
year + fes_scenario + smr_case
```

Final row count:

```text
2,524,608 rows
```

The row count is:

```text
140,256 timestamps × 2 FES scenarios × 3 climate members × 3 weather-year roles × 1 SMR case
```

Final key:

```text
timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case
```

## Final Coverage

- Start: `2030-01-01 00:00 UTC`
- End: `2045-12-31 23:00 UTC`
- FES scenarios: `Electric Engagement`, `Holistic Transition`
- Climate members: `member_06`, `member_12`, `member_13`
- Weather-year roles: `average_wind`, `high_wind`, `low_wind`
- SMR cases: `staggered_commissioning`

## Core Columns

The final master table includes:

```text
timestamp_utc
year
fes_scenario
climate_member
weather_year_role
weather_year
smr_case
demand_mw
wind_mw
solar_mw
nuclear_existing_mw
biomass_mw
hydro_mw
other_mw
imports_net_baseline_mw
unit1_delivered_mw
unit2_delivered_mw
unit3_delivered_mw
smr_total_delivered_mw
exogenous_supply_mw
storage_net_mw
gas_reference_mw
coal_reference_mw
```

`exogenous_supply_mw` is calculated as:

```text
wind_mw + solar_mw + nuclear_existing_mw + biomass_mw + hydro_mw + other_mw
```

`gas_reference_mw` is retained only as a diagnostic reference from Objective
1. It is not the final gas dispatch result. Owner 4 should calculate
`gas_needed_before_mw`, `gas_needed_after_mw`, and gas displacement metrics
from this master dataset.

## QA Summary

The integration passed these checks:

- Full timestamp coverage for all source tables.
- No duplicate source keys.
- No duplicate final master keys.
- No missing values in required final columns.
- SMR unit columns reconcile to `smr_total_delivered_mw`.
- Final row count matches the full scenario skeleton.

Detailed QA files are in `QA/`.

## Known Limitations

- Owner 2 currently supplies only one `smr_case`: `staggered_commissioning`.
- If a simultaneous commissioning sensitivity case is later added, rerun the
  script; the master row count will expand automatically.
- The model is a GB system-balance integration dataset, not a network-
  constrained transmission model.
