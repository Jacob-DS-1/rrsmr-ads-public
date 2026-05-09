# Objective 3 Master README

## Purpose

Objective 3 integrates three Wylfa SMRs into the GB future electricity system
modelling chain, then estimates system impact metrics and presents final
visualisations.

## Main Model and Sensitivity Scope

The main Objective 3 model uses:

```text
weather_year_role = average_wind
smr_case = staggered_commissioning
```

The project separates the climate and wind-resource dimensions:

- `climate_member` captures Objective 2 demand-side climate uncertainty.
- `weather_year_role` captures Objective 1 supply-side wind/solar resource
  uncertainty.

These dimensions are intentionally not collapsed into one key. Owner 3 keeps
them separate in the integrated master dataset.

For Owner 5, sensitivities are stress tests rather than new base models. The
`simultaneous_commissioning` case is constructed for final robustness testing:

```text
all three SMR units online from 2035-01-01 00:00:00+00:00
```

It uses the same nameplate capacity, net delivery factor, planned outage, and
forced outage assumptions as the delivered staggered case.

## Objective 3 Folder Structure

```text
ADS repo Objective 3 paths/
├── config/smr_assumptions.csv
├── data/processed/objective3_smr_integration/          # generated local data, not committed
├── outputs/objective3_smr_integration/                 # generated local outputs, not committed
├── outputs/figures/objective3_smr_integration/          # generated local figures, not committed
└── docs/validation/objective3_smr_integration/          # committed validation evidence
```

## Key Deliverables

### Owner 1

- `smr_assumptions.csv`
- `smr_hourly_library_2030_2045.parquet`

### Owner 2

- `smr_fleet_hourly_2030_2045.parquet`
- `smr_hourly_fleet_scenarios.parquet`

### Owner 3

- `grid_master_hourly_2030_2045.parquet`
- `grid_master_hourly_2030_2045_partitioned/`
- `grid_master_schema.csv`
- Owner 3 QA logs

### Owner 4

- `system_impact_metrics_hourly_2030_2045.parquet`
- `system_impact_summary_annual_2030_2045.csv`
- `system_impact_summary_period_2030_2045.csv`

### Owner 5

- `Outputs/system_impact_metrics_hourly_owner5_sensitivity_2030_2045.parquet`
- `Outputs/system_impact_summary_annual_owner5_sensitivity_2030_2045.csv`
- `Outputs/system_impact_summary_period_owner5_sensitivity_2030_2045.csv`
- `Outputs/owner5_sensitivity_definitions.csv`
- `Outputs/low_wind_case_study_pressure_day.csv`
- `Outputs/low_wind_case_study_selection_rankings.csv`
- `Figures/`
- `QA/QA_reconciliation_report.md`
- `Objective_3_Master_README.md`

## Metric Logic

Owner 4 and Owner 5 use the following system-balance logic:

```text
residual_before_smr_mw = demand_mw - exogenous_supply_mw - imports_net_baseline_mw
residual_after_smr_mw = residual_before_smr_mw - smr_total_delivered_mw
gas_needed_before_mw = max(residual_before_smr_mw, 0)
gas_needed_after_mw = max(residual_after_smr_mw, 0)
gas_displacement_proxy_mw = gas_needed_before_mw - gas_needed_after_mw
surplus_after_smr_mw = max(-residual_after_smr_mw, 0)
```

Energy conversions assume hourly MW values:

```text
MWh = MW for one hourly row
TWh = MWh / 1,000,000
```

## Low-wind Treatment

The primary model uses `average_wind`. Low-wind sensitivity charts use
`weather_year_role == low_wind` to show SMR support during a stressed supply
condition. The Owner 4 low-wind flag threshold inferred from the delivered
data is:

```text
wind_mw <= 12085.499343
```

## Graphical Assets

- `residual_demand_comparison_average_wind`: `outputs/figures/objective3_smr_integration/residual_demand_comparison_average_wind.png`
- `annual_smr_energy_gas_displacement_trends`: `outputs/figures/objective3_smr_integration/annual_smr_energy_gas_displacement_trends.png`
- `net_load_duration_curves_rollout_sensitivity_2036`: `outputs/figures/objective3_smr_integration/net_load_duration_curves_rollout_sensitivity_2036.png`
- `low_wind_case_study_pressure_day`: `outputs/figures/objective3_smr_integration/low_wind_case_study_pressure_day.png`
- `smr_case_stress_test_comparison`: `outputs/figures/objective3_smr_integration/smr_case_stress_test_comparison.png`

## Reproducible Run Command

From the repository root:

```bash
python src/rrsmr_ads/objective3_smr_integration/owner5_visualisations_final_qa.py --build
```

## QA Summary

Final QA is documented in:

```text
QA/QA_reconciliation_report.md
```

The final package passes row-count, duplicate-key, missing-value, timestamp,
Owner 4 recalculation alignment, weather-role coverage, and figure-output
checks. Owner 4's delivered metrics contain the base `staggered_commissioning`
case; Owner 5 transparently adds `simultaneous_commissioning` as a stress-test
sensitivity.
