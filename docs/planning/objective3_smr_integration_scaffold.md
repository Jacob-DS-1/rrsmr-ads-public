# Objective 3 SMR Integration Scaffold

## Purpose

Objective 3 integrates future GB demand, future GB supply, and a 3-unit Rolls-Royce SMR fleet at Wylfa into an hourly system-impact model for 2030-2045.

This scaffold fixes the expected inputs, assumptions, outputs, and validation checks before implementation work begins.

## Scope

Objective 3 covers:

- 3 Rolls-Royce SMRs at Wylfa.
- 470 MWe nameplate capacity per unit.
- GB electricity system only.
- Hourly UTC modelling using `timestamp_utc`.
- Future years 2030-2045.
- FES scenarios:
  - `Electric Engagement`
  - `Holistic Transition`
- Demand input from Objective 2.
- Supply input from Objective 1.
- Rule-based residual-demand and balancing calculations.

Objective 3 does not model Northern Ireland.

## Input contracts

### Future demand

Expected path:

`data/processed/objective2_demand/demand_future_hourly_2030_2045/`

Expected key columns:

- `timestamp_utc`
- `year`
- `fes_scenario`
- `climate_member`
- `demand_mw`

### Future generation

Expected path:

`data/processed/objective1_generation/generation_future_hourly_2030_2045/`

Expected key columns:

- `timestamp_utc`
- `year`
- `fes_scenario`
- `wind_mw`
- `solar_mw`
- `nuclear_existing_mw`
- `biomass_mw`
- `hydro_mw`
- `imports_net_baseline_mw`
- `other_mw`

### SMR assumptions

Expected config:

`config/smr_assumptions.csv`

The current scaffold includes two cases:

- `staggered_commissioning`
- `simultaneous_commissioning`

Each case should contain exactly three Wylfa SMR units at 470 MWe each.

## Output contracts

### SMR fleet hourly

Expected path:

`data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045/`

Required columns:

- `timestamp_utc`
- `year`
- `smr_case`
- `unit1_delivered_mw`
- `unit2_delivered_mw`
- `unit3_delivered_mw`
- `smr_total_delivered_mw`

### Grid master hourly

Expected path:

`data/processed/objective3_smr_integration/grid_master_hourly_2030_2045/`

Required columns:

- `timestamp_utc`
- `year`
- `fes_scenario`
- `climate_member`
- `smr_case`
- `demand_mw`
- `wind_mw`
- `solar_mw`
- `nuclear_existing_mw`
- `biomass_mw`
- `hydro_mw`
- `imports_net_baseline_mw`
- `other_mw`
- `smr_total_delivered_mw`
- `residual_before_smr_mw`
- `residual_after_smr_mw`
- `gas_needed_before_mw`
- `gas_needed_after_mw`
- `gas_displacement_proxy_mw`
- `surplus_after_smr_mw`

### Summary outputs

Expected summary tables:

- `outputs/tables/system_impact_summary_annual_2030_2045.csv`
- `outputs/tables/system_impact_summary_period_2030_2045.csv`

These are generated outputs and should not be committed unless intentionally reduced to small validation evidence in a later PR.

## Rule-based balancing outline

For each hour:

1. Join future demand and future generation through the scenario skeleton, retaining Objective 2 `climate_member` and Objective 1 `weather_year_role`. Generation is keyed by `timestamp_utc`, `year`, `fes_scenario`, and `weather_year_role`; demand is keyed by `timestamp_utc`, `year`, `fes_scenario`, and `climate_member`.
2. Expand by `smr_case`.
3. Join the SMR fleet profile by `timestamp_utc`, `year`, and `smr_case`.
4. Calculate exogenous supply from wind, solar, existing nuclear, biomass, hydro, imports baseline, and other supply.
5. Calculate residual demand before SMR.
6. Subtract SMR delivered output.
7. Use gas as the simplified balancing source.
8. Record remaining negative residual as a surplus or curtailment proxy.

Core residual fields:

- `residual_before_smr_mw`
- `residual_after_smr_mw`
- `gas_needed_before_mw`
- `gas_needed_after_mw`
- `gas_displacement_proxy_mw`
- `surplus_after_smr_mw`

## Out of scope for this scaffold PR

This PR should not:

- execute notebooks
- create generated Parquet outputs
- commit model artefacts
- refactor Objective 1 or Objective 2 logic
- implement the full Objective 3 runtime pipeline

## Validation expectations

Static validation should confirm:

- Objective 3 config paths exist in `config/paths.yaml`
- SMR cases exist in `config/scenarios.yaml`
- `config/smr_assumptions.csv` contains 3 units per case
- each SMR unit has 470 MWe nameplate capacity
- integration schema documents required Objective 3 outputs
- generated Objective 3 data paths remain ignored
- no forbidden generated artefacts are tracked by Git
