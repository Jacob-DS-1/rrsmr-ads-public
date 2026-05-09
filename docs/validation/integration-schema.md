# Integration Schema

This document defines the expected schema for the final integrated modelling tables.

## Common scenario fields

| Column | Description |
|---|---|
| `timestamp_utc` | Canonical hourly UTC timestamp |
| `year` | Calendar year |
| `fes_scenario` | FES scenario label retained in model outputs, e.g. `Electric Engagement` or `Holistic Transition` |
| `climate_member` | UKCP18 climate member or selected climate case |
| `smr_case` | SMR commissioning case |

## Demand future hourly

Expected location:

`data/processed/objective2_demand/demand_future_hourly_2030_2045/`

Required columns:

- `timestamp_utc`
- `year`
- `fes_scenario`
- `climate_member`
- `demand_mw`

## Generation future hourly

Expected location:

`data/processed/objective1_generation/generation_future_hourly_2030_2045/`

Required columns:

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

## SMR fleet hourly

Expected location:

`data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045/`

Required columns:

- `timestamp_utc`
- `year`
- `smr_case`
- `unit1_delivered_mw`
- `unit2_delivered_mw`
- `unit3_delivered_mw`
- `smr_total_delivered_mw`

## Grid master hourly

Expected location:

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


## Migrated Owner 3 integration note

The migrated Owner 3 integration evidence retains both uncertainty dimensions:

- Objective 2 demand climate uncertainty: `climate_member`
- Objective 1 supply weather-year uncertainty: `weather_year_role`

The Owner 3 final integration key is:

`timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case`

The migrated Owner 3 validation evidence is stored in:

`docs/validation/objective3_smr_integration/`
