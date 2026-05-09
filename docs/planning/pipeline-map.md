# Pipeline Map

## Core modelling scope

- System boundary: Great Britain only.
- Canonical time key: `timestamp_utc`.
- Core resolution: hourly UTC.
- Historic calibration window: generally 2010-2024.
- Future simulation window: 2030-2045.
- Core FES pathways: `electric_engagement` and `holistic_transition`.
- Core SMR cases: `staggered_commissioning` and `simultaneous_commissioning`.

## Repo organisation

The repo is organised around the main practical sections of the project:

1. `preprocessing`
2. `objective1_generation`
3. `objective2_demand`
4. `objective3_smr_integration`

## Practical run order

Although the repo is organised by objective, the practical build order is:

1. `preprocessing`
2. `objective2_demand`
3. `objective1_generation`
4. `objective3_smr_integration`

Reason: the future demand model defines the scenario table dimensions used in the integrated model.

## Stage 0: preprocessing

Creates cleaned, documented, QA-checked datasets.

Key outputs:

- `demand_hist_hourly.parquet`
- `demand_hist_daily.parquet`
- `demand_hourly_shape_library.parquet`
- `genmix_hist_hourly.parquet`
- `genmix_profile_library.parquet`
- `fes_demand_annual_2030_2045.parquet`
- `fes_supply_annual_2030_2045.parquet`
- `weather_hist_daily.parquet`
- `weather_future_daily_ukcp18.parquet`
- `era5_resource_hourly_gb_2010_2024.parquet`
- `calendar_hourly_2010_2045.parquet`

## Stage 1: Objective 2 demand

Produces:

- `demand_future_hourly_2030_2045/`

Main keys:

- `timestamp_utc`
- `year`
- `fes_scenario`
- `climate_member`

## Stage 2: Objective 1 generation

Produces:

- `generation_future_hourly_2030_2045/`

Main keys:

- `timestamp_utc`
- `year`
- `fes_scenario`

## Stage 3: Objective 3 SMR integration

Produces:

- `smr_fleet_hourly_2030_2045/`
- `grid_master_hourly_2030_2045/`
- `system_impact_summary_annual_2030_2045.csv`
- `system_impact_summary_period_2030_2045.csv`

Main keys:

- `timestamp_utc`
- `year`
- `fes_scenario`
- `climate_member`
- `smr_case`
