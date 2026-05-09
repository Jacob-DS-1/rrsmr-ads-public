# Objective 3 Owner 3 Integration Runner

This document records the repository-path-aware runner for Objective 3 Owner 3 data integration.

The runner is:

    scripts/run_objective3_integration.sh

It first regenerates the Objective 3 SMR fleet output:

    scripts/run_objective3_smr_fleet.sh --clean

Then it runs Owner 3 integration:

    python src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py --integrate

## Inputs

Required generated upstream model outputs:

    data/processed/objective1_generation/generation_future_hourly_2030_2045
    data/processed/objective2_demand/demand_future_hourly_2030_2045
    data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045

## Generated outputs

Canonical grid master output:

    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045

Legacy compatibility output:

    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045.parquet

Partitioned compatibility output:

    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045_partitioned

These are generated artifacts and should not be committed.

## Expected grid-master key

    timestamp_utc
    year
    fes_scenario
    climate_member
    weather_year_role
    smr_case

## Expected dimensions

    hours: 140256
    FES scenarios: 2
    climate members: 3
    weather-year roles: 3
    SMR cases: 2

Expected rows:

    5049216

This runner covers Objective 3 Owner/Part 1, Owner/Part 2, and Owner/Part 3. It does not yet run Owner/Part 4 system impact metrics or Owner/Part 5 visualisation/QA outputs.
