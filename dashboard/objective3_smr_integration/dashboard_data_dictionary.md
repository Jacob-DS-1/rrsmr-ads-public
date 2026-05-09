# Dashboard data dictionary

The dashboard uses compact generated files built from canonical Objective 3 repo outputs.

Build command:

    python dashboard/objective3_smr_integration/scripts/build_dashboard_data.py

Generated data directory:

    outputs/dashboard/objective3_smr_integration/data/

## hourly_metrics_dashboard.parquet

Source:

    data/processed/objective3_smr_integration/system_impact_hourly_2030_2045
    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045

Granularity: hourly rows for 2030-2045 by FES scenario, climate member, weather-year role, and SMR case.

Expected weather roles:

    average_wind
    high_wind
    low_wind

Key columns:

- `timestamp_utc`: hourly UTC timestamp.
- `year`: calendar year.
- `fes_scenario`: FES pathway.
- `climate_member`: demand-side UKCP18 climate member.
- `weather_year_role`: supply-side weather role.
- `smr_case`: staggered commissioning or simultaneous commissioning.
- `demand_mw`: projected demand in MW.
- `wind_mw`: projected wind generation in MW.
- `unit1_delivered_mw`, `unit2_delivered_mw`, `unit3_delivered_mw`: SMR unit delivered output in MW.
- `smr_total_delivered_mw`: total SMR fleet delivered output in MW.
- `residual_before_smr_mw`: residual demand before SMR output is added.
- `residual_after_smr_mw`: residual demand after SMR output is added.
- `gas_needed_before_mw`: non-negative gas or balancing requirement before SMR.
- `gas_needed_after_mw`: non-negative gas or balancing requirement after SMR.
- `gas_displacement_proxy_mw`: gas-needed reduction after adding SMR output.
- `surplus_after_smr_mw`: oversupply or curtailment-risk proxy after SMR.
- `low_wind_flag`: low-wind indicator from Objective 3.
- `low_wind_support_flag`: low-wind support indicator from Objective 3.

## Summary CSVs

- `annual_summary.csv`: annual metrics by year, FES scenario, climate member, weather role, and SMR case.
- `period_summary.csv`: whole-period 2030-2045 metrics by FES scenario, climate member, weather role, and SMR case.
- `low_wind_case_study_selection_rankings.csv`: ranked low-wind pressure days.
- `low_wind_case_study_pressure_day.csv`: selected low-wind pressure-day hourly rows.
- `qa_checks.csv`: dashboard-facing build and coverage checks.
- `sensitivity_definitions.csv`: definitions of main model and sensitivity views.
- `smr_assumptions.csv`: current SMR assumptions copied from `config/smr_assumptions.csv`.

## Notes

Hourly MW values are converted to MWh by summing hourly rows. TWh = MWh / 1,000,000.

The simultaneous commissioning case is a stress-test sensitivity.

The main model view is average-wind supply case plus staggered commissioning.
