# Objective 3 SMR integration dashboard

This Streamlit dashboard presents the repo-generated Objective 3 system-impact outputs for the DATA70202 Rolls-Royce SMR project.

It is student project material, not official Rolls-Royce SMR branding.

## What the dashboard shows

The app helps users explore:

1. Whole-period SMR impact, 2030-2045.
2. Annual trends in SMR delivered energy and gas-displacement proxy.
3. Hourly before/after residual demand and gas requirement.
4. Residual-load duration curves.
5. Low-wind pressure-day case studies.
6. SMR deployment and availability assumptions.
7. Methodology and quality checks.
8. Downloadable filtered extracts.

The central model view is:

    supply/weather case = Average-wind supply case
    SMR deployment case = Staggered commissioning

The simultaneous commissioning case is a stress-test sensitivity. The high-wind and low-wind supply/weather roles are sensitivity views from the canonical Objective 3 scenario cube.

## Prerequisites

Generate the model outputs first:

    scripts/run_complete_model.sh --kernel-name rrsmr-ads --source-dir /path/to/source/raw --era5-source-dir /path/to/ERA5

Or, if outputs already exist, make sure these paths are present:

    data/processed/objective3_smr_integration/system_impact_hourly_2030_2045
    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045
    data/processed/objective3_smr_integration/system_impact_summary_annual_2030_2045.csv
    data/processed/objective3_smr_integration/system_impact_summary_period_2030_2045.csv
    config/smr_assumptions.csv

## Build dashboard data

From the repository root:

    python dashboard/objective3_smr_integration/scripts/build_dashboard_data.py

This writes generated, ignored dashboard data to:

    outputs/dashboard/objective3_smr_integration/data/

The generated dashboard data is not committed. Rebuild it after rerunning Objective 3.

## Run locally

Install dashboard dependencies if needed:

    pip install -r requirements.txt

Then run:

    streamlit run dashboard/objective3_smr_integration/app.py

The app reads from:

    outputs/dashboard/objective3_smr_integration/data/

To use a different generated data folder:

    RRSMR_DASHBOARD_DATA_DIR=/path/to/data streamlit run dashboard/objective3_smr_integration/app.py

## Access control

For local project review, the app can run without a password.

For sharing, set either:

    DASHBOARD_PASSWORD=your-access-code

or configure `dashboard_password` in Streamlit secrets. Do not commit real secrets.

## Interpretation notes

`gas_displacement_proxy_mw` is a simplified rule-based balancing metric. It estimates the reduction in non-negative residual demand after SMR output is added.

It is not a full dispatch, market, or carbon-emissions model.

`surplus_after_smr_mw` is an oversupply or curtailment-risk proxy.

The final Objective 3 fleet model no longer applies a permanent 0.9 derating to SMR output. Each SMR is modelled as delivering 470 MW when online, with availability losses represented explicitly through an 18-day planned outage every 24 months and a deterministic 2% forced-outage loss.
