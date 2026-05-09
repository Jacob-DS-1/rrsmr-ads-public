# Reproducible preprocessing workflow

This document describes the shared preprocessing entry point for rebuilding cleaned inputs used by Objective 1, Objective 2, and Objective 3.

The preprocessing layer converts local/raw source files into canonical ignored outputs under:

    data/processed/preprocessing/

Generated preprocessing outputs are not committed to git.

## Main command

Run from the repository root:

    scripts/run_preprocessing.sh --source-dir /path/to/source/raw

If ERA5 NetCDFs and the REPD workbook are stored separately, pass them with:

    scripts/run_preprocessing.sh --source-dir /path/to/source/raw --era5-source-dir /path/to/era5

For the current local source-population bundle, the source directory is usually:

    external_data/source_inputs/objective1_raw

To check whether the declared raw inputs are present without executing notebooks:

    scripts/run_preprocessing.sh --source-dir /path/to/source/raw --era5-source-dir /path/to/era5 --check-only

To remove staged work, executed notebooks, and canonical preprocessing outputs before rerunning:

    scripts/run_preprocessing.sh --source-dir /path/to/source/raw --clean

Executed notebook copies are written outside the repo by default:

    /tmp/rrsmr-ads-preprocessing-executed-notebooks

Staged notebook work directories are written outside the repo by default:

    /tmp/rrsmr-ads-preprocessing-work

## Current source contract

The runner expects raw inputs for these stages:

1. Holiday calendar
   - GOV.UK bank-holidays JSON.

2. Hourly calendar
   - Uses the generated holiday calendar from stage 1.

3. Historic demand
   - NESO historic demand annual CSVs named demanddata_YYYY.csv.
   - Builds hourly, daily, and shape-library demand outputs.

4. Historic generation mix
   - NESO df_fuel_ckan.csv.
   - Builds hourly generation mix and profile-library outputs.

5. DUKES reference tables
   - DUKES 5.7, 5.10, 5.13, 6.2, and 6.3 workbooks.

6. FES annual tables
   - FES 2025 ES1 supply CSV.
   - FES 2025 ED1/demand CSV.

7. Weather tables
   - HadUK tasmax and tasmin NetCDFs.
   - UKCP18 regional future temperature CSVs.

8. ERA5 resource table
   - ERA5 100m u-component wind NetCDFs.
   - ERA5 100m v-component wind NetCDFs.
   - ERA5 surface solar radiation NetCDFs.
   - REPD workbook.

## Canonical outputs verified by the runner

The runner verifies that these files exist after a successful run:

    data/processed/preprocessing/demand_hist_hourly.parquet
    data/processed/preprocessing/demand_hist_daily.parquet
    data/processed/preprocessing/demand_hourly_shape_library.parquet
    data/processed/preprocessing/genmix_hist_hourly.parquet
    data/processed/preprocessing/genmix_profile_library.parquet
    data/processed/preprocessing/fes_demand_annual_2030_2045.parquet
    data/processed/preprocessing/fes_supply_annual_2030_2045.parquet
    data/processed/preprocessing/weather_hist_daily.parquet
    data/processed/preprocessing/weather_future_daily_ukcp18.parquet
    data/processed/preprocessing/era5_resource_hourly_gb_2010_2024.parquet
    data/processed/preprocessing/calendar_hourly_2010_2045.parquet
    data/processed/preprocessing/dukes_capacity_hist_2010_2024.parquet
    data/processed/preprocessing/dukes_loadfactor_hist_2010_2024.parquet
    data/processed/preprocessing/interconnector_annual_hist_2010_2024.parquet
    data/processed/preprocessing/holiday_calendar_2010_2045.csv

## Validated local run

A full preprocessing rebuild was validated with the source-population raw bundle plus a separate local ERA5 folder.

Validated command shape:

    scripts/run_preprocessing.sh --source-dir /path/to/source/raw --era5-source-dir /path/to/era5

For the current local setup, the two source roots are:

    external_data/source_inputs/objective1_raw
    external_data/source_inputs/ERA5

The split source layout matters because the source-population raw bundle contains most preprocessing inputs, while the local ERA5 folder contains the complete ERA5 wind files, surface solar radiation files, and `repd.xlsx`.

The runner still supports bridge mode for future machines where a stage's raw inputs are unavailable but the ignored canonical output already exists locally:

    scripts/run_preprocessing.sh --source-dir /path/to/source/raw --allow-existing-outputs

Bridge mode should be treated as a temporary audit aid, not the preferred workflow.

## Relationship to the Objective 1 restore helper

The older helper:

    scripts/restore_objective1_preprocessing_inputs.sh external_data/source_inputs

only restores the seven Objective 1 prerequisite artifacts from a migrated handoff folder.

The preprocessing runner is broader. It is intended to rebuild or verify shared cleaned preprocessing inputs for the whole workflow, including demand, weather, calendar, FES, DUKES, generation mix, and ERA5 resource tables.
