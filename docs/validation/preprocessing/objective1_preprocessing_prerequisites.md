# Objective 1 preprocessing prerequisites

Objective 1 generation scripts are rerunnable from repository source once their ignored preprocessing inputs exist locally under:

- data/processed/preprocessing/

These files are generated/model data artifacts and must not be committed.

## Required files

Objective 1 currently requires the following preprocessing artifacts:

- genmix_hist_hourly.parquet
- era5_resource_hourly_gb_2010_2024.parquet
- dukes_capacity_hist_2010_2024.parquet
- dukes_loadfactor_hist_2010_2024.parquet
- genmix_profile_library.parquet
- fes_supply_annual_2030_2045.parquet
- interconnector_annual_hist_2010_2024.parquet

## Local restore command

For the current migrated evidence bundle, restore the files with:

    scripts/restore_objective1_preprocessing_inputs.sh external_data/source_inputs

Then rerun Objective 1:

    rm -rf outputs/objective1_generation
    rm -rf data/processed/objective1_generation

    for script in \
      src/task1_prep_and_calibration.py \
      src/task2_ml_training.py \
      src/task3_baseline_and_weather_scaffold.py \
      src/task4_weather_adjustment.py \
      src/task5_fes_anchoring_and_export.py
    do
      PYTHONHASHSEED=0 python "$script"
    done

## Reproducibility note

The restore helper is an Objective 1 bridge for restoring ignored migrated preprocessing evidence.

The broader preprocessing workflow is now documented at:

- docs/reproducibility/preprocessing_workflow.md

Preferred route:

    scripts/run_preprocessing.sh --source-dir /path/to/source/raw

The runner rebuilds or verifies the shared cleaned preprocessing layer across demand, generation mix, DUKES, FES, weather, calendar, and ERA5 resource stages. If a raw source input is missing, it fails with a stage-specific missing-input message instead of leaving the reproducibility gap vague.
