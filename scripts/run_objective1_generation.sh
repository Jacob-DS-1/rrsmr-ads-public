#!/usr/bin/env bash
set -euo pipefail

echo "Running Objective 1 generation workflow"

if [ "${1:-}" = "--clean" ]; then
  echo "Removing generated Objective 1 outputs"
  rm -rf outputs/objective1_generation
  rm -rf data/processed/objective1_generation
fi

required_inputs=(
  "data/processed/preprocessing/genmix_hist_hourly.parquet"
  "data/processed/preprocessing/era5_resource_hourly_gb_2010_2024.parquet"
  "data/processed/preprocessing/dukes_capacity_hist_2010_2024.parquet"
  "data/processed/preprocessing/dukes_loadfactor_hist_2010_2024.parquet"
  "data/processed/preprocessing/genmix_profile_library.parquet"
  "data/processed/preprocessing/fes_supply_annual_2030_2045.parquet"
  "data/processed/preprocessing/interconnector_annual_hist_2010_2024.parquet"
)

for path in "${required_inputs[@]}"; do
  if [ ! -f "$path" ]; then
    echo "Missing required preprocessing input: $path" >&2
    echo "Restore inputs first, for example:" >&2
    echo "  scripts/restore_objective1_preprocessing_inputs.sh external_data/source_inputs" >&2
    exit 1
  fi
done

scripts_to_run=(
  "src/task1_prep_and_calibration.py"
  "src/task2_ml_training.py"
  "src/task3_baseline_and_weather_scaffold.py"
  "src/task4_weather_adjustment.py"
  "src/task5_fes_anchoring_and_export.py"
)

for script in "${scripts_to_run[@]}"; do
  echo
  echo "---- running $script ----"
  PYTHONHASHSEED=0 python "$script"
done

echo
echo "Objective 1 complete"
echo "Primary output:"
echo "  data/processed/objective1_generation/generation_future_hourly_2030_2045.parquet"
