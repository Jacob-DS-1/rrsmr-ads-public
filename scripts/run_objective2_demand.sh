#!/usr/bin/env bash
set -euo pipefail

echo "Running Objective 2 demand workflow"

EXECUTED_DIR="${EXECUTED_DIR:-/tmp/rrsmr-ads-objective2-rerun-notebooks}"
KERNEL_NAME="${KERNEL_NAME:-python3}"
echo "Kernel name: $KERNEL_NAME"

if [ "${1:-}" = "--clean" ]; then
  echo "Removing generated Objective 2 outputs"
  rm -rf data/processed/objective2_demand
  rm -rf outputs/objective2_demand
  rm -rf "$EXECUTED_DIR"
fi

mkdir -p "$EXECUTED_DIR"

notebooks=(
  "notebooks/objective2_demand/task1_demand_model_training_data.ipynb"
  "notebooks/objective2_demand/task2_weather_feature_engineering.ipynb"
  "notebooks/objective2_demand/task3_demand_model_integration.ipynb"
  "notebooks/objective2_demand/task4_ukcp18_member_selection.ipynb"
  "notebooks/objective2_demand/task5_future_daily_climate_demand.ipynb"
  "notebooks/objective2_demand/task6_fes_annual_anchoring.ipynb"
  "notebooks/objective2_demand/task7_hourly_disaggregation.ipynb"
  "notebooks/objective2_demand/task8_final_demand_qa_export.ipynb"
)

for notebook in "${notebooks[@]}"; do
  if [ ! -f "$notebook" ]; then
    echo "Missing Objective 2 notebook: $notebook" >&2
    exit 1
  fi
done

for notebook in "${notebooks[@]}"; do
  echo
  echo "---- executing $notebook ----"
  jupyter nbconvert \
    --to notebook \
    --execute "$notebook" \
    --output-dir "$EXECUTED_DIR" \
    --ExecutePreprocessor.kernel_name="$KERNEL_NAME"
done

echo
echo "Objective 2 complete"
echo "Executed notebooks written to:"
echo "  $EXECUTED_DIR"
echo "Primary output:"
echo "  data/processed/objective2_demand/demand_future_hourly_2030_2045.parquet"
