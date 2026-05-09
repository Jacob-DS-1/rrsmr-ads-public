#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

show_usage() {
  cat <<'USAGE'
Usage:
  scripts/run_complete_model.sh --source-dir PATH [--era5-source-dir PATH] [options]

Options:
  --clean                 Remove generated outputs before rerunning supported stages.
  --skip-preprocessing    Do not run preprocessing; assume data/processed/preprocessing exists.
  --source-dir PATH       Raw/source preprocessing bundle.
  --era5-source-dir PATH  Separate ERA5 source folder, if used by preprocessing.
  --kernel-name NAME      Jupyter kernel for notebook-based stages. Defaults to KERNEL_NAME or python3.
  -h, --help              Show this help.

Examples:
  scripts/run_complete_model.sh \
    --source-dir "external_data/source_inputs/objective1_raw" \
    --era5-source-dir "/path/to/ERA5"

  scripts/run_complete_model.sh \
    --clean \
    --source-dir "external_data/source_inputs/objective1_raw" \
    --era5-source-dir "/path/to/ERA5"

  scripts/run_complete_model.sh --skip-preprocessing --clean
USAGE
}

CLEAN=0
SKIP_PREPROCESSING=0
SOURCE_DIR=""
ERA5_SOURCE_DIR=""
KERNEL_NAME_VALUE="${KERNEL_NAME:-python3}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean)
      CLEAN=1
      shift
      ;;
    --skip-preprocessing)
      SKIP_PREPROCESSING=1
      shift
      ;;
    --source-dir)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --source-dir" >&2
        exit 2
      fi
      SOURCE_DIR="$2"
      shift 2
      ;;
    --era5-source-dir)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --era5-source-dir" >&2
        exit 2
      fi
      ERA5_SOURCE_DIR="$2"
      shift 2
      ;;
    --kernel-name)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --kernel-name" >&2
        exit 2
      fi
      KERNEL_NAME_VALUE="$2"
      shift 2
      ;;
    -h|--help)
      show_usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      show_usage >&2
      exit 2
      ;;
  esac
done

if [[ "$SKIP_PREPROCESSING" -eq 0 && -z "$SOURCE_DIR" ]]; then
  echo "Missing required --source-dir unless --skip-preprocessing is used." >&2
  show_usage >&2
  exit 2
fi

export KERNEL_NAME="$KERNEL_NAME_VALUE"
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"

section() {
  echo
  echo "== $1 =="
}

run_step() {
  echo "+ $*"
  "$@"
}

clean_objective3_outputs() {
  rm -rf \
    data/processed/objective3_smr_integration/smr_hourly_library_2030_2045 \
    data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045 \
    data/processed/objective3_smr_integration/smr_hourly_fleet_scenarios.parquet \
    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045 \
    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045.parquet \
    data/processed/objective3_smr_integration/grid_master_hourly_2030_2045_partitioned \
    data/processed/objective3_smr_integration/system_impact_hourly_2030_2045 \
    data/processed/objective3_smr_integration/system_impact_metrics_hourly_2030_2045.parquet \
    data/processed/objective3_smr_integration/system_impact_summary_annual_2030_2045.csv \
    data/processed/objective3_smr_integration/system_impact_summary_period_2030_2045.csv \
    outputs/objective3_smr_integration \
    outputs/figures/objective3_smr_integration
}

run_cleanable_step() {
  if [[ "$CLEAN" -eq 1 ]]; then
    run_step "$1" --clean
  else
    run_step "$1"
  fi
}

section "Complete model workflow"
echo "Kernel name: $KERNEL_NAME"
echo "PYTHONHASHSEED: $PYTHONHASHSEED"

if [[ "$SKIP_PREPROCESSING" -eq 1 ]]; then
  section "Preprocessing"
  echo "Skipping preprocessing; assuming data/processed/preprocessing already exists."
else
  preprocessing_args=()
  if [[ "$CLEAN" -eq 1 ]]; then
    preprocessing_args+=(--clean)
  fi
  preprocessing_args+=(--source-dir "$SOURCE_DIR")
  if [[ -n "$ERA5_SOURCE_DIR" ]]; then
    preprocessing_args+=(--era5-source-dir "$ERA5_SOURCE_DIR")
  fi
  preprocessing_args+=(--kernel-name "$KERNEL_NAME")

  section "Preprocessing"
  run_step scripts/run_preprocessing.sh "${preprocessing_args[@]}"
fi

section "Objective 1 generation"
run_cleanable_step scripts/run_objective1_generation.sh

section "Objective 2 demand"
run_cleanable_step scripts/run_objective2_demand.sh

if [[ "$CLEAN" -eq 1 ]]; then
  section "Objective 3 clean"
  echo "Removing known Objective 3 generated outputs"
  clean_objective3_outputs
fi

section "Objective 3 SMR fleet"
run_cleanable_step scripts/run_objective3_smr_fleet.sh

section "Objective 3 integration"
run_step scripts/run_objective3_integration.sh --skip-smr-fleet

section "Objective 3 system impact"
run_cleanable_step scripts/run_objective3_system_impact.sh

section "Objective 3 final QA"
run_cleanable_step scripts/run_objective3_final_qa.sh

section "Reproducible output audit"
run_step python scripts/audit_reproducible_outputs.py

section "Complete model workflow finished"
