#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

RUN_SMR_FLEET=1
integration_args=()
integration_args_count=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-smr-fleet)
      RUN_SMR_FLEET=0
      shift
      ;;
    -h|--help)
      cat <<'USAGE'
Usage:
  scripts/run_objective3_integration.sh [--skip-smr-fleet]

Options:
  --skip-smr-fleet  Do not rerun the SMR fleet step before integration.
USAGE
      exit 0
      ;;
    *)
      integration_args+=("$1")
      integration_args_count=$((integration_args_count + 1))
      shift
      ;;
  esac
done

if [[ "$RUN_SMR_FLEET" -eq 1 ]]; then
  scripts/run_objective3_smr_fleet.sh --clean
fi

if [[ "$integration_args_count" -gt 0 ]]; then
  python src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py --integrate "${integration_args[@]}"
else
  python src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py --integrate
fi
