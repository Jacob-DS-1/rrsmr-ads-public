#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

CANONICAL_GRID_MASTER="data/processed/objective3_smr_integration/grid_master_hourly_2030_2045"
LEGACY_GRID_MASTER="data/processed/objective3_smr_integration/grid_master_hourly_2030_2045.parquet"

if [[ ! -e "$CANONICAL_GRID_MASTER" && ! -e "$LEGACY_GRID_MASTER" ]]; then
  echo "Missing Objective 3 grid master input."
  echo "Expected one of:"
  echo "  $CANONICAL_GRID_MASTER"
  echo "  $LEGACY_GRID_MASTER"
  echo
  echo "Run this first:"
  echo "  scripts/run_objective3_integration.sh"
  exit 1
fi

python src/rrsmr_ads/objective3_smr_integration/system_impact_metrics.py "$@"
