#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

HOURLY_SYSTEM_IMPACT="data/processed/objective3_smr_integration/system_impact_hourly_2030_2045"
ANNUAL_SYSTEM_IMPACT="data/processed/objective3_smr_integration/system_impact_summary_annual_2030_2045.csv"
PERIOD_SYSTEM_IMPACT="data/processed/objective3_smr_integration/system_impact_summary_period_2030_2045.csv"

if [[ ! -e "$HOURLY_SYSTEM_IMPACT" || ! -f "$ANNUAL_SYSTEM_IMPACT" || ! -f "$PERIOD_SYSTEM_IMPACT" ]]; then
  echo "Missing Objective 3 system impact inputs."
  echo "Expected:"
  echo "  $HOURLY_SYSTEM_IMPACT"
  echo "  $ANNUAL_SYSTEM_IMPACT"
  echo "  $PERIOD_SYSTEM_IMPACT"
  echo
  echo "Run these first:"
  echo "  scripts/run_objective3_integration.sh"
  echo "  scripts/run_objective3_system_impact.sh --clean"
  exit 1
fi

if [[ "${1:-}" == "--clean" ]]; then
  rm -rf \
    outputs/objective3_smr_integration \
    outputs/figures/objective3_smr_integration
  shift
fi

python src/rrsmr_ads/objective3_smr_integration/owner5_visualisations_final_qa.py --build "$@"
