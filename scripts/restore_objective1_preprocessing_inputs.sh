#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 external_data/source_inputs" >&2
  exit 2
fi

SRC_POP="$1"
SRC_OBJ1="$SRC_POP/objective1/objective1_raw/data/preprocessed"
DEST="data/processed/preprocessing"

required_files=(
  "genmix_hist_hourly.parquet"
  "era5_resource_hourly_gb_2010_2024.parquet"
  "dukes_capacity_hist_2010_2024.parquet"
  "dukes_loadfactor_hist_2010_2024.parquet"
  "genmix_profile_library.parquet"
  "fes_supply_annual_2030_2045.parquet"
  "interconnector_annual_hist_2010_2024.parquet"
)

mkdir -p "$DEST"

echo "Restoring Objective 1 preprocessing inputs from:"
echo "  $SRC_OBJ1"
echo

for file in "${required_files[@]}"; do
  src="$SRC_OBJ1/$file"
  dst="$DEST/$file"

  if [ ! -f "$src" ]; then
    echo "Missing source file: $src" >&2
    exit 1
  fi

  cp -v "$src" "$dst"
done

echo
echo "Restored files under $DEST"
echo "These generated preprocessing artifacts should remain ignored by git."
