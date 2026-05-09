#!/usr/bin/env python3
"""Audit current reproducible model outputs.

This script validates structural reproducibility checks for ignored generated
outputs. It does not create or modify model outputs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


SCENARIOS = ["Electric Engagement", "Holistic Transition"]
YEARS = list(range(2030, 2046))
CLIMATE_MEMBERS = ["member_06", "member_12", "member_13"]
WEATHER_YEAR_ROLES = ["average_wind", "high_wind", "low_wind"]
SMR_CASES = ["simultaneous_commissioning", "staggered_commissioning"]


EXPECTED_DATASETS: dict[str, dict[str, Any]] = {
    "objective1_generation": {
        "path": "data/processed/objective1_generation/generation_future_hourly_2030_2045.parquet",
        "format": "parquet",
        "expected_rows": 841536,
        "timestamp_col": "timestamp_utc",
        "year_col": "year",
        "expected_start": "2030-01-01 00:00 UTC",
        "expected_end": "2045-12-31 23:00 UTC",
        "expected_years": YEARS,
        "expected_values": {
            "fes_scenario": SCENARIOS,
            "weather_year": ["2010", "2014", "2015"],
            "weather_year_role": WEATHER_YEAR_ROLES,
        },
        "unique_key": ["timestamp_utc", "year", "fes_scenario", "weather_year"],
        "non_null_columns": [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "weather_year",
            "weather_year_role",
        ],
        "check_mw_columns_non_null": True,
        "check_mw_columns_non_negative": True,
        "reference_content_sha256": "a8feb70e7b7a27ec2c7087fed6486f133f109f949227429a4c2ca8a6a2073d99",
    },
    "objective2_demand": {
        "path": "data/processed/objective2_demand/demand_future_hourly_2030_2045.parquet",
        "format": "parquet",
        "expected_rows": 841536,
        "timestamp_col": "timestamp_utc",
        "year_col": "year",
        "expected_start": "2030-01-01 00:00 UTC",
        "expected_end": "2045-12-31 23:00 UTC",
        "expected_years": YEARS,
        "expected_values": {
            "fes_scenario": SCENARIOS,
            "climate_member": CLIMATE_MEMBERS,
        },
        "unique_key": ["timestamp_utc", "year", "fes_scenario", "climate_member"],
        "non_null_columns": [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "climate_member",
            "demand_mw",
        ],
        "non_negative_columns": ["demand_mw"],
        "reference_content_sha256": "0ea51272fb868017b33a3d3feea15221693066f950027b8a78944cc545537bc8",
    },
    "objective3_smr_fleet": {
        "path": "data/processed/objective3_smr_integration/smr_hourly_fleet_scenarios.parquet",
        "format": "parquet",
        "expected_rows": 561024,
        "timestamp_col": "timestamp_utc",
        "year_col": "year",
        "expected_start": "2030-01-01 00:00 UTC",
        "expected_end": "2045-12-31 23:00 UTC",
        "expected_years": YEARS,
        "expected_values": {
            "fes_scenario": SCENARIOS,
            "smr_case": SMR_CASES,
        },
        "unique_key": ["timestamp_utc", "year", "fes_scenario", "smr_case"],
        "non_null_columns": [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "smr_case",
            "unit1_delivered_mw",
            "unit2_delivered_mw",
            "unit3_delivered_mw",
            "smr_total_delivered_mw",
        ],
        "non_negative_columns": [
            "unit1_delivered_mw",
            "unit2_delivered_mw",
            "unit3_delivered_mw",
            "smr_total_delivered_mw",
        ],
        "expected_max_values": {
            "smr_total_delivered_mw": 1410.0,
        },
    },
    "objective3_grid_master": {
        "path": "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045",
        "format": "parquet",
        "expected_rows": 5049216,
        "timestamp_col": "timestamp_utc",
        "year_col": "year",
        "expected_start": "2030-01-01 00:00 UTC",
        "expected_end": "2045-12-31 23:00 UTC",
        "expected_years": YEARS,
        "expected_values": {
            "fes_scenario": SCENARIOS,
            "climate_member": CLIMATE_MEMBERS,
            "weather_year_role": WEATHER_YEAR_ROLES,
            "smr_case": SMR_CASES,
        },
        "unique_key": [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "smr_case",
        ],
        "non_null_columns": [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "weather_year",
            "smr_case",
            "demand_mw",
            "exogenous_supply_mw",
            "imports_net_baseline_mw",
            "smr_total_delivered_mw",
        ],
        "non_negative_columns": [
            "demand_mw",
            "wind_mw",
            "solar_mw",
            "nuclear_existing_mw",
            "biomass_mw",
            "hydro_mw",
            "other_mw",
            "smr_total_delivered_mw",
            "exogenous_supply_mw",
        ],
        "expected_max_values": {
            "smr_total_delivered_mw": 1410.0,
        },
    },
    "objective3_system_impact_hourly": {
        "path": "data/processed/objective3_smr_integration/system_impact_hourly_2030_2045",
        "format": "parquet",
        "expected_rows": 5049216,
        "timestamp_col": "timestamp_utc",
        "year_col": "year",
        "expected_start": "2030-01-01 00:00 UTC",
        "expected_end": "2045-12-31 23:00 UTC",
        "expected_years": YEARS,
        "expected_values": {
            "fes_scenario": SCENARIOS,
            "climate_member": CLIMATE_MEMBERS,
            "weather_year_role": WEATHER_YEAR_ROLES,
            "smr_case": SMR_CASES,
        },
        "unique_key": [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "smr_case",
        ],
        "non_null_columns": [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "weather_year",
            "smr_case",
            "demand_mw",
            "wind_mw",
            "exogenous_supply_mw",
            "imports_net_baseline_mw",
            "smr_total_delivered_mw",
            "residual_before_smr_mw",
            "residual_after_smr_mw",
            "gas_needed_before_mw",
            "gas_needed_after_mw",
            "gas_displacement_proxy_mw",
            "surplus_after_smr_mw",
            "residual_reduction_mw",
            "low_wind_flag",
            "low_wind_support_flag",
        ],
        "non_negative_columns": [
            "demand_mw",
            "wind_mw",
            "smr_total_delivered_mw",
            "gas_needed_before_mw",
            "gas_needed_after_mw",
            "gas_displacement_proxy_mw",
            "surplus_after_smr_mw",
            "residual_reduction_mw",
        ],
        "expected_max_values": {
            "smr_total_delivered_mw": 1410.0,
            "gas_displacement_proxy_mw": 1410.0,
        },
    },
    "objective3_system_impact_annual": {
        "path": "data/processed/objective3_smr_integration/system_impact_summary_annual_2030_2045.csv",
        "format": "csv",
        "expected_rows": 576,
        "year_col": "year",
        "expected_years": YEARS,
        "expected_values": {
            "fes_scenario": SCENARIOS,
            "climate_member": CLIMATE_MEMBERS,
            "weather_year_role": WEATHER_YEAR_ROLES,
            "smr_case": SMR_CASES,
        },
        "unique_key": [
            "year",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "smr_case",
        ],
        "non_null_columns": [
            "year",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "smr_case",
            "annual_smr_delivered_energy_twh",
            "annual_gas_displacement_twh",
            "annual_surplus_energy_twh",
            "surplus_hours_count",
            "low_wind_hours_count",
            "low_wind_support_hours",
        ],
        "non_negative_columns": [
            "annual_smr_delivered_energy_twh",
            "annual_gas_displacement_twh",
            "annual_surplus_energy_twh",
            "surplus_hours_count",
            "low_wind_hours_count",
            "low_wind_support_hours",
        ],
    },
    "objective3_system_impact_period": {
        "path": "data/processed/objective3_smr_integration/system_impact_summary_period_2030_2045.csv",
        "format": "csv",
        "expected_rows": 36,
        "expected_values": {
            "fes_scenario": SCENARIOS,
            "climate_member": CLIMATE_MEMBERS,
            "weather_year_role": WEATHER_YEAR_ROLES,
            "smr_case": SMR_CASES,
        },
        "unique_key": [
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "smr_case",
        ],
        "non_null_columns": [
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "smr_case",
            "cumulative_smr_delivered_energy_twh",
            "cumulative_gas_displacement_twh",
            "cumulative_surplus_energy_twh",
            "total_surplus_hours",
            "total_low_wind_hours",
            "total_low_wind_support_hours",
        ],
        "non_negative_columns": [
            "cumulative_smr_delivered_energy_twh",
            "cumulative_gas_displacement_twh",
            "cumulative_surplus_energy_twh",
            "total_surplus_hours",
            "total_low_wind_hours",
            "total_low_wind_support_hours",
        ],
    },
    "objective3_owner5_hourly": {
        "path": "outputs/objective3_smr_integration/system_impact_metrics_hourly_owner5_sensitivity_2030_2045.parquet",
        "format": "parquet",
        "expected_rows": 3366144,
        "timestamp_col": "timestamp_utc",
        "year_col": "year",
        "expected_start": "2030-01-01 00:00 UTC",
        "expected_end": "2045-12-31 23:00 UTC",
        "expected_years": YEARS,
        "expected_values": {
            "fes_scenario": SCENARIOS,
            "climate_member": CLIMATE_MEMBERS,
            "weather_year_role": ["average_wind", "low_wind"],
        },
        "non_null_columns": [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
        ],
    },
    "objective3_owner5_annual": {
        "path": "outputs/objective3_smr_integration/system_impact_summary_annual_owner5_sensitivity_2030_2045.csv",
        "format": "csv",
        "expected_rows": 384,
    },
    "objective3_owner5_period": {
        "path": "outputs/objective3_smr_integration/system_impact_summary_period_owner5_sensitivity_2030_2045.csv",
        "format": "csv",
        "expected_rows": 24,
    },
    "objective3_owner5_figures": {
        "path": "outputs/figures/objective3_smr_integration",
        "format": "files",
        "pattern": "*.png",
        "expected_file_count": 5,
    },
}


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()

    if path.is_file():
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        digest.update(child.relative_to(path).as_posix().encode("utf-8"))
        digest.update(b"\0")
        with child.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)

    return digest.hexdigest()


def format_utc(value: Any) -> str:
    import pandas as pd

    timestamp = pd.to_datetime(value, utc=True)
    return timestamp.strftime("%Y-%m-%d %H:%M UTC")


def sorted_string_values(values: Any) -> list[str]:
    return sorted(str(value) for value in values)


def read_dataset(path: Path, file_format: str):
    import pandas as pd

    if file_format == "csv":
        return pd.read_csv(path)

    if file_format == "parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported dataset format: {file_format}")


def audit_file_set(name: str, spec: dict[str, Any], repo_root: Path, allow_missing: bool) -> dict[str, Any]:
    path = repo_root / spec["path"]
    result: dict[str, Any] = {
        "name": name,
        "path": spec["path"],
        "status": "pass",
        "errors": [],
    }

    if not path.exists():
        result["status"] = "missing"
        if not allow_missing:
            result["errors"].append("Output path is missing. Run the relevant objective runner first.")
        return result

    pattern = spec.get("pattern", "*")
    files = sorted(child for child in path.glob(pattern) if child.is_file())
    result["file_count"] = len(files)
    result["files"] = [child.name for child in files]
    result["file_sha256"] = sha256_path(path)

    expected_file_count = spec.get("expected_file_count")
    if expected_file_count is not None and len(files) != expected_file_count:
        result["errors"].append(
            f"Expected {expected_file_count} files matching {pattern}, found {len(files)}."
        )

    if result["errors"]:
        result["status"] = "fail"

    return result


def audit_dataset(
    name: str,
    spec: dict[str, Any],
    repo_root: Path,
    allow_missing: bool,
    strict_file_hash: bool,
) -> dict[str, Any]:
    path = repo_root / spec["path"]
    file_format = spec.get("format", "parquet")

    if file_format == "files":
        return audit_file_set(name, spec, repo_root, allow_missing)

    result: dict[str, Any] = {
        "name": name,
        "path": spec["path"],
        "status": "pass",
        "errors": [],
    }

    if not path.exists():
        result["status"] = "missing"
        if not allow_missing:
            result["errors"].append("Output path is missing. Run the relevant objective runner first.")
        return result

    result["file_sha256"] = sha256_path(path)
    result["reference_content_sha256"] = spec.get("reference_content_sha256")

    if strict_file_hash and spec.get("reference_content_sha256"):
        if result["file_sha256"] != spec["reference_content_sha256"]:
            result["errors"].append(
                "File SHA-256 does not match the reference content SHA-256. "
                "This may indicate a real output change, or a different parquet writer/metadata layout."
            )

    df = read_dataset(path, file_format)

    result["rows"] = int(len(df))
    if result["rows"] != spec["expected_rows"]:
        result["errors"].append(
            f"Expected {spec['expected_rows']} rows, found {result['rows']}."
        )

    required_columns = sorted(
        set(spec.get("non_null_columns", []))
        | set(spec.get("unique_key", []))
        | set(spec.get("expected_values", {}).keys())
        | set(spec.get("non_negative_columns", []))
        | set(spec.get("expected_max_values", {}).keys())
    )

    if "timestamp_col" in spec:
        required_columns.append(spec["timestamp_col"])
    if "year_col" in spec:
        required_columns.append(spec["year_col"])

    required_columns = sorted(set(required_columns))
    missing_columns = [column for column in required_columns if column not in df.columns]
    result["missing_columns"] = missing_columns

    if missing_columns:
        result["errors"].append(f"Missing required columns: {missing_columns}")
        result["status"] = "fail"
        return result

    if "timestamp_col" in spec:
        timestamp_col = spec["timestamp_col"]
        timestamps = __import__("pandas").to_datetime(df[timestamp_col], utc=True)
        result["start"] = format_utc(timestamps.min())
        result["end"] = format_utc(timestamps.max())

        if result["start"] != spec["expected_start"]:
            result["errors"].append(
                f"Expected start {spec['expected_start']}, found {result['start']}."
            )

        if result["end"] != spec["expected_end"]:
            result["errors"].append(
                f"Expected end {spec['expected_end']}, found {result['end']}."
            )

    if "year_col" in spec:
        years = sorted(int(year) for year in df[spec["year_col"]].dropna().unique())
        result["years"] = years
        if years != spec["expected_years"]:
            result["errors"].append(
                f"Expected years {spec['expected_years']}, found {years}."
            )

    for column, expected in spec.get("expected_values", {}).items():
        actual_values = sorted_string_values(df[column].dropna().unique())
        expected_values = sorted_string_values(expected)
        result[f"{column}_values"] = actual_values
        if actual_values != expected_values:
            result["errors"].append(
                f"Expected {column} values {expected_values}, found {actual_values}."
            )

    if spec.get("unique_key"):
        duplicate_count = int(df.duplicated(spec["unique_key"]).sum())
        result["duplicate_key_count"] = duplicate_count
        if duplicate_count != 0:
            result["errors"].append(
                f"Expected 0 duplicate keys for {spec['unique_key']}, found {duplicate_count}."
            )

    non_null_columns = sorted(set(spec.get("non_null_columns", [])))
    if non_null_columns:
        null_count = int(df[non_null_columns].isna().sum().sum())
        result["required_null_count"] = null_count
        if null_count != 0:
            result["errors"].append(
                f"Expected 0 nulls in required columns, found {null_count}."
            )

    if spec.get("check_mw_columns_non_null"):
        mw_columns = [column for column in df.columns if column.endswith("_mw")]
        result["mw_columns_checked"] = sorted(mw_columns)
        if not mw_columns:
            result["errors"].append("No *_mw columns found for MW output checks.")
        else:
            mw_null_count = int(df[mw_columns].isna().sum().sum())
            result["mw_null_count"] = mw_null_count
            if mw_null_count != 0:
                result["errors"].append(
                    f"Expected 0 nulls in *_mw columns, found {mw_null_count}."
                )

    non_negative_columns = list(spec.get("non_negative_columns", []))

    if spec.get("check_mw_columns_non_negative"):
        non_negative_columns.extend(
            column for column in df.columns if column.endswith("_mw")
        )

    non_negative_columns = sorted(set(non_negative_columns))
    negative_counts = {}
    for column in non_negative_columns:
        values = __import__("pandas").to_numeric(df[column], errors="coerce")
        negative_counts[column] = int((values < 0).sum())

    result["negative_counts"] = {
        column: count for column, count in negative_counts.items() if count
    }

    if result["negative_counts"]:
        result["errors"].append(
            f"Expected no negative values, found {result['negative_counts']}."
        )

    for column, expected_max in spec.get("expected_max_values", {}).items():
        values = __import__("pandas").to_numeric(df[column], errors="coerce")
        actual_max = float(values.max())
        result[f"{column}_max"] = actual_max
        if abs(actual_max - float(expected_max)) > 1e-9:
            result["errors"].append(
                f"Expected max {column} to be {expected_max}, found {actual_max}."
            )

    if result["errors"]:
        result["status"] = "fail"

    return result


def print_text_report(payload: dict[str, Any]) -> None:
    print(f"Overall status: {payload['overall_status']}")

    for result in payload["results"]:
        print("")
        print(f"{result['name']}: {result['status'].upper()}")
        print(f"  path: {result['path']}")

        for field in [
            "rows",
            "start",
            "end",
            "duplicate_key_count",
            "required_null_count",
            "mw_null_count",
            "file_count",
            "file_sha256",
            "reference_content_sha256",
            "smr_total_delivered_mw_max",
            "gas_displacement_proxy_mw_max",
        ]:
            if field in result:
                print(f"  {field}: {result[field]}")

        if result.get("errors"):
            print("  errors:")
            for error in result["errors"]:
                print(f"    - {error}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit current reproducible generated model outputs."
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root. Defaults to the current working directory.",
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Return success when outputs are missing. Useful for fresh clones and CI smoke tests.",
    )
    parser.add_argument(
        "--strict-file-hash",
        action="store_true",
        help=(
            "Compare the byte-level file/tree SHA-256 to stored reference content SHA-256 values. "
            "Use with care because parquet metadata can make byte-level hashes differ."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of the text report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).resolve()

    results = [
        audit_dataset(
            name=name,
            spec=spec,
            repo_root=repo_root,
            allow_missing=args.allow_missing,
            strict_file_hash=args.strict_file_hash,
        )
        for name, spec in EXPECTED_DATASETS.items()
    ]

    has_errors = any(result.get("errors") for result in results)
    has_missing = any(result["status"] == "missing" for result in results)

    if has_errors:
        overall_status = "fail"
    elif has_missing:
        overall_status = "missing_allowed"
    else:
        overall_status = "pass"

    payload = {
        "overall_status": overall_status,
        "results": results,
    }

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_text_report(payload)

    return 1 if has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
