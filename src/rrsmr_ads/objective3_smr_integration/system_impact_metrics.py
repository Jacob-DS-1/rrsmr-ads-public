#!/usr/bin/env python3
"""Objective 3 system impact metrics from the repo grid-master output."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Iterable

import pandas as pd


TECH_SUPPLY_COLUMNS = [
    "wind_mw",
    "solar_mw",
    "nuclear_existing_mw",
    "biomass_mw",
    "hydro_mw",
    "other_mw",
]

KEY_COLUMNS = [
    "timestamp_utc",
    "year",
    "fes_scenario",
    "climate_member",
    "weather_year_role",
    "smr_case",
]

ANNUAL_GROUP_COLUMNS = [
    "year",
    "fes_scenario",
    "climate_member",
    "weather_year_role",
    "smr_case",
]

PERIOD_GROUP_COLUMNS = [
    "fes_scenario",
    "climate_member",
    "weather_year_role",
    "smr_case",
]

HOURLY_OUTPUT_NAME = "system_impact_hourly_2030_2045"
LEGACY_HOURLY_OUTPUT_NAME = "system_impact_metrics_hourly_2030_2045.parquet"
ANNUAL_OUTPUT_NAME = "system_impact_summary_annual_2030_2045.csv"
PERIOD_OUTPUT_NAME = "system_impact_summary_period_2030_2045.csv"


def resolve_repo_root(start: Path | None = None) -> Path:
    """Resolve the repository root by searching upward for config/paths.yaml."""
    current = (start or Path.cwd()).resolve()
    if current.is_file():
        current = current.parent

    for candidate in [current, *current.parents]:
        if (candidate / "config" / "paths.yaml").exists():
            return candidate

    raise FileNotFoundError(
        f"Could not resolve repo root from {current}; config/paths.yaml not found."
    )


def remove_path(path: Path) -> None:
    """Remove a file or directory if it exists."""
    if not path.exists():
        return

    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _as_repo_path(path: Path, repo_root: Path) -> Path:
    if path.is_absolute():
        return path
    return repo_root / path


def _require_columns(df: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def ensure_exogenous_supply(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure exogenous_supply_mw is available for residual-demand metrics."""
    result = df.copy()

    if "exogenous_supply_mw" not in result.columns:
        _require_columns(result, TECH_SUPPLY_COLUMNS, "grid master")
        result["exogenous_supply_mw"] = result[TECH_SUPPLY_COLUMNS].sum(axis=1)

    return result


def _low_wind_threshold(df: pd.DataFrame, quantile: float) -> float:
    wind_key = [
        col
        for col in [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "weather_year_role",
            "weather_year",
        ]
        if col in df.columns
    ]

    wind_source = df[wind_key + ["wind_mw"]].drop_duplicates()
    return float(wind_source["wind_mw"].quantile(quantile))


def build_hourly_metrics(
    grid_master: pd.DataFrame,
    low_wind_quantile: float = 0.10,
) -> pd.DataFrame:
    """Calculate hourly residual, gas-displacement, surplus, and low-wind metrics."""
    if not 0 < low_wind_quantile < 1:
        raise ValueError("low_wind_quantile must be between 0 and 1.")

    df = ensure_exogenous_supply(grid_master)

    required = KEY_COLUMNS + [
        "demand_mw",
        "imports_net_baseline_mw",
        "smr_total_delivered_mw",
        "wind_mw",
        "exogenous_supply_mw",
    ]
    _require_columns(df, required, "grid master")

    wind_threshold = _low_wind_threshold(df, low_wind_quantile)

    df["residual_before_smr_mw"] = (
        df["demand_mw"] - df["exogenous_supply_mw"] - df["imports_net_baseline_mw"]
    )
    df["residual_after_smr_mw"] = (
        df["residual_before_smr_mw"] - df["smr_total_delivered_mw"]
    )
    df["gas_needed_before_mw"] = df["residual_before_smr_mw"].clip(lower=0)
    df["gas_needed_after_mw"] = df["residual_after_smr_mw"].clip(lower=0)
    df["gas_displacement_proxy_mw"] = (
        df["gas_needed_before_mw"] - df["gas_needed_after_mw"]
    )
    df["surplus_after_smr_mw"] = (-df["residual_after_smr_mw"]).clip(lower=0)
    df["residual_reduction_mw"] = (
        df["residual_before_smr_mw"] - df["residual_after_smr_mw"]
    )
    df["low_wind_flag"] = df["wind_mw"] <= wind_threshold
    df["low_wind_support_flag"] = (
        df["low_wind_flag"] & (df["gas_displacement_proxy_mw"] > 0)
    )

    output_columns = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
    ]

    if "weather_year" in df.columns:
        output_columns.append("weather_year")

    output_columns.extend(
        [
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
        ]
    )

    return df[output_columns].copy()


def build_annual_summary(hourly: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly metrics to annual system-impact summaries."""
    _require_columns(hourly, ANNUAL_GROUP_COLUMNS, "hourly metrics")

    metrics = hourly.copy()
    metrics["smr_delivered_mwh"] = metrics["smr_total_delivered_mw"]
    metrics["gas_displacement_mwh"] = metrics["gas_displacement_proxy_mw"]
    metrics["surplus_mwh"] = metrics["surplus_after_smr_mw"]

    annual = (
        metrics.groupby(ANNUAL_GROUP_COLUMNS, dropna=False)
        .agg(
            annual_smr_delivered_energy_twh=(
                "smr_delivered_mwh",
                lambda x: x.sum() / 1_000_000,
            ),
            annual_gas_displacement_twh=(
                "gas_displacement_mwh",
                lambda x: x.sum() / 1_000_000,
            ),
            annual_surplus_energy_twh=(
                "surplus_mwh",
                lambda x: x.sum() / 1_000_000,
            ),
            surplus_hours_count=("surplus_after_smr_mw", lambda x: int((x > 0).sum())),
            low_wind_hours_count=("low_wind_flag", "sum"),
            low_wind_support_hours=("low_wind_support_flag", "sum"),
            average_residual_before_mw=("residual_before_smr_mw", "mean"),
            average_residual_after_mw=("residual_after_smr_mw", "mean"),
            average_residual_reduction_mw=("residual_reduction_mw", "mean"),
            average_gas_displacement_proxy_mw=("gas_displacement_proxy_mw", "mean"),
        )
        .reset_index()
    )

    count_columns = ["surplus_hours_count", "low_wind_hours_count", "low_wind_support_hours"]
    annual[count_columns] = annual[count_columns].astype(int)

    return annual


def build_period_summary(hourly: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly metrics to 2030-2045 period summaries."""
    _require_columns(hourly, PERIOD_GROUP_COLUMNS, "hourly metrics")

    metrics = hourly.copy()
    metrics["smr_delivered_mwh"] = metrics["smr_total_delivered_mw"]
    metrics["gas_displacement_mwh"] = metrics["gas_displacement_proxy_mw"]
    metrics["surplus_mwh"] = metrics["surplus_after_smr_mw"]

    period = (
        metrics.groupby(PERIOD_GROUP_COLUMNS, dropna=False)
        .agg(
            cumulative_smr_delivered_energy_twh=(
                "smr_delivered_mwh",
                lambda x: x.sum() / 1_000_000,
            ),
            cumulative_gas_displacement_twh=(
                "gas_displacement_mwh",
                lambda x: x.sum() / 1_000_000,
            ),
            cumulative_surplus_energy_twh=(
                "surplus_mwh",
                lambda x: x.sum() / 1_000_000,
            ),
            total_surplus_hours=("surplus_after_smr_mw", lambda x: int((x > 0).sum())),
            total_low_wind_hours=("low_wind_flag", "sum"),
            total_low_wind_support_hours=("low_wind_support_flag", "sum"),
            average_residual_before_mw=("residual_before_smr_mw", "mean"),
            average_residual_after_mw=("residual_after_smr_mw", "mean"),
            average_residual_demand_reduction_mw=("residual_reduction_mw", "mean"),
            average_gas_displacement_proxy_mw=("gas_displacement_proxy_mw", "mean"),
        )
        .reset_index()
    )

    count_columns = [
        "total_surplus_hours",
        "total_low_wind_hours",
        "total_low_wind_support_hours",
    ]
    period[count_columns] = period[count_columns].astype(int)

    return period


def write_outputs(
    hourly: pd.DataFrame,
    annual: pd.DataFrame,
    period: pd.DataFrame,
    output_dir: Path,
    clean: bool = False,
) -> dict[str, Path]:
    """Write canonical and legacy system-impact outputs."""
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "hourly": output_dir / HOURLY_OUTPUT_NAME,
        "legacy_hourly": output_dir / LEGACY_HOURLY_OUTPUT_NAME,
        "annual": output_dir / ANNUAL_OUTPUT_NAME,
        "period": output_dir / PERIOD_OUTPUT_NAME,
    }

    if clean:
        for path in paths.values():
            remove_path(path)

    hourly.to_parquet(paths["hourly"], index=False)
    hourly.to_parquet(paths["legacy_hourly"], index=False)
    annual.to_csv(paths["annual"], index=False)
    period.to_csv(paths["period"], index=False)

    return paths


def run(
    grid_master_path: Path,
    output_dir: Path,
    clean: bool = False,
    low_wind_quantile: float = 0.10,
) -> dict[str, object]:
    """Run the system-impact calculation and return a compact audit dictionary."""
    grid_master = pd.read_parquet(grid_master_path)

    hourly = build_hourly_metrics(grid_master, low_wind_quantile=low_wind_quantile)
    annual = build_annual_summary(hourly)
    period = build_period_summary(hourly)

    output_paths = write_outputs(hourly, annual, period, output_dir, clean=clean)

    key_columns = KEY_COLUMNS
    duplicate_hourly_keys = int(hourly.duplicated(key_columns).sum())
    required_nulls = int(hourly[key_columns].isna().sum().sum())

    return {
        "grid_master_path": grid_master_path,
        "output_paths": output_paths,
        "hourly_rows": len(hourly),
        "hourly_columns": len(hourly.columns),
        "annual_rows": len(annual),
        "period_rows": len(period),
        "duplicate_hourly_keys": duplicate_hourly_keys,
        "required_key_nulls": required_nulls,
        "fes_scenarios": sorted(hourly["fes_scenario"].dropna().unique().tolist()),
        "climate_members": sorted(hourly["climate_member"].dropna().unique().tolist()),
        "weather_year_roles": sorted(hourly["weather_year_role"].dropna().unique().tolist()),
        "smr_cases": sorted(hourly["smr_case"].dropna().unique().tolist()),
        "gas_displacement_twh": float(
            hourly["gas_displacement_proxy_mw"].sum() / 1_000_000
        ),
        "surplus_hours": int((hourly["surplus_after_smr_mw"] > 0).sum()),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate Objective 3 system impact metrics from the grid master."
    )
    parser.add_argument(
        "--grid-master",
        type=Path,
        default=None,
        help="Grid master parquet path. Defaults to the canonical Objective 3 grid-master output.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults to data/processed/objective3_smr_integration.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing system-impact outputs before writing.",
    )
    parser.add_argument(
        "--low-wind-quantile",
        type=float,
        default=0.10,
        help="Quantile used to flag low-wind hours. Default: 0.10.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    repo_root = resolve_repo_root(Path(__file__))

    default_output_dir = (
        repo_root / "data" / "processed" / "objective3_smr_integration"
    )
    output_dir = _as_repo_path(args.output_dir, repo_root) if args.output_dir else default_output_dir

    if args.grid_master:
        grid_master_path = _as_repo_path(args.grid_master, repo_root)
    else:
        canonical_grid_master = output_dir / "grid_master_hourly_2030_2045"
        legacy_grid_master = output_dir / "grid_master_hourly_2030_2045.parquet"
        grid_master_path = (
            canonical_grid_master
            if canonical_grid_master.exists()
            else legacy_grid_master
        )

    if not grid_master_path.exists():
        raise FileNotFoundError(
            f"Grid master not found: {grid_master_path}. "
            "Run scripts/run_objective3_integration.sh first."
        )

    result = run(
        grid_master_path=grid_master_path,
        output_dir=output_dir,
        clean=args.clean,
        low_wind_quantile=args.low_wind_quantile,
    )

    print("Objective 3 system impact metrics complete")
    print(f"grid_master_path: {result['grid_master_path']}")
    for name, path in result["output_paths"].items():
        print(f"{name}_path: {path}")
    print(f"hourly_rows: {result['hourly_rows']}")
    print(f"hourly_columns: {result['hourly_columns']}")
    print(f"annual_rows: {result['annual_rows']}")
    print(f"period_rows: {result['period_rows']}")
    print(f"duplicate_hourly_keys: {result['duplicate_hourly_keys']}")
    print(f"required_key_nulls: {result['required_key_nulls']}")
    print(f"fes_scenarios: {result['fes_scenarios']}")
    print(f"climate_members: {result['climate_members']}")
    print(f"weather_year_roles: {result['weather_year_roles']}")
    print(f"smr_cases: {result['smr_cases']}")
    print(f"gas_displacement_twh: {result['gas_displacement_twh']:.6f}")
    print(f"surplus_hours: {result['surplus_hours']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
