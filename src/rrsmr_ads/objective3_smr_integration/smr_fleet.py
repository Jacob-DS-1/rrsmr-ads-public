#!/usr/bin/env python3
"""Generate Objective 3 SMR unit library and fleet scenario outputs.

This script migrates the Objective 3 Owner/Part 1 and Owner/Part 2 notebook
logic into a repository-path-aware, reproducible implementation.

It reads config/smr_assumptions.csv and writes ignored generated outputs under
data/processed/objective3_smr_integration/.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path

import hashlib
import numpy as np
import pandas as pd


FES_SCENARIOS = ("Electric Engagement", "Holistic Transition")
REQUIRED_ASSUMPTION_COLUMNS = {
    "unit_id",
    "smr_case",
    "unit_name",
    "nameplate_mwe",
    "net_delivery_factor",
    "commissioning_date",
    "planned_outage_window",
    "forced_outage_rate",
}

OPTIONAL_ASSUMPTION_DEFAULTS = {
    "planned_outage_frequency_months": 24,
}


@dataclass(frozen=True)
class SmrFleetPaths:
    repo_root: Path
    assumptions: Path
    processed_dir: Path
    unit_library: Path
    fleet: Path
    legacy_fleet: Path


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_paths(repo_root: Path | None = None) -> SmrFleetPaths:
    root = repo_root or resolve_repo_root()
    processed_dir = root / "data" / "processed" / "objective3_smr_integration"
    return SmrFleetPaths(
        repo_root=root,
        assumptions=root / "config" / "smr_assumptions.csv",
        processed_dir=processed_dir,
        unit_library=processed_dir / "smr_hourly_library_2030_2045",
        fleet=processed_dir / "smr_fleet_hourly_2030_2045",
        legacy_fleet=processed_dir / "smr_hourly_fleet_scenarios.parquet",
    )


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def load_assumptions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"SMR assumptions file not found: {path}")

    assumptions = pd.read_csv(path)
    for column, default in OPTIONAL_ASSUMPTION_DEFAULTS.items():
        if column not in assumptions.columns:
            assumptions[column] = default

    missing = sorted(REQUIRED_ASSUMPTION_COLUMNS - set(assumptions.columns))
    if missing:
        raise ValueError(f"SMR assumptions file is missing columns: {missing}")

    assumptions = assumptions.copy()
    assumptions["commissioning_date"] = pd.to_datetime(
        assumptions["commissioning_date"], utc=True
    )
    assumptions["nameplate_mwe"] = pd.to_numeric(assumptions["nameplate_mwe"])
    assumptions["net_delivery_factor"] = pd.to_numeric(
        assumptions["net_delivery_factor"]
    )
    assumptions["planned_outage_window"] = pd.to_numeric(
        assumptions["planned_outage_window"]
    ).astype(int)
    assumptions["planned_outage_frequency_months"] = pd.to_numeric(
        assumptions["planned_outage_frequency_months"]
    ).astype(int)
    assumptions["forced_outage_rate"] = pd.to_numeric(
        assumptions["forced_outage_rate"]
    )

    invalid_frequency = assumptions["planned_outage_frequency_months"] <= 0
    if invalid_frequency.any():
        raise ValueError("planned_outage_frequency_months must be positive")

    invalid_forced = ~assumptions["forced_outage_rate"].between(0.0, 1.0)
    if invalid_forced.any():
        raise ValueError("forced_outage_rate must be between 0 and 1")

    duplicated = assumptions.duplicated(["smr_case", "unit_id"]).sum()
    if duplicated:
        raise ValueError(
            "SMR assumptions must be unique by smr_case + unit_id; "
            f"found {duplicated} duplicate rows."
        )

    return assumptions


def hourly_index(start_year: int = 2030, end_year: int = 2045) -> pd.DatetimeIndex:
    return pd.date_range(
        start=f"{start_year}-01-01 00:00:00",
        end=f"{end_year}-12-31 23:00:00",
        freq="h",
        tz="UTC",
        name="timestamp_utc",
    )


def build_unit_library(
    assumptions: pd.DataFrame,
    timestamps: pd.DatetimeIndex | None = None,
) -> pd.DataFrame:
    """Build unit-level hourly SMR availability and delivered output.

    Planned outages are scheduled every planned_outage_frequency_months
    after each unit's commissioning date. Forced outages are represented by a
    deterministic, reproducible allocation of otherwise available hours within
    each year. This avoids stochastic run-to-run drift while still respecting
    the forced_outage_rate assumption.
    """

    ts = timestamps if timestamps is not None else hourly_index()
    time_frame = pd.DataFrame({"timestamp_utc": ts})
    time_frame["year"] = time_frame["timestamp_utc"].dt.year.astype("int32")
    time_frame["month"] = time_frame["timestamp_utc"].dt.month.astype("int32")
    time_frame["day"] = time_frame["timestamp_utc"].dt.day.astype("int32")
    time_frame["hour"] = time_frame["timestamp_utc"].dt.hour.astype("int32")
    time_frame["day_of_year"] = time_frame["timestamp_utc"].dt.dayofyear.astype("int32")

    max_timestamp = time_frame["timestamp_utc"].max()
    frames: list[pd.DataFrame] = []

    for _, unit in assumptions.sort_values(["smr_case", "unit_id"]).iterrows():
        frame = time_frame.copy()

        commissioning_date = unit["commissioning_date"]
        planned_outage_days = int(unit["planned_outage_window"])
        planned_frequency_months = int(unit["planned_outage_frequency_months"])
        forced_outage_rate = float(unit["forced_outage_rate"])

        frame["unit_id"] = unit["unit_id"]
        frame["smr_case"] = unit["smr_case"]
        frame["unit_name"] = unit["unit_name"]
        frame["nameplate_mw"] = float(unit["nameplate_mwe"])
        frame["net_delivery_factor"] = float(unit["net_delivery_factor"])
        frame["planned_outage_window"] = planned_outage_days
        frame["planned_outage_frequency_months"] = planned_frequency_months
        frame["forced_outage_rate"] = forced_outage_rate

        frame["is_commissioned"] = frame["timestamp_utc"] >= commissioning_date

        planned_outage = pd.Series(False, index=frame.index)
        outage_start = commissioning_date
        while outage_start <= max_timestamp:
            outage_end = outage_start + pd.Timedelta(days=planned_outage_days)
            planned_outage |= (
                (frame["timestamp_utc"] >= outage_start)
                & (frame["timestamp_utc"] < outage_end)
            )
            outage_start = outage_start + pd.DateOffset(
                months=planned_frequency_months
            )

        frame["planned_outage"] = frame["is_commissioned"] & planned_outage
        frame["forced_outage"] = False

        otherwise_available = frame["is_commissioned"] & ~frame["planned_outage"]

        if forced_outage_rate > 0:
            candidate_positions = frame.index[otherwise_available].to_numpy()
            candidate_years = frame.loc[otherwise_available, "year"].to_numpy()

            for year in np.unique(candidate_years):
                year_positions = candidate_positions[candidate_years == year]
                forced_hours = int(round(len(year_positions) * forced_outage_rate))

                if forced_hours <= 0:
                    continue

                seed_input = (
                    f"{unit['smr_case']}|{unit['unit_id']}|{year}|"
                    f"{forced_outage_rate:.8f}"
                )
                seed = int(
                    hashlib.sha256(seed_input.encode("utf-8")).hexdigest()[:16],
                    16,
                ) % (2**32)

                rng = np.random.default_rng(seed)
                selected_positions = rng.choice(
                    year_positions,
                    size=min(forced_hours, len(year_positions)),
                    replace=False,
                )
                frame.loc[selected_positions, "forced_outage"] = True

        frame["is_available"] = (
            frame["is_commissioned"]
            & ~frame["planned_outage"]
            & ~frame["forced_outage"]
        )
        frame["delivered_mw"] = (
            frame["nameplate_mw"] * frame["net_delivery_factor"]
        ).where(frame["is_available"], 0.0)

        frames.append(frame)

    library = pd.concat(frames, ignore_index=True)

    output_columns = [
        "timestamp_utc",
        "year",
        "month",
        "day",
        "hour",
        "unit_id",
        "smr_case",
        "unit_name",
        "nameplate_mw",
        "net_delivery_factor",
        "planned_outage_window",
        "planned_outage_frequency_months",
        "forced_outage_rate",
        "is_commissioned",
        "planned_outage",
        "forced_outage",
        "is_available",
        "delivered_mw",
    ]

    return library[output_columns].sort_values(
        ["smr_case", "unit_id", "timestamp_utc"]
    ).reset_index(drop=True)


def build_fleet_scenarios(
    unit_library: pd.DataFrame,
    fes_scenarios: tuple[str, ...] = FES_SCENARIOS,
) -> pd.DataFrame:
    pivot = (
        unit_library.pivot_table(
            index=["timestamp_utc", "year", "smr_case"],
            columns="unit_id",
            values="delivered_mw",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    expected_units = ["unit_1", "unit_2", "unit_3"]
    for unit in expected_units:
        if unit not in pivot.columns:
            pivot[unit] = 0.0

    fleet = pivot.rename(
        columns={
            "unit_1": "unit_1_mw",
            "unit_2": "unit_2_mw",
            "unit_3": "unit_3_mw",
        }
    )
    fleet["total_fleet_mw"] = (
        fleet["unit_1_mw"] + fleet["unit_2_mw"] + fleet["unit_3_mw"]
    )

    fleet["unit1_delivered_mw"] = fleet["unit_1_mw"]
    fleet["unit2_delivered_mw"] = fleet["unit_2_mw"]
    fleet["unit3_delivered_mw"] = fleet["unit_3_mw"]
    fleet["smr_total_delivered_mw"] = fleet["total_fleet_mw"]

    scenarios = pd.DataFrame({"fes_scenario": list(fes_scenarios)})
    fleet["_join_key"] = 1
    scenarios["_join_key"] = 1

    fleet = fleet.merge(scenarios, on="_join_key", how="inner").drop(
        columns=["_join_key"]
    )

    output_columns = [
        "timestamp_utc",
        "fes_scenario",
        "smr_case",
        "year",
        "unit_1_mw",
        "unit_2_mw",
        "unit_3_mw",
        "total_fleet_mw",
        "unit1_delivered_mw",
        "unit2_delivered_mw",
        "unit3_delivered_mw",
        "smr_total_delivered_mw",
    ]

    return fleet[output_columns].sort_values(
        ["timestamp_utc", "fes_scenario", "smr_case"]
    ).reset_index(drop=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    remove_path(path)
    df.to_parquet(path, index=False)


def generate_outputs(paths: SmrFleetPaths, clean: bool = False) -> dict[str, object]:
    if clean:
        remove_path(paths.unit_library)
        remove_path(paths.fleet)
        remove_path(paths.legacy_fleet)

    assumptions = load_assumptions(paths.assumptions)
    unit_library = build_unit_library(assumptions)
    fleet = build_fleet_scenarios(unit_library)

    duplicate_unit_keys = int(
        unit_library.duplicated(["timestamp_utc", "smr_case", "unit_id"]).sum()
    )
    if duplicate_unit_keys:
        raise RuntimeError(
            "Generated unit library has duplicate timestamp_utc + smr_case + unit_id keys: "
            f"{duplicate_unit_keys}"
        )

    duplicate_fleet_keys = int(
        fleet.duplicated(["timestamp_utc", "fes_scenario", "smr_case"]).sum()
    )
    if duplicate_fleet_keys:
        raise RuntimeError(
            "Generated fleet output has duplicate timestamp_utc + fes_scenario + smr_case keys: "
            f"{duplicate_fleet_keys}"
        )

    write_parquet(unit_library, paths.unit_library)
    write_parquet(fleet, paths.fleet)
    write_parquet(fleet, paths.legacy_fleet)

    return {
        "unit_library_path": str(paths.unit_library.relative_to(paths.repo_root)),
        "fleet_path": str(paths.fleet.relative_to(paths.repo_root)),
        "legacy_fleet_path": str(paths.legacy_fleet.relative_to(paths.repo_root)),
        "unit_library_rows": int(len(unit_library)),
        "fleet_rows": int(len(fleet)),
        "smr_cases": sorted(fleet["smr_case"].unique().tolist()),
        "fes_scenarios": sorted(fleet["fes_scenario"].unique().tolist()),
        "timestamp_start": fleet["timestamp_utc"].min().strftime("%Y-%m-%d %H:%M UTC"),
        "timestamp_end": fleet["timestamp_utc"].max().strftime("%Y-%m-%d %H:%M UTC"),
        "duplicate_unit_keys": duplicate_unit_keys,
        "duplicate_fleet_keys": duplicate_fleet_keys,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate Objective 3 SMR unit library and fleet scenario outputs."
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing generated SMR fleet outputs before writing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paths = default_paths()
    summary = generate_outputs(paths, clean=args.clean)

    print("Objective 3 SMR fleet generation complete")
    for key, value in summary.items():
        print(f"{key}: {value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
