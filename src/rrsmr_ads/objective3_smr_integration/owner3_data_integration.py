#!/usr/bin/env python3
"""
Objective 3 - Owner 3 data integration and consistency QA.

Owner 3 merges three upstream model outputs into one hourly master dataset:

- Objective 2 demand: timestamp_utc + year + fes_scenario + climate_member
- Objective 1 generation: timestamp_utc + year + fes_scenario + weather_year_role
- Owner 2 SMR fleet: timestamp_utc + year + fes_scenario + smr_case

The agreed integration key for the final master output is:

timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case

This keeps both climate uncertainty from Objective 2 and supply weather-year
uncertainty from Objective 1 instead of collapsing either dimension.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


ALLOWED_SCENARIOS = ("Electric Engagement", "Holistic Transition")
EXPECTED_HOURS_2030_2045 = sum(
    8784 if calendar.isleap(year) else 8760 for year in range(2030, 2046)
)


@dataclass(frozen=True)
class PackagePaths:
    owner_dir: Path
    code_dir: Path
    inputs_dir: Path
    outputs_dir: Path
    qa_dir: Path
    team_folder: Path
    outputs_root: Path
    workspace_root: Path
    obj1_supply: Path
    obj1_qa_notes: Path
    obj1_qa_reconciliation: Path
    obj2_demand: Path
    obj2_qa_summary: Path
    smr_fleet_expected: Path
    smr_fleet_extracted: Path


def resolve_repo_root(start: Path | None = None) -> Path:
    """Resolve the repository root by searching upward for config/paths.yaml."""
    current = (start or Path(__file__).resolve()).resolve()
    if current.is_file():
        current = current.parent

    for candidate in [current, *current.parents]:
        if (candidate / "config" / "paths.yaml").exists():
            return candidate

    raise FileNotFoundError(
        "Could not resolve repository root from "
        f"{current}; expected to find config/paths.yaml in a parent directory."
    )


def resolve_generated_parquet_path(path: Path) -> Path:
    """Return an existing generated parquet path, allowing legacy no-suffix contracts."""
    if path.exists():
        return path

    if path.suffix == "":
        parquet_path = path.with_suffix(".parquet")
        if parquet_path.exists():
            return parquet_path

    return path


def resolve_paths() -> PackagePaths:
    """Resolve Objective 3 paths from the repository root.

    This migrated version uses the ADS repo layout rather than the original
    local handoff folder structure.
    """
    code_dir = Path(__file__).resolve().parent
    repo_root = resolve_repo_root(code_dir)
    validation_dir = repo_root / "docs" / "validation" / "objective3_smr_integration"
    objective3_processed = repo_root / "data" / "processed" / "objective3_smr_integration"

    return PackagePaths(
        owner_dir=repo_root,
        code_dir=code_dir,
        inputs_dir=validation_dir,
        outputs_dir=objective3_processed,
        qa_dir=validation_dir,
        team_folder=repo_root,
        outputs_root=repo_root / "outputs",
        workspace_root=repo_root,
        obj1_supply=resolve_generated_parquet_path(
            repo_root
            / "data"
            / "processed"
            / "objective1_generation"
            / "generation_future_hourly_2030_2045"
        ),
        obj1_qa_notes=repo_root
        / "docs"
        / "validation"
        / "objective1_generation"
        / "qa_notes.md",
        obj1_qa_reconciliation=repo_root
        / "docs"
        / "validation"
        / "objective1_generation"
        / "qa_fes_reconciliation.csv",
        obj2_demand=resolve_generated_parquet_path(
            repo_root
            / "data"
            / "processed"
            / "objective2_demand"
            / "demand_future_hourly_2030_2045"
        ),
        obj2_qa_summary=repo_root
        / "docs"
        / "validation"
        / "objective2_demand"
        / "demand_future_hourly_2030_2045_QA_summary.csv",
        smr_fleet_expected=objective3_processed / "smr_fleet_hourly_2030_2045",
        smr_fleet_extracted=objective3_processed / "smr_hourly_fleet_scenarios.parquet",
    )


def discover_smr_fleet(paths: PackagePaths, explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit
    for candidate in (paths.smr_fleet_expected, paths.smr_fleet_extracted):
        if candidate.exists():
            return candidate
    return paths.smr_fleet_expected


def ensure_dirs(paths: PackagePaths) -> None:
    for folder in (paths.inputs_dir, paths.outputs_dir, paths.qa_dir):
        folder.mkdir(parents=True, exist_ok=True)


def rel(path: Path, base: Path) -> str:
    try:
        return str(path.resolve().relative_to(base.resolve()))
    except ValueError:
        return str(path)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def read_obj2_qa(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.reader(f):
            if len(row) >= 2 and row[0]:
                values[row[0]] = row[1]
    return values


def file_status(path: Path) -> tuple[str, str]:
    if path.exists():
        return "available", str(path.stat().st_size)
    return "missing", ""


def build_manifest(paths: PackagePaths, smr_fleet: Path) -> list[dict]:
    specs = [
        (
            "obj1_supply",
            paths.obj1_supply,
            "Objective 1 final future hourly supply",
            "required_for_final_integration",
            "Keyed by timestamp_utc + year + fes_scenario + weather_year_role.",
        ),
        (
            "obj1_qa_notes",
            paths.obj1_qa_notes,
            "Objective 1 QA notes",
            "supporting_evidence",
            "Documents calendar, duplicate-key, sign, and reconciliation QA.",
        ),
        (
            "obj2_demand",
            paths.obj2_demand,
            "Objective 2 final future hourly demand",
            "required_for_final_integration",
            "Keyed by timestamp_utc + year + fes_scenario + climate_member.",
        ),
        (
            "obj2_qa_summary",
            paths.obj2_qa_summary,
            "Objective 2 QA summary",
            "supporting_evidence",
            "Documents row count, timestamp coverage, scenario count, member count, and reconciliation.",
        ),
        (
            "owner2_smr_fleet",
            smr_fleet,
            "Owner 2 SMR fleet hourly scenario output",
            "required_for_final_integration",
            "Actual file is adapted from unit_1_mw/unit_2_mw/unit_3_mw/total_fleet_mw naming.",
        ),
    ]
    rows = []
    for name, path, description, role, notes in specs:
        status, size = file_status(path)
        rows.append(
            {
                "input_name": name,
                "status": status,
                "path": rel(path, paths.team_folder),
                "size_bytes": size,
                "role": role,
                "description": description,
                "notes": notes,
            }
        )
    return rows


def decision_register_rows(smr_available: bool) -> list[dict]:
    return [
        {
            "decision_id": "D01",
            "decision": "Obj1 weather_year_role and Obj2 climate_member treatment",
            "current_status": "agreed_by_team",
            "recommended_default": "Keep both dimensions and expand the final key to timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case.",
            "alternatives": "Use only average_wind supply; map climate_member to weather_year_role.",
            "impact_if_unresolved": "Resolved. Master dataset preserves both demand climate uncertainty and supply weather-year uncertainty.",
            "owner3_action": "Build a full scenario skeleton and join each upstream dataset on its native key.",
        },
        {
            "decision_id": "D02",
            "decision": "Owner 2 SMR fleet input availability",
            "current_status": "available" if smr_available else "missing",
            "recommended_default": "Use provided smr_hourly_fleet_scenarios.parquet and adapt column names for Owner 3.",
            "alternatives": "Wait for a renamed smr_fleet_hourly_2030_2045.parquet.",
            "impact_if_unresolved": "No blocker if provided file is available.",
            "owner3_action": "Rename unit_1_mw/unit_2_mw/unit_3_mw/total_fleet_mw to Owner 3 standard delivered_mw names.",
        },
        {
            "decision_id": "D03",
            "decision": "Imports treatment in integrated master",
            "current_status": "agreed_in_objective3_plan",
            "recommended_default": "Keep imports_net_baseline_mw static; do not make imports respond to SMR output.",
            "alternatives": "Semi-flexible import cap; exclude imports from residual calculation.",
            "impact_if_unresolved": "Owner 4 residual/gas-displacement metrics would be inconsistent.",
            "owner3_action": "Carry imports_net_baseline_mw unchanged.",
        },
        {
            "decision_id": "D04",
            "decision": "Gas reference column use",
            "current_status": "agreed_in_objective1_readme",
            "recommended_default": "Retain gas_reference_mw as diagnostic only; do not treat it as final gas dispatch.",
            "alternatives": "Drop the column from master output.",
            "impact_if_unresolved": "Could confuse fixed reference output with Owner 4 balancing result.",
            "owner3_action": "Schema marks gas_reference_mw optional/diagnostic.",
        },
    ]


def schema_rows() -> list[dict]:
    rows = [
        ("timestamp_utc", "datetime64[ns, UTC]", "yes", "Hour-beginning UTC timestamp.", "identifier"),
        ("year", "int", "yes", "Calendar year derived from timestamp_utc.", "identifier"),
        ("fes_scenario", "string", "yes", "FES scenario: Electric Engagement or Holistic Transition.", "identifier"),
        ("climate_member", "string", "yes", "Selected UKCP18 climate member from Objective 2.", "identifier"),
        ("weather_year_role", "string", "yes", "Objective 1 supply weather role: low_wind, average_wind, high_wind.", "identifier"),
        ("weather_year", "int", "yes", "Representative historical weather year behind weather_year_role.", "identifier"),
        ("smr_case", "string", "yes", "SMR deployment case from Owner 2.", "identifier"),
        ("demand_mw", "float64", "yes", "Future hourly GB National Demand.", "demand"),
        ("wind_mw", "float64", "yes", "Objective 1 wind supply.", "supply"),
        ("solar_mw", "float64", "yes", "Objective 1 solar supply.", "supply"),
        ("nuclear_existing_mw", "float64", "yes", "Existing nuclear baseline, excluding new SMRs.", "supply"),
        ("biomass_mw", "float64", "yes", "Objective 1 biomass/waste supply.", "supply"),
        ("hydro_mw", "float64", "yes", "Objective 1 hydro supply.", "supply"),
        ("other_mw", "float64", "yes", "Objective 1 other renewable/thermal/hydrogen supply.", "supply"),
        ("imports_net_baseline_mw", "float64", "yes", "Static net import baseline; positive means import to GB.", "supply"),
        ("storage_net_mw", "float64", "optional", "Objective 1 storage net output; positive means discharge.", "diagnostic_supply"),
        ("gas_reference_mw", "float64", "optional", "Diagnostic gas reference only; not Owner 4 gas-needed result.", "diagnostic_supply"),
        ("coal_reference_mw", "float64", "optional", "Diagnostic historic coal reference.", "diagnostic_supply"),
        ("unit1_delivered_mw", "float64", "yes", "SMR unit 1 delivered MW after Owner 2 adaptation.", "smr"),
        ("unit2_delivered_mw", "float64", "yes", "SMR unit 2 delivered MW after Owner 2 adaptation.", "smr"),
        ("unit3_delivered_mw", "float64", "yes", "SMR unit 3 delivered MW after Owner 2 adaptation.", "smr"),
        ("smr_total_delivered_mw", "float64", "yes", "Fleet-level SMR delivered output from Owner 2.", "smr"),
        ("exogenous_supply_mw", "float64", "yes", "wind + solar + nuclear_existing + biomass + hydro + other.", "derived"),
    ]
    return [
        {
            "column_name": name,
            "dtype": dtype,
            "required": required,
            "description": desc,
            "category": category,
        }
        for name, dtype, required, desc, category in rows
    ]


def write_static_outputs(paths: PackagePaths, smr_fleet: Path) -> None:
    manifest = build_manifest(paths, smr_fleet)
    for row in manifest:
        if "size_bytes" in row:
            row["size_bytes"] = "not_recorded"
    write_csv(
        paths.inputs_dir / "input_manifest.csv",
        manifest,
        ["input_name", "status", "path", "size_bytes", "role", "description", "notes"],
    )
    write_csv(
        paths.qa_dir / "owner3_decision_register.csv",
        decision_register_rows(smr_fleet.exists()),
        [
            "decision_id",
            "decision",
            "current_status",
            "recommended_default",
            "alternatives",
            "impact_if_unresolved",
            "owner3_action",
        ],
    )
    write_csv(
        paths.outputs_dir / "grid_master_schema.csv",
        schema_rows(),
        ["column_name", "dtype", "required", "description", "category"],
    )


def require_pandas(paths: PackagePaths):
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Integration mode requires pandas plus a parquet engine from the "
            "project runtime environment."
        ) from exc
    return pd


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def read_parquet(pd, path: Path):
    return pd.read_parquet(path)

def write_parquet(df, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    remove_path(path)
    df.to_parquet(path, index=False)

def write_partitioned_parquet(df, path: Path, partition_cols: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    remove_path(path)
    df.to_parquet(path, index=False, partition_cols=partition_cols)

def expected_timestamps(pd):
    return pd.date_range(
        "2030-01-01 00:00:00",
        "2045-12-31 23:00:00",
        freq="h",
        tz="UTC",
    )


def standardise_timestamp(pd, df, column: str = "timestamp_utc"):
    df[column] = pd.to_datetime(df[column], utc=True)
    return df


def check_required_columns(df, required: Iterable[str], dataset: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"{dataset} is missing required columns: {missing}")


def cross_join(pd, left, right):
    return left.merge(right, how="cross")


def load_and_standardise_inputs(pd, paths: PackagePaths, smr_fleet: Path):
    demand = read_parquet(pd, paths.obj2_demand).copy()
    generation = read_parquet(pd, paths.obj1_supply).copy()
    smr = read_parquet(pd, smr_fleet).copy()
    smr = normalise_smr_fleet_schema(smr)

    check_required_columns(
        demand,
        ["timestamp_utc", "year", "fes_scenario", "climate_member", "demand_mw"],
        "Objective 2 demand",
    )
    check_required_columns(
        generation,
        [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "weather_year",
            "weather_year_role",
            "wind_mw",
            "solar_mw",
            "nuclear_existing_mw",
            "biomass_mw",
            "hydro_mw",
            "other_mw",
            "imports_net_baseline_mw",
        ],
        "Objective 1 generation",
    )
    check_required_columns(
        smr,
        ["timestamp_utc", "year", "fes_scenario", "smr_case"],
        "Owner 2 SMR fleet",
    )

    demand = standardise_timestamp(pd, demand)
    generation = standardise_timestamp(pd, generation)
    smr = standardise_timestamp(pd, smr)

    demand = demand[demand["fes_scenario"].isin(ALLOWED_SCENARIOS)].copy()
    generation = generation[generation["fes_scenario"].isin(ALLOWED_SCENARIOS)].copy()
    smr = smr[smr["fes_scenario"].isin(ALLOWED_SCENARIOS)].copy()

    check_required_columns(
        smr,
        [
            "unit1_delivered_mw",
            "unit2_delivered_mw",
            "unit3_delivered_mw",
            "smr_total_delivered_mw",
        ],
        "Owner 2 SMR fleet after column-name adaptation",
    )

    return demand, generation, smr


def normalise_smr_fleet_schema(smr):
    """Normalise Owner 2 SMR fleet columns for Owner 3 integration.

    The migrated SMR fleet runner writes both legacy/source-style columns
    and canonical Objective 3 delivered-output aliases. Owner 3 uses the
    canonical unit1_delivered_mw/unit2_delivered_mw/unit3_delivered_mw/
    smr_total_delivered_mw columns internally.
    """
    smr = smr.copy()

    alias_pairs = {
        "unit1_delivered_mw": "unit_1_mw",
        "unit2_delivered_mw": "unit_2_mw",
        "unit3_delivered_mw": "unit_3_mw",
        "smr_total_delivered_mw": "total_fleet_mw",
    }

    for canonical, legacy in alias_pairs.items():
        if canonical not in smr.columns and legacy in smr.columns:
            smr[canonical] = smr[legacy]

    legacy_columns = [legacy for legacy in alias_pairs.values() if legacy in smr.columns]
    smr = smr.drop(columns=legacy_columns)

    smr = smr.loc[:, ~smr.columns.duplicated()].copy()
    return smr


def validate_source_inputs(pd, demand, generation, smr) -> dict[str, dict]:
    expected_ts = pd.Index(expected_timestamps(pd))
    results: dict[str, dict] = {}

    source_specs = [
        (
            "obj2_demand",
            demand,
            ["timestamp_utc", "year", "fes_scenario", "climate_member"],
        ),
        (
            "obj1_generation",
            generation,
            ["timestamp_utc", "year", "fes_scenario", "weather_year_role"],
        ),
        (
            "owner2_smr_fleet",
            smr,
            ["timestamp_utc", "year", "fes_scenario", "smr_case"],
        ),
    ]

    for name, df, key_cols in source_specs:
        actual_ts = pd.Index(df["timestamp_utc"].drop_duplicates().sort_values())
        missing_ts = expected_ts.difference(actual_ts)
        extra_ts = actual_ts.difference(expected_ts)
        duplicate_count = int(df.duplicated(key_cols).sum())
        missing_values = int(df.isna().sum().sum())
        results[name] = {
            "rows": len(df),
            "unique_timestamps": int(actual_ts.size),
            "missing_timestamps": int(len(missing_ts)),
            "extra_timestamps": int(len(extra_ts)),
            "duplicate_keys": duplicate_count,
            "missing_values": missing_values,
            "key_cols": "; ".join(key_cols),
        }
        if missing_ts.size or extra_ts.size:
            raise RuntimeError(
                f"{name} timestamp coverage failed: "
                f"missing={len(missing_ts)}, extra={len(extra_ts)}"
            )
        if duplicate_count:
            raise RuntimeError(f"{name} duplicate keys: {duplicate_count}")

    if not (demand["demand_mw"] > 0).all():
        raise RuntimeError("Objective 2 demand contains non-positive demand_mw.")

    unit_total_diff = (
        smr["unit1_delivered_mw"].astype("float64")
        + smr["unit2_delivered_mw"].astype("float64")
        + smr["unit3_delivered_mw"].astype("float64")
        - smr["smr_total_delivered_mw"].astype("float64")
    ).abs()
    max_unit_diff = float(unit_total_diff.max())
    results["owner2_smr_fleet"]["max_unit_sum_diff_mw"] = max_unit_diff
    if max_unit_diff > 1e-6:
        raise RuntimeError(
            f"SMR unit columns do not reconcile to total. max diff={max_unit_diff}"
        )

    return results


def build_scenario_skeleton(pd, demand, generation, smr):
    timestamps = demand[["timestamp_utc", "year"]].drop_duplicates().sort_values(
        ["timestamp_utc"]
    )
    scenarios = demand[["fes_scenario"]].drop_duplicates().sort_values("fes_scenario")
    climate_members = demand[["climate_member"]].drop_duplicates().sort_values(
        "climate_member"
    )
    weather_roles = generation[["weather_year_role"]].drop_duplicates().sort_values(
        "weather_year_role"
    )
    smr_cases = smr[["smr_case"]].drop_duplicates().sort_values("smr_case")

    skeleton = cross_join(pd, timestamps, scenarios)
    skeleton = cross_join(pd, skeleton, climate_members)
    skeleton = cross_join(pd, skeleton, weather_roles)
    skeleton = cross_join(pd, skeleton, smr_cases)

    dimensions = {
        "timestamps": len(timestamps),
        "fes_scenarios": len(scenarios),
        "climate_members": len(climate_members),
        "weather_year_roles": len(weather_roles),
        "smr_cases": len(smr_cases),
        "expected_master_rows": len(skeleton),
        "scenario_values": sorted(scenarios["fes_scenario"].astype(str).tolist()),
        "climate_member_values": sorted(climate_members["climate_member"].astype(str).tolist()),
        "weather_year_role_values": sorted(weather_roles["weather_year_role"].astype(str).tolist()),
        "smr_case_values": sorted(smr_cases["smr_case"].astype(str).tolist()),
    }
    return skeleton, dimensions


def integrate(paths: PackagePaths, smr_fleet: Path) -> dict:
    if not smr_fleet.exists():
        raise FileNotFoundError(f"SMR fleet input is missing: {smr_fleet}")

    pd = require_pandas(paths)
    demand, generation, smr = load_and_standardise_inputs(pd, paths, smr_fleet)
    source_results = validate_source_inputs(pd, demand, generation, smr)
    skeleton, dimensions = build_scenario_skeleton(pd, demand, generation, smr)

    generation_cols = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "weather_year_role",
        "weather_year",
        "wind_mw",
        "solar_mw",
        "nuclear_existing_mw",
        "biomass_mw",
        "hydro_mw",
        "other_mw",
        "imports_net_baseline_mw",
    ]
    optional_generation_cols = ["storage_net_mw", "gas_reference_mw", "coal_reference_mw"]
    generation_cols.extend([c for c in optional_generation_cols if c in generation.columns])

    smr_cols = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "smr_case",
        "unit1_delivered_mw",
        "unit2_delivered_mw",
        "unit3_delivered_mw",
        "smr_total_delivered_mw",
    ]

    master = (
        skeleton.merge(
            demand,
            on=["timestamp_utc", "year", "fes_scenario", "climate_member"],
            how="left",
            validate="many_to_one",
        )
        .merge(
            generation[generation_cols],
            on=["timestamp_utc", "year", "fes_scenario", "weather_year_role"],
            how="left",
            validate="many_to_one",
        )
        .merge(
            smr[smr_cols],
            on=["timestamp_utc", "year", "fes_scenario", "smr_case"],
            how="left",
            validate="many_to_one",
        )
    )

    supply_cols_for_sum = [
        "wind_mw",
        "solar_mw",
        "nuclear_existing_mw",
        "biomass_mw",
        "hydro_mw",
        "other_mw",
    ]
    master["exogenous_supply_mw"] = master[supply_cols_for_sum].sum(axis=1).astype(
        "float64"
    )

    final_key = [
        "timestamp_utc",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
    ]
    required_final = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "weather_year",
        "smr_case",
        "demand_mw",
        "wind_mw",
        "solar_mw",
        "nuclear_existing_mw",
        "biomass_mw",
        "hydro_mw",
        "other_mw",
        "imports_net_baseline_mw",
        "unit1_delivered_mw",
        "unit2_delivered_mw",
        "unit3_delivered_mw",
        "smr_total_delivered_mw",
        "exogenous_supply_mw",
    ]
    final_cols = required_final + [c for c in optional_generation_cols if c in master.columns]

    missing_required = master[required_final].isna().sum()
    missing_required = missing_required[missing_required > 0]
    if len(missing_required):
        raise RuntimeError(
            f"Final master has missing required values: {missing_required.to_dict()}"
        )

    duplicate_final = int(master.duplicated(final_key).sum())
    if duplicate_final:
        raise RuntimeError(f"Final master duplicate keys: {duplicate_final}")

    output_path = paths.outputs_dir / "grid_master_hourly_2030_2045"
    legacy_output_path = paths.outputs_dir / "grid_master_hourly_2030_2045.parquet"
    partitioned_output_path = (
        paths.outputs_dir / "grid_master_hourly_2030_2045_partitioned"
    )
    write_parquet(master[final_cols], output_path)
    write_parquet(master[final_cols], legacy_output_path)
    write_partitioned_parquet(
        master[final_cols],
        partitioned_output_path,
        ["year", "fes_scenario", "smr_case"],
    )

    final_results = {
        "master_rows": len(master),
        "master_columns": len(final_cols),
        "duplicate_final_keys": duplicate_final,
        "missing_required_values": int(master[required_final].isna().sum().sum()),
        "final_key": "; ".join(final_key),
        "output_path": str(output_path),
        "legacy_output_path": str(legacy_output_path),
        "partitioned_output_path": str(partitioned_output_path),
        "partition_columns": "year; fes_scenario; smr_case",
    }

    write_dynamic_qa(paths, smr_fleet, source_results, dimensions, final_results)
    return {
        "sources": source_results,
        "dimensions": dimensions,
        "final": final_results,
    }


def write_dynamic_qa(
    paths: PackagePaths,
    smr_fleet: Path,
    source_results: dict[str, dict],
    dimensions: dict,
    final_results: dict,
) -> None:
    manifest = build_manifest(paths, smr_fleet)
    write_static_outputs(paths, smr_fleet)

    write_csv(
        paths.qa_dir / "row_count_reconciliation.csv",
        [
            {
                "dataset": name,
                "expected_rows": "native upstream row count",
                "observed_rows": str(info["rows"]),
                "status": "pass",
                "notes": f"Key columns: {info['key_cols']}",
            }
            for name, info in source_results.items()
        ]
        + [
            {
                "dataset": "grid_master_hourly_2030_2045",
                "expected_rows": str(dimensions["expected_master_rows"]),
                "observed_rows": str(final_results["master_rows"]),
                "status": "pass"
                if final_results["master_rows"] == dimensions["expected_master_rows"]
                else "fail",
                "notes": "Full cross product: timestamps x scenarios x climate_members x weather_year_roles x smr_cases.",
            }
        ],
        ["dataset", "expected_rows", "observed_rows", "status", "notes"],
    )

    write_csv(
        paths.qa_dir / "timestamp_coverage_check.csv",
        [
            {
                "dataset": name,
                "expected_start_utc": "2030-01-01 00:00:00+00:00",
                "expected_end_utc": "2045-12-31 23:00:00+00:00",
                "unique_timestamps": str(info["unique_timestamps"]),
                "missing_timestamps": str(info["missing_timestamps"]),
                "extra_timestamps": str(info["extra_timestamps"]),
                "status": "pass"
                if info["missing_timestamps"] == 0 and info["extra_timestamps"] == 0
                else "fail",
                "notes": "Validated directly from parquet input.",
            }
            for name, info in source_results.items()
        ],
        [
            "dataset",
            "expected_start_utc",
            "expected_end_utc",
            "unique_timestamps",
            "missing_timestamps",
            "extra_timestamps",
            "status",
            "notes",
        ],
    )

    write_csv(
        paths.qa_dir / "scenario_coverage_check.csv",
        [
            {
                "dimension": "fes_scenario",
                "count": str(dimensions["fes_scenarios"]),
                "values": "; ".join(dimensions["scenario_values"]),
                "status": "pass",
                "notes": "Allowed FES scenarios only.",
            },
            {
                "dimension": "climate_member",
                "count": str(dimensions["climate_members"]),
                "values": "; ".join(dimensions["climate_member_values"]),
                "status": "pass",
                "notes": "Objective 2 selected UKCP18 climate members.",
            },
            {
                "dimension": "weather_year_role",
                "count": str(dimensions["weather_year_roles"]),
                "values": "; ".join(dimensions["weather_year_role_values"]),
                "status": "pass",
                "notes": "Objective 1 retained supply weather roles.",
            },
            {
                "dimension": "smr_case",
                "count": str(dimensions["smr_cases"]),
                "values": "; ".join(dimensions["smr_case_values"]),
                "status": "pass",
                "notes": "Owner 2 supplied SMR cases. Current file includes both commissioning cases.",
            },
        ],
        ["dimension", "count", "values", "status", "notes"],
    )

    duplicate_rows = [
        {
            "dataset": name,
            "key_columns": info["key_cols"],
            "duplicate_count": str(info["duplicate_keys"]),
            "status": "pass" if info["duplicate_keys"] == 0 else "fail",
            "notes": "Validated directly from parquet input.",
        }
        for name, info in source_results.items()
    ]
    duplicate_rows.append(
        {
            "dataset": "grid_master_hourly_2030_2045",
            "key_columns": final_results["final_key"],
            "duplicate_count": str(final_results["duplicate_final_keys"]),
            "status": "pass" if final_results["duplicate_final_keys"] == 0 else "fail",
            "notes": "Final agreed Owner 3 key.",
        }
    )
    write_csv(
        paths.qa_dir / "duplicate_key_check.csv",
        duplicate_rows,
        ["dataset", "key_columns", "duplicate_count", "status", "notes"],
    )

    missing_rows = [
        {
            "dataset": name,
            "columns_checked": "all source columns",
            "missing_values": str(info["missing_values"]),
            "status": "pass" if info["missing_values"] == 0 else "review",
            "notes": "Validated directly from parquet input.",
        }
        for name, info in source_results.items()
    ]
    missing_rows.append(
        {
            "dataset": "grid_master_hourly_2030_2045",
            "columns_checked": "required final columns",
            "missing_values": str(final_results["missing_required_values"]),
            "status": "pass" if final_results["missing_required_values"] == 0 else "fail",
            "notes": "Required fields after all joins.",
        }
    )
    write_csv(
        paths.qa_dir / "missing_value_check.csv",
        missing_rows,
        ["dataset", "columns_checked", "missing_values", "status", "notes"],
    )

    write_csv(
        paths.outputs_dir / "integration_summary.csv",
        [
            {
                "dataset": "grid_master_hourly_2030_2045",
                "check_name": "final_rows",
                "status": "pass",
                "expected": str(dimensions["expected_master_rows"]),
                "observed": str(final_results["master_rows"]),
                "notes": "Full retained scenario skeleton.",
            },
            {
                "dataset": "grid_master_hourly_2030_2045",
                "check_name": "final_key",
                "status": "pass",
                "expected": "timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case",
                "observed": final_results["final_key"].replace("; ", " + "),
                "notes": "Matches team-agreed Owner 3 key.",
            },
            {
                "dataset": "owner2_smr_fleet",
                "check_name": "unit_total_reconciliation",
                "status": "pass",
                "expected": "max unit sum diff <= 1e-6 MW",
                "observed": str(source_results["owner2_smr_fleet"].get("max_unit_sum_diff_mw", "")),
                "notes": "unit1 + unit2 + unit3 equals smr_total_delivered_mw.",
            },
            {
                "dataset": "grid_master_hourly_2030_2045_partitioned",
                "check_name": "partitioned_parquet_output",
                "status": "pass",
                "expected": "partitioned Parquet dataset",
                "observed": final_results["partition_columns"].replace("; ", " + "),
                "notes": "Performance-oriented companion output; single-file parquet is retained for simple handoff.",
            },
        ],
        ["dataset", "check_name", "status", "expected", "observed", "notes"],
    )

    write_qa_markdown(paths, smr_fleet, manifest, dimensions, final_results, completed=True)


def write_stage_outputs(paths: PackagePaths, smr_fleet: Path) -> None:
    write_static_outputs(paths, smr_fleet)
    manifest = build_manifest(paths, smr_fleet)
    obj2_qa = read_obj2_qa(paths.obj2_qa_summary)

    write_csv(
        paths.outputs_dir / "integration_summary.csv",
        [
            {
                "dataset": "obj1_supply",
                "check_name": "file_available",
                "status": "pass" if paths.obj1_supply.exists() else "fail",
                "expected": "file exists",
                "observed": "file exists" if paths.obj1_supply.exists() else "missing",
                "notes": "Direct parquet QA runs in integrate mode.",
            },
            {
                "dataset": "obj2_demand",
                "check_name": "file_available",
                "status": "pass" if paths.obj2_demand.exists() else "fail",
                "expected": "file exists",
                "observed": "file exists" if paths.obj2_demand.exists() else "missing",
                "notes": "Objective 2 companion QA summary is available.",
            },
            {
                "dataset": "owner2_smr_fleet",
                "check_name": "file_available",
                "status": "pass" if smr_fleet.exists() else "blocked",
                "expected": "SMR fleet scenario parquet exists",
                "observed": "file exists" if smr_fleet.exists() else "missing",
                "notes": "Use --integrate to validate schema and build final master.",
            },
            {
                "dataset": "obj1_obj2",
                "check_name": "join_key_policy",
                "status": "pass",
                "expected": "retain climate_member and weather_year_role",
                "observed": "team agreed full cross-product key",
                "notes": "Final key includes timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case.",
            },
        ],
        ["dataset", "check_name", "status", "expected", "observed", "notes"],
    )
    write_csv(
        paths.qa_dir / "row_count_reconciliation.csv",
        [
            {
                "dataset": "obj2_demand",
                "expected_rows": str(EXPECTED_HOURS_2030_2045 * len(ALLOWED_SCENARIOS) * 3),
                "observed_rows": obj2_qa.get("n_rows", "not_read_directly"),
                "status": "pass"
                if obj2_qa.get("n_rows")
                == str(EXPECTED_HOURS_2030_2045 * len(ALLOWED_SCENARIOS) * 3)
                else "not_run",
                "notes": "Read from Objective 2 QA summary.",
            },
            {
                "dataset": "obj1_supply",
                "expected_rows": str(EXPECTED_HOURS_2030_2045 * len(ALLOWED_SCENARIOS) * 3),
                "observed_rows": "not_read_directly",
                "status": "not_run",
                "notes": "Direct parquet QA runs in integrate mode.",
            },
            {
                "dataset": "owner2_smr_fleet",
                "expected_rows": "hours x scenarios x smr_cases",
                "observed_rows": "not_read_directly" if smr_fleet.exists() else "missing",
                "status": "not_run" if smr_fleet.exists() else "blocked",
                "notes": "Direct parquet QA runs in integrate mode.",
            },
        ],
        ["dataset", "expected_rows", "observed_rows", "status", "notes"],
    )
    write_qa_markdown(paths, smr_fleet, manifest, {}, {}, completed=False)


def write_qa_markdown(
    paths: PackagePaths,
    smr_fleet: Path,
    manifest: list[dict],
    dimensions: dict,
    final_results: dict,
    completed: bool,
) -> None:
    status = "completed integration log" if completed else "stage-check log"
    text = f"""# Objective 3 Owner 3 - Input Alignment QA Log

Generated by the reproducible Objective 3 validation workflow.

## Scope

Owner 3 validates alignment across Objective 1 supply, Objective 2 demand,
and Owner 2 SMR fleet data, then builds the integrated hourly master dataset
for 2030-2045.

This is a {status}.

## Team-Agreed Join Strategy

The team decision is to keep both uncertainty dimensions:

```text
timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case
```

Source keys:

```text
demand:     timestamp_utc + year + fes_scenario + climate_member
generation: timestamp_utc + year + fes_scenario + weather_year_role
smr:        timestamp_utc + year + fes_scenario + smr_case
```

Owner 3 builds a scenario skeleton using the cross product of timestamps,
FES scenarios, climate members, weather-year roles, and SMR cases, then joins
each source onto that skeleton using its native key.

## Input Availability

| Input | Status | Notes |
|---|---:|---|
"""
    for row in manifest:
        text += f"| `{row['input_name']}` | {row['status']} | {row['notes']} |\n"

    if completed:
        text += f"""

## Scenario Dimensions

- Unique timestamps: `{dimensions['timestamps']}`
- FES scenarios: `{dimensions['fes_scenarios']}` ({', '.join(dimensions['scenario_values'])})
- Climate members: `{dimensions['climate_members']}` ({', '.join(dimensions['climate_member_values'])})
- Weather-year roles: `{dimensions['weather_year_roles']}` ({', '.join(dimensions['weather_year_role_values'])})
- SMR cases: `{dimensions['smr_cases']}` ({', '.join(dimensions['smr_case_values'])})

## Final Integration Result

- Output: `Outputs/grid_master_hourly_2030_2045.parquet`
- Partitioned output: `Outputs/grid_master_hourly_2030_2045_partitioned/`
- Partition columns: `{final_results['partition_columns']}`
- Rows: `{final_results['master_rows']}`
- Columns: `{final_results['master_columns']}`
- Duplicate final keys: `{final_results['duplicate_final_keys']}`
- Missing required values: `{final_results['missing_required_values']}`

Final status: PASS.
"""
    else:
        text += f"""

## Current Status

Final `grid_master_hourly_2030_2045.parquet` has not been generated by this
stage-check run. Run `--integrate` to validate the upstream parquet files and
build the final master table.

Expected SMR input path:

`{rel(smr_fleet, paths.team_folder)}`
"""

    (paths.qa_dir / "input_alignment_qa_log.md").write_text(text, encoding="utf-8", newline="\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--stage-check",
        action="store_true",
        help="Write metadata and QA placeholders without building final parquet.",
    )
    mode.add_argument(
        "--integrate",
        action="store_true",
        help="Build the final grid master parquet using the team-agreed full key.",
    )
    parser.add_argument(
        "--smr-fleet",
        type=Path,
        default=None,
        help="Path to Owner 2 SMR fleet parquet. Defaults to the discovered extracted file.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if not args.stage_check and not args.integrate:
        args.stage_check = True

    paths = resolve_paths()
    ensure_dirs(paths)
    smr_fleet = discover_smr_fleet(paths, args.smr_fleet)

    if args.stage_check:
        write_stage_outputs(paths, smr_fleet)
        print(f"Stage-check outputs written to: {paths.owner_dir}")

    if args.integrate:
        result = integrate(paths, smr_fleet)
        print(f"Final grid master parquet written to: {paths.outputs_dir}")
        print(f"Rows: {result['final']['master_rows']}")
        print(f"Columns: {result['final']['master_columns']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
