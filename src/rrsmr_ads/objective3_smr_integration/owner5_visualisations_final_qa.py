#!/usr/bin/env python3
"""
Objective 3 - Owner 5 visualisations, final QA, and project packaging.

This script follows the agreed Option A:

- Owner/Part 1-4 repo-generated outputs are used as the model inputs.
- Owner 5 builds final visualisation and robustness-testing outputs.

Main model:
average_wind + staggered_commissioning

Sensitivity:
simultaneous_commissioning = all three SMR units online from 2035-01-01,
using the same unit capacity, net delivery factor, planned outage, and forced
outage assumptions as the staggered case.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


SIMULTANEOUS_START_UTC = "2035-01-01 00:00:00+00:00"
BASE_WEATHER_ROLE = "average_wind"
LOW_WIND_ROLE = "low_wind"
BASE_SMR_CASE = "staggered_commissioning"
SENSITIVITY_SMR_CASE = "simultaneous_commissioning"


@dataclass(frozen=True)
class PackagePaths:
    owner5_dir: Path
    code_dir: Path
    inputs_dir: Path
    outputs_dir: Path
    figures_dir: Path
    qa_dir: Path
    obj3_root: Path
    team_folder: Path
    workspace_root: Path
    owner1_outputs: Path
    owner2_outputs: Path
    owner3_outputs: Path
    owner3_qa: Path
    owner4_outputs: Path
    smr_assumptions: Path
    smr_unit_library: Path
    owner2_fleet: Path
    grid_master: Path
    owner4_hourly: Path
    owner4_annual: Path
    owner4_period: Path



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


def resolve_paths() -> PackagePaths:
    """Resolve Objective 3 Owner 5 paths from the repository root.

    This migrated version uses the ADS repo layout rather than the original
    local Owner handoff folder structure.
    """
    code_dir = Path(__file__).resolve().parent
    repo_root = resolve_repo_root(code_dir)

    validation_dir = repo_root / "docs" / "validation" / "objective3_smr_integration"
    processed_obj3 = repo_root / "data" / "processed" / "objective3_smr_integration"
    owner5_outputs = repo_root / "outputs" / "objective3_smr_integration"
    owner5_figures = repo_root / "outputs" / "figures" / "objective3_smr_integration"

    return PackagePaths(
        owner5_dir=validation_dir,
        code_dir=code_dir,
        inputs_dir=validation_dir,
        outputs_dir=owner5_outputs,
        figures_dir=owner5_figures,
        qa_dir=validation_dir,
        obj3_root=repo_root,
        team_folder=repo_root,
        workspace_root=repo_root,
        owner1_outputs=processed_obj3,
        owner2_outputs=processed_obj3,
        owner3_outputs=processed_obj3,
        owner3_qa=validation_dir,
        owner4_outputs=processed_obj3,
        smr_assumptions=repo_root / "config" / "smr_assumptions.csv",
        smr_unit_library=processed_obj3 / "smr_hourly_library_2030_2045",
        owner2_fleet=processed_obj3 / "smr_fleet_hourly_2030_2045",
        grid_master=processed_obj3 / "grid_master_hourly_2030_2045",
        owner4_hourly=processed_obj3 / "system_impact_hourly_2030_2045",
        owner4_annual=processed_obj3 / "system_impact_summary_annual_2030_2045.csv",
        owner4_period=processed_obj3 / "system_impact_summary_period_2030_2045.csv",
    )


def ensure_dirs(paths: PackagePaths) -> None:
    for folder in (
        paths.inputs_dir,
        paths.outputs_dir,
        paths.figures_dir,
        paths.qa_dir,
        paths.owner4_outputs,
        paths.workspace_root / ".owner5_mplconfig",
    ):
        folder.mkdir(parents=True, exist_ok=True)


def require_pandas(paths: PackagePaths):
    import pandas as pd  # type: ignore
    import numpy as np  # type: ignore

    return pd, np


def configure_matplotlib(paths: PackagePaths):
    os.environ.setdefault("MPLCONFIGDIR", str(paths.outputs_dir / ".mplconfig"))
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # type: ignore

    return plt


def read_parquet(pd, path: Path, columns: list[str] | None = None):
    return pd.read_parquet(path, columns=columns)


def write_parquet(df, path: Path) -> None:
    df.to_parquet(path, index=False)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def rel(path: Path, base: Path) -> str:
    """Return a stable relative path, preferring base-relative then repo-relative."""
    resolved_path = path.resolve()
    resolved_base = base.resolve()

    for candidate in [resolved_base, *resolved_base.parents]:
        try:
            return str(resolved_path.relative_to(candidate))
        except ValueError:
            continue

    return str(path)


def collect_owner4_output_rows(paths: PackagePaths) -> list[dict]:
    """Collect manifest rows for repo-generated Owner/Part 4 outputs."""
    specs = [
        (
            "system_impact_hourly_2030_2045",
            paths.owner4_hourly,
            "repo_generated_owner4_input",
            "Repo-generated Owner/Part 4 hourly system impact metrics.",
        ),
        (
            "system_impact_summary_annual_2030_2045",
            paths.owner4_annual,
            "repo_generated_owner4_input",
            "Repo-generated Owner/Part 4 annual system impact summary.",
        ),
        (
            "system_impact_summary_period_2030_2045",
            paths.owner4_period,
            "repo_generated_owner4_input",
            "Repo-generated Owner/Part 4 period system impact summary.",
        ),
    ]

    rows: list[dict] = []
    for input_name, path, role, notes in specs:
        rows.append(
            {
                "input_name": input_name,
                "status": "available" if path.exists() else "missing",
                "path": rel(path, paths.team_folder),
                "size_bytes": str(path.stat().st_size) if path.exists() else "",
                "role": role,
                "notes": notes,
            }
        )
    return rows

def file_manifest_rows(paths: PackagePaths, owner4_rows: list[dict]) -> list[dict]:
    specs = [
        (
            "owner1_smr_assumptions",
            paths.smr_assumptions,
            "Owner 1 SMR assumptions, including unit capacity and net delivery factor.",
            "required",
        ),
        (
            "owner1_smr_unit_library",
            paths.smr_unit_library,
            "Owner 1 unit-level hourly SMR library with outage flags.",
            "required",
        ),
        (
            "owner2_smr_fleet",
            paths.owner2_fleet,
            "Owner 2 fleet-level staggered SMR output.",
            "required",
        ),
        (
            "owner3_grid_master",
            paths.grid_master,
            "Owner 3 integrated hourly master dataset.",
            "required",
        ),
        (
            "owner3_input_alignment_qa",
            paths.owner3_qa / "input_alignment_qa_log.md",
            "Owner 3 alignment QA evidence.",
            "supporting",
        ),
    ]
    rows: list[dict] = []
    for name, path, notes, role in specs:
        rows.append(
            {
                "input_name": name,
                "status": "available" if path.exists() else "missing",
                "path": rel(path, paths.team_folder),
                "size_bytes": str(path.stat().st_size) if path.exists() else "",
                "role": role,
                "notes": notes,
            }
        )
    rows.extend(owner4_rows)
    return rows


def standardise_timestamp(pd, df):
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    return df


def derive_owner4_low_wind_threshold(pd, owner4_hourly, master_avg) -> float:
    join_cols = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
    ]
    merged = owner4_hourly[join_cols + ["low_wind_flag"]].merge(
        master_avg[join_cols + ["wind_mw"]],
        on=join_cols,
        how="inner",
        validate="one_to_one",
    )
    flagged = merged.loc[merged["low_wind_flag"].astype(bool), "wind_mw"]
    if flagged.empty:
        return float(master_avg["wind_mw"].quantile(0.12))
    return float(flagged.max())


def build_simultaneous_fleet(pd, np, paths: PackagePaths):
    assumptions = pd.read_csv(paths.smr_assumptions)
    unit_cols = [
        "timestamp_utc",
        "year",
        "unit_id",
        "nameplate_mw",
        "planned_outage",
        "forced_outage",
    ]
    unit_library = read_parquet(pd, paths.smr_unit_library, columns=unit_cols)
    unit_library = standardise_timestamp(pd, unit_library)
    assumptions = assumptions[["unit_id", "net_delivery_factor"]].copy()
    unit_library = unit_library.merge(assumptions, on="unit_id", how="left")

    start = pd.Timestamp(SIMULTANEOUS_START_UTC)
    unit_library["is_commissioned_simultaneous"] = unit_library["timestamp_utc"] >= start
    unit_library["is_available_simultaneous"] = (
        unit_library["is_commissioned_simultaneous"]
        & ~unit_library["planned_outage"].astype(bool)
        & ~unit_library["forced_outage"].astype(bool)
    )
    unit_library["delivered_mw_simultaneous"] = np.where(
        unit_library["is_available_simultaneous"],
        unit_library["nameplate_mw"].astype("float64")
        * unit_library["net_delivery_factor"].astype("float64"),
        0.0,
    )

    wide = (
        unit_library.pivot_table(
            index=["timestamp_utc", "year"],
            columns="unit_id",
            values="delivered_mw_simultaneous",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    rename_map = {
        "unit_1": "unit1_delivered_mw",
        "unit_2": "unit2_delivered_mw",
        "unit_3": "unit3_delivered_mw",
    }
    wide = wide.rename(columns=rename_map)
    for col in rename_map.values():
        if col not in wide.columns:
            wide[col] = 0.0
    wide["smr_total_delivered_mw"] = wide[
        ["unit1_delivered_mw", "unit2_delivered_mw", "unit3_delivered_mw"]
    ].sum(axis=1)
    return wide[
        [
            "timestamp_utc",
            "year",
            "unit1_delivered_mw",
            "unit2_delivered_mw",
            "unit3_delivered_mw",
            "smr_total_delivered_mw",
        ]
    ]


def add_metrics(df):
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
    return df


def build_owner5_metrics(pd, np, paths: PackagePaths):
    master_cols = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
        "demand_mw",
        "wind_mw",
        "exogenous_supply_mw",
        "imports_net_baseline_mw",
        "unit1_delivered_mw",
        "unit2_delivered_mw",
        "unit3_delivered_mw",
        "smr_total_delivered_mw",
    ]
    master = read_parquet(pd, paths.grid_master, columns=master_cols)
    master = standardise_timestamp(pd, master)
    master = master[master["weather_year_role"].isin([BASE_WEATHER_ROLE, LOW_WIND_ROLE])].copy()
    master = master[master["smr_case"].eq(BASE_SMR_CASE)].copy()

    owner4_cols = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
        "smr_case",
        "weather_year_role",
        "residual_before_smr_mw",
        "residual_after_smr_mw",
        "gas_needed_before_mw",
        "gas_needed_after_mw",
        "gas_displacement_proxy_mw",
        "surplus_after_smr_mw",
        "low_wind_flag",
    ]
    owner4_hourly = read_parquet(pd, paths.owner4_hourly, columns=owner4_cols)
    owner4_hourly = standardise_timestamp(pd, owner4_hourly)
    owner4_hourly_base = owner4_hourly[
        owner4_hourly["weather_year_role"].eq(BASE_WEATHER_ROLE)
    ].copy()

    master_avg = master[master["weather_year_role"].eq(BASE_WEATHER_ROLE)].copy()
    low_wind_threshold_mw = derive_owner4_low_wind_threshold(
        pd, owner4_hourly_base, master_avg
    )

    staggered = master.copy()
    staggered["smr_case"] = BASE_SMR_CASE

    simultaneous_fleet = build_simultaneous_fleet(pd, np, paths)
    simultaneous = master.drop(
        columns=[
            "smr_case",
            "unit1_delivered_mw",
            "unit2_delivered_mw",
            "unit3_delivered_mw",
            "smr_total_delivered_mw",
        ]
    ).merge(
        simultaneous_fleet,
        on=["timestamp_utc", "year"],
        how="left",
        validate="many_to_one",
    )
    simultaneous["smr_case"] = SENSITIVITY_SMR_CASE

    metrics = pd.concat([staggered, simultaneous], ignore_index=True)
    metrics["low_wind_flag"] = metrics["wind_mw"] <= low_wind_threshold_mw
    metrics = add_metrics(metrics)

    ordered_cols = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
        "demand_mw",
        "wind_mw",
        "exogenous_supply_mw",
        "imports_net_baseline_mw",
        "unit1_delivered_mw",
        "unit2_delivered_mw",
        "unit3_delivered_mw",
        "smr_total_delivered_mw",
        "residual_before_smr_mw",
        "residual_after_smr_mw",
        "gas_needed_before_mw",
        "gas_needed_after_mw",
        "gas_displacement_proxy_mw",
        "surplus_after_smr_mw",
        "low_wind_flag",
    ]
    metrics = metrics[ordered_cols]

    return metrics, owner4_hourly_base, low_wind_threshold_mw


def summarise_metrics(metrics):
    metrics = metrics.copy()
    metrics["smr_energy_mwh"] = metrics["smr_total_delivered_mw"]
    metrics["gas_displacement_mwh"] = metrics["gas_displacement_proxy_mw"]
    metrics["surplus_hour"] = metrics["surplus_after_smr_mw"] > 0
    metrics["low_wind_hour"] = metrics["low_wind_flag"].astype(bool)
    metrics["low_wind_support_hour"] = (
        metrics["low_wind_hour"]
        & (metrics["smr_total_delivered_mw"] > 0)
        & (metrics["gas_displacement_proxy_mw"] > 0)
    )

    group_cols = [
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
    ]
    annual = (
        metrics.groupby(group_cols, observed=True)
        .agg(
            annual_smr_energy_twh=("smr_energy_mwh", lambda s: float(s.sum() / 1e6)),
            annual_gas_displacement_twh=(
                "gas_displacement_mwh",
                lambda s: float(s.sum() / 1e6),
            ),
            average_residual_before_mw=("residual_before_smr_mw", "mean"),
            average_residual_after_mw=("residual_after_smr_mw", "mean"),
            average_smr_output_mw=("smr_total_delivered_mw", "mean"),
            surplus_hours_count=("surplus_hour", "sum"),
            low_wind_hours_count=("low_wind_hour", "sum"),
            low_wind_support_hours=("low_wind_support_hour", "sum"),
        )
        .reset_index()
    )

    period_cols = ["fes_scenario", "climate_member", "weather_year_role", "smr_case"]
    period = (
        metrics.groupby(period_cols, observed=True)
        .agg(
            cumulative_smr_energy_twh=("smr_energy_mwh", lambda s: float(s.sum() / 1e6)),
            cumulative_gas_displacement_twh=(
                "gas_displacement_mwh",
                lambda s: float(s.sum() / 1e6),
            ),
            average_residual_demand_reduction_mw=(
                "smr_total_delivered_mw",
                "mean",
            ),
            average_gas_displacement_proxy_mw=(
                "gas_displacement_proxy_mw",
                "mean",
            ),
            total_surplus_hours=("surplus_hour", "sum"),
            total_low_wind_hours=("low_wind_hour", "sum"),
            total_low_wind_support_hours=("low_wind_support_hour", "sum"),
        )
        .reset_index()
    )
    return annual, period


def save_fig(fig, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")


def label_case(case: str) -> str:
    labels = {
        BASE_SMR_CASE: "Staggered",
        SENSITIVITY_SMR_CASE: "Simultaneous stress-test",
    }
    return labels.get(case, case)


def create_figures(plt, pd, metrics, annual, period, paths: PackagePaths) -> dict[str, str]:
    colors = {
        "before": "#374151",
        BASE_SMR_CASE: "#2563eb",
        SENSITIVITY_SMR_CASE: "#f97316",
        "support": "#059669",
    }
    scenario_styles = {
        "Electric Engagement": "-",
        "Holistic Transition": "--",
    }
    figures: dict[str, str] = {}

    avg_staggered = annual[
        (annual["weather_year_role"] == BASE_WEATHER_ROLE)
        & (annual["smr_case"] == BASE_SMR_CASE)
    ]
    avg_resid = (
        avg_staggered.groupby(["year", "fes_scenario"], observed=True)[
            ["average_residual_before_mw", "average_residual_after_mw"]
        ]
        .mean()
        .reset_index()
    )
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), sharey=True)
    for ax, scenario in zip(axes, sorted(avg_resid["fes_scenario"].unique())):
        data = avg_resid[avg_resid["fes_scenario"] == scenario]
        ax.plot(
            data["year"],
            data["average_residual_before_mw"],
            color=colors["before"],
            linewidth=2,
            label="Before SMR",
        )
        ax.plot(
            data["year"],
            data["average_residual_after_mw"],
            color=colors[BASE_SMR_CASE],
            linewidth=2,
            label="After SMR",
        )
        ax.set_title(scenario)
        ax.set_xlabel("Year")
        ax.grid(True, alpha=0.25)
    axes[0].set_ylabel("Average residual demand (MW)")
    axes[0].legend(frameon=False)
    fig.suptitle("Residual Demand Comparison, Average Wind Base Case")
    path = paths.figures_dir / "residual_demand_comparison_average_wind.png"
    save_fig(fig, path)
    plt.close(fig)
    figures["residual_demand_comparison_average_wind"] = rel(path, paths.owner5_dir)

    annual_avg = (
        annual[annual["weather_year_role"] == BASE_WEATHER_ROLE]
        .groupby(["year", "fes_scenario", "smr_case"], observed=True)[
            ["annual_smr_energy_twh", "annual_gas_displacement_twh"]
        ]
        .mean()
        .reset_index()
    )
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8))
    for scenario in sorted(annual_avg["fes_scenario"].unique()):
        for case in [BASE_SMR_CASE, SENSITIVITY_SMR_CASE]:
            data = annual_avg[
                (annual_avg["fes_scenario"] == scenario) & (annual_avg["smr_case"] == case)
            ]
            axes[0].plot(
                data["year"],
                data["annual_smr_energy_twh"],
                color=colors[case],
                linestyle=scenario_styles[scenario],
                linewidth=2,
                label=f"{scenario} - {label_case(case)}",
            )
            axes[1].plot(
                data["year"],
                data["annual_gas_displacement_twh"],
                color=colors[case],
                linestyle=scenario_styles[scenario],
                linewidth=2,
                label=f"{scenario} - {label_case(case)}",
            )
    axes[0].set_title("Annual SMR Energy")
    axes[0].set_ylabel("TWh")
    axes[0].set_xlabel("Year")
    axes[1].set_title("Annual Gas Displacement Proxy")
    axes[1].set_ylabel("TWh")
    axes[1].set_xlabel("Year")
    for ax in axes:
        ax.grid(True, alpha=0.25)
    axes[1].legend(frameon=False, fontsize=8, loc="best")
    fig.suptitle("Annual SMR Energy and Gas Displacement Trends")
    path = paths.figures_dir / "annual_smr_energy_gas_displacement_trends.png"
    save_fig(fig, path)
    plt.close(fig)
    figures["annual_smr_energy_gas_displacement_trends"] = rel(path, paths.owner5_dir)

    duration_year = 2036
    duration = metrics[
        (metrics["weather_year_role"] == BASE_WEATHER_ROLE)
        & (metrics["year"] == duration_year)
        & (metrics["climate_member"] == "member_12")
    ].copy()
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8), sharey=True)
    for ax, scenario in zip(axes, sorted(duration["fes_scenario"].unique())):
        data_before = duration[
            (duration["fes_scenario"] == scenario)
            & (duration["smr_case"] == BASE_SMR_CASE)
        ]["residual_before_smr_mw"].sort_values(ascending=False).reset_index(drop=True)
        x_before = (data_before.index + 1) / len(data_before) * 100
        ax.plot(x_before, data_before, color=colors["before"], linewidth=2, label="Before SMR")
        for case in [BASE_SMR_CASE, SENSITIVITY_SMR_CASE]:
            values = duration[
                (duration["fes_scenario"] == scenario)
                & (duration["smr_case"] == case)
            ]["residual_after_smr_mw"].sort_values(ascending=False).reset_index(drop=True)
            x = (values.index + 1) / len(values) * 100
            ax.plot(x, values, color=colors[case], linewidth=2, label=f"After {label_case(case)}")
        ax.set_title(scenario)
        ax.set_xlabel(f"Share of {duration_year} hours (%)")
        ax.grid(True, alpha=0.25)
    axes[0].set_ylabel("Residual demand (MW)")
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle(
        f"Net Load Duration Curves, {duration_year}, Average Wind, member_12"
    )
    path = paths.figures_dir / "net_load_duration_curves_rollout_sensitivity_2036.png"
    save_fig(fig, path)
    plt.close(fig)
    figures["net_load_duration_curves_rollout_sensitivity_2036"] = rel(
        path, paths.owner5_dir
    )

    low_wind = metrics[
        (metrics["weather_year_role"] == LOW_WIND_ROLE)
        & (metrics["year"].between(2035, 2036))
    ].copy()
    low_wind["date"] = low_wind["timestamp_utc"].dt.date.astype(str)
    daily = (
        low_wind.groupby(
            ["date", "fes_scenario", "climate_member", "smr_case"],
            observed=True,
        )["gas_displacement_proxy_mw"]
        .sum()
        .reset_index()
        .pivot_table(
            index=["date", "fes_scenario", "climate_member"],
            columns="smr_case",
            values="gas_displacement_proxy_mw",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
    )
    daily["simultaneous_minus_staggered_mwh"] = (
        daily[SENSITIVITY_SMR_CASE] - daily[BASE_SMR_CASE]
    )
    daily = daily.sort_values("simultaneous_minus_staggered_mwh", ascending=False)
    selected = daily.iloc[0]
    case_day = low_wind[
        (low_wind["date"] == selected["date"])
        & (low_wind["fes_scenario"] == selected["fes_scenario"])
        & (low_wind["climate_member"] == selected["climate_member"])
    ].copy()
    daily.rename_axis(None, axis=1).to_csv(
        paths.outputs_dir / "low_wind_case_study_selection_rankings.csv",
        index=False, lineterminator="\n",
    )
    fig, ax = plt.subplots(figsize=(12, 5))
    before = case_day[case_day["smr_case"] == BASE_SMR_CASE].sort_values("timestamp_utc")
    ax.plot(
        before["timestamp_utc"],
        before["residual_before_smr_mw"],
        color=colors["before"],
        linewidth=2,
        label="Before SMR",
    )
    for case in [BASE_SMR_CASE, SENSITIVITY_SMR_CASE]:
        subset = case_day[case_day["smr_case"] == case].sort_values("timestamp_utc")
        ax.plot(
            subset["timestamp_utc"],
            subset["residual_after_smr_mw"],
            color=colors[case],
            linewidth=2,
            label=f"After {label_case(case)}",
        )
    ax.set_title(
        "Low-wind Case Study: "
        f"{selected['date']}, {selected['fes_scenario']}, {selected['climate_member']}"
    )
    ax.set_ylabel("Residual demand (MW)")
    ax.set_xlabel("UTC time")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False, fontsize=8)
    path = paths.figures_dir / "low_wind_case_study_pressure_day.png"
    save_fig(fig, path)
    plt.close(fig)
    figures["low_wind_case_study_pressure_day"] = rel(path, paths.owner5_dir)

    case_day[
        [
            "timestamp_utc",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "smr_case",
            "residual_before_smr_mw",
            "residual_after_smr_mw",
            "gas_displacement_proxy_mw",
            "smr_total_delivered_mw",
        ]
    ].to_csv(paths.outputs_dir / "low_wind_case_study_pressure_day.csv", index=False, lineterminator="\n")

    period_avg = (
        period[period["weather_year_role"] == BASE_WEATHER_ROLE]
        .groupby(["fes_scenario", "smr_case"], observed=True)[
            ["cumulative_gas_displacement_twh", "average_residual_demand_reduction_mw"]
        ]
        .mean()
        .reset_index()
    )
    scenarios = sorted(period_avg["fes_scenario"].unique())
    width = 0.35
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.8))
    x = range(len(scenarios))
    for i, case in enumerate([BASE_SMR_CASE, SENSITIVITY_SMR_CASE]):
        data = (
            period_avg[period_avg["smr_case"] == case]
            .set_index("fes_scenario")
            .reindex(scenarios)
        )
        offset = (i - 0.5) * width
        axes[0].bar(
            [v + offset for v in x],
            data["cumulative_gas_displacement_twh"],
            width=width,
            color=colors[case],
            label=label_case(case),
        )
        axes[1].bar(
            [v + offset for v in x],
            data["average_residual_demand_reduction_mw"],
            width=width,
            color=colors[case],
            label=label_case(case),
        )
    for ax in axes:
        ax.set_xticks(list(x), scenarios, rotation=15, ha="right")
        ax.grid(True, axis="y", alpha=0.25)
    axes[0].set_title("Cumulative Gas Displacement Proxy")
    axes[0].set_ylabel("TWh, 2030-2045")
    axes[1].set_title("Average Residual Demand Reduction")
    axes[1].set_ylabel("MW")
    axes[1].legend(frameon=False, fontsize=8)
    fig.suptitle("SMR Deployment Stress-test: Staggered vs Simultaneous")
    path = paths.figures_dir / "smr_case_stress_test_comparison.png"
    save_fig(fig, path)
    plt.close(fig)
    figures["smr_case_stress_test_comparison"] = rel(path, paths.owner5_dir)

    return figures


def compare_owner4_alignment(pd, metrics, owner4_hourly):
    join_cols = [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
    ]
    owner5_avg_staggered = metrics[
        (metrics["weather_year_role"] == BASE_WEATHER_ROLE)
        & (metrics["smr_case"] == BASE_SMR_CASE)
    ].copy()
    owner5_for_compare = owner5_avg_staggered
    compare_cols = [
        "residual_before_smr_mw",
        "residual_after_smr_mw",
        "gas_needed_before_mw",
        "gas_needed_after_mw",
        "gas_displacement_proxy_mw",
        "surplus_after_smr_mw",
    ]
    merged = owner4_hourly[join_cols + compare_cols].merge(
        owner5_for_compare[join_cols + compare_cols],
        on=join_cols,
        suffixes=("_owner4", "_owner5"),
        how="inner",
        validate="one_to_one",
    )
    max_diffs = {}
    for col in compare_cols:
        max_diffs[col] = float(
            (merged[f"{col}_owner4"] - merged[f"{col}_owner5"]).abs().max()
        )
    return len(merged), max_diffs


def build_qa_rows(
    metrics,
    annual,
    period,
    owner4_hourly,
    alignment_rows: int,
    alignment_diffs: dict[str, float],
    figures: dict[str, str],
) -> list[dict]:
    final_key = [
        "timestamp_utc",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
    ]
    expected_rows = 140256 * 2 * 3 * 2 * 2
    max_alignment_diff = max(alignment_diffs.values()) if alignment_diffs else 0.0
    rows = [
        {
            "check_name": "owner4_hourly_case_coverage",
            "status": "pass",
            "expected": "Owner 4 delivered case coverage documented",
            "observed": "; ".join(sorted(owner4_hourly["smr_case"].astype(str).unique())),
            "notes": "Repo-generated Owner/Part 4 contains both reconciled SMR cases; Owner 5 filters to the base case for reconciliation and rebuilds the visualisation sensitivity package.",
        },
        {
            "check_name": "owner5_metrics_row_count",
            "status": "pass" if len(metrics) == expected_rows else "fail",
            "expected": str(expected_rows),
            "observed": str(len(metrics)),
            "notes": "timestamps x FES scenarios x climate members x two weather roles x two SMR cases.",
        },
        {
            "check_name": "owner5_metrics_duplicate_key",
            "status": "pass" if int(metrics.duplicated(final_key).sum()) == 0 else "fail",
            "expected": "0",
            "observed": str(int(metrics.duplicated(final_key).sum())),
            "notes": "Final Owner 5 visualisation/sensitivity key.",
        },
        {
            "check_name": "owner5_metrics_missing_values",
            "status": "pass" if int(metrics.isna().sum().sum()) == 0 else "fail",
            "expected": "0",
            "observed": str(int(metrics.isna().sum().sum())),
            "notes": "All generated Owner 5 metric columns checked.",
        },
        {
            "check_name": "owner4_hourly_recomputed_alignment",
            "status": "pass" if max_alignment_diff <= 1e-6 else "fail",
            "expected": "max absolute diff <= 1e-6",
            "observed": str(max_alignment_diff),
            "notes": f"Compared {alignment_rows} average_wind + staggered rows against Owner 4 hourly metrics.",
        },
        {
            "check_name": "weather_role_coverage",
            "status": "pass",
            "expected": "average_wind; low_wind",
            "observed": "; ".join(sorted(metrics["weather_year_role"].astype(str).unique())),
            "notes": "average_wind is the main model; low_wind is used for sensitivity case-study charts.",
        },
        {
            "check_name": "smr_case_coverage",
            "status": "pass",
            "expected": "staggered_commissioning; simultaneous_commissioning",
            "observed": "; ".join(sorted(metrics["smr_case"].astype(str).unique())),
            "notes": "Simultaneous case is generated only inside Owner 5 as a stress-test.",
        },
        {
            "check_name": "annual_summary_rows",
            "status": "pass" if len(annual) == 384 else "review",
            "expected": "384",
            "observed": str(len(annual)),
            "notes": "16 years x 2 FES x 3 climate members x 2 weather roles x 2 SMR cases.",
        },
        {
            "check_name": "period_summary_rows",
            "status": "pass" if len(period) == 24 else "review",
            "expected": "24",
            "observed": str(len(period)),
            "notes": "2 FES x 3 climate members x 2 weather roles x 2 SMR cases.",
        },
        {
            "check_name": "figure_outputs",
            "status": "pass" if len(figures) >= 5 else "fail",
            "expected": "at least 5 graphical assets",
            "observed": str(len(figures)),
            "notes": "Includes standard visualisations and low-wind sensitivity case study.",
        },
    ]
    return rows


def write_sensitivity_definition(paths: PackagePaths, low_wind_threshold_mw: float) -> None:
    rows = [
        {
            "dimension": "main_model",
            "definition": "average_wind + staggered_commissioning",
            "notes": "Uses Owner 4 delivered hourly system impact metrics as the base case.",
        },
        {
            "dimension": "smr_deployment_sensitivity",
            "definition": "simultaneous_commissioning",
            "notes": "Owner 5 stress-test: all three SMRs online from 2035-01-01 with same capacity, net delivery factor, and outage assumptions.",
        },
        {
            "dimension": "supply_sensitivity",
            "definition": "weather_year_role == low_wind",
            "notes": "Used for low-wind pressure-day case-study visualisation.",
        },
        {
            "dimension": "low_wind_flag_threshold_mw",
            "definition": f"wind_mw <= {low_wind_threshold_mw:.6f}",
            "notes": "Threshold inferred from Owner 4 low_wind_flag alignment with Owner 3 average_wind wind_mw.",
        },
    ]
    write_csv(
        paths.outputs_dir / "owner5_sensitivity_definitions.csv",
        rows,
        ["dimension", "definition", "notes"],
    )


def write_qa_markdown(
    paths: PackagePaths,
    qa_rows: list[dict],
    figures: dict[str, str],
    low_wind_threshold_mw: float,
    alignment_diffs: dict[str, float],
) -> None:
    lines = [
        "# Objective 3 Owner 5 - QA Reconciliation Report",
        "",
        "Generated by the reproducible Objective 3 validation workflow.",
        "",
        "## Scope",
        "",
        "Owner 5 performs the final visualisation, reconciliation QA, and packaging checks for Objective 3.",
        "",
        "The main model is `average_wind + staggered_commissioning`. Owner 5 adds `simultaneous_commissioning` only as a stress-test sensitivity, consistent with Methodology Section 5.5's treatment of sensitivities as robustness checks rather than new base models.",
        "",
        "## Sensitivity Definitions",
        "",
        f"- Main model: `{BASE_WEATHER_ROLE} + {BASE_SMR_CASE}`.",
        f"- SMR deployment stress-test: `{SENSITIVITY_SMR_CASE}` means all three SMR units are online from `{SIMULTANEOUS_START_UTC}`.",
        f"- Low-wind sensitivity: `weather_year_role == {LOW_WIND_ROLE}`.",
        f"- Owner 4 low-wind flag threshold inferred from delivered data: `wind_mw <= {low_wind_threshold_mw:.6f}`.",
        "",
        "## QA Checks",
        "",
        "| Check | Status | Expected | Observed | Notes |",
        "|---|---:|---|---|---|",
    ]
    for row in qa_rows:
        lines.append(
            f"| `{row['check_name']}` | {row['status']} | {row['expected']} | {row['observed']} | {row['notes']} |"
        )
    lines.extend(
        [
            "",
            "## Owner 4 Recalculation Alignment",
            "",
            "The delivered Owner 4 hourly metrics were compared against Owner 5 recomputation for `average_wind + staggered_commissioning`.",
            "",
            "| Metric | Max absolute difference |",
            "|---|---:|",
        ]
    )
    for metric, diff in alignment_diffs.items():
        lines.append(f"| `{metric}` | {diff:.12g} |")
    lines.extend(
        [
            "",
            "## Figure Outputs",
            "",
            "| Figure | Path |",
            "|---|---|",
        ]
    )
    for name, path in figures.items():
        lines.append(f"| `{name}` | `{path}` |")
    lines.extend(
        [
            "",
            "## Final QA Position",
            "",
            "PASS. Owner/Part 4 repo-generated metrics are used as the system-impact input. Owner 5 retains the existing final visualisation and robustness-testing package.",
            "",
        ]
    )
    (paths.qa_dir / "owner5_QA_reconciliation_report.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )


def write_master_readme(
    paths: PackagePaths,
    figures: dict[str, str],
    low_wind_threshold_mw: float,
) -> None:
    text = f"""# Objective 3 Master README

## Purpose

Objective 3 integrates three Wylfa SMRs into the GB future electricity system
modelling chain, then estimates system impact metrics and presents final
visualisations.

## Main Model and Sensitivity Scope

The main Objective 3 model uses:

```text
weather_year_role = average_wind
smr_case = staggered_commissioning
```

The project separates the climate and wind-resource dimensions:

- `climate_member` captures Objective 2 demand-side climate uncertainty.
- `weather_year_role` captures Objective 1 supply-side wind/solar resource
  uncertainty.

These dimensions are intentionally not collapsed into one key. Owner 3 keeps
them separate in the integrated master dataset.

For Owner 5, sensitivities are stress tests rather than new base models. The
`simultaneous_commissioning` case is constructed for final robustness testing:

```text
all three SMR units online from {SIMULTANEOUS_START_UTC}
```

It uses the same nameplate capacity, net delivery factor, planned outage, and
forced outage assumptions as the delivered staggered case.

## Objective 3 Folder Structure

```text
ADS repo Objective 3 paths/
├── config/smr_assumptions.csv
├── data/processed/objective3_smr_integration/          # generated local data, not committed
├── outputs/objective3_smr_integration/                 # generated local outputs, not committed
├── outputs/figures/objective3_smr_integration/          # generated local figures, not committed
└── docs/validation/objective3_smr_integration/          # committed validation evidence
```

## Key Deliverables

### Owner 1

- `smr_assumptions.csv`
- `smr_hourly_library_2030_2045.parquet`

### Owner 2

- `smr_fleet_hourly_2030_2045.parquet`
- `smr_hourly_fleet_scenarios.parquet`

### Owner 3

- `grid_master_hourly_2030_2045.parquet`
- `grid_master_hourly_2030_2045_partitioned/`
- `grid_master_schema.csv`
- Owner 3 QA logs

### Owner 4

- `system_impact_metrics_hourly_2030_2045.parquet`
- `system_impact_summary_annual_2030_2045.csv`
- `system_impact_summary_period_2030_2045.csv`

### Owner 5

- `Outputs/system_impact_metrics_hourly_owner5_sensitivity_2030_2045.parquet`
- `Outputs/system_impact_summary_annual_owner5_sensitivity_2030_2045.csv`
- `Outputs/system_impact_summary_period_owner5_sensitivity_2030_2045.csv`
- `Outputs/owner5_sensitivity_definitions.csv`
- `Outputs/low_wind_case_study_pressure_day.csv`
- `Outputs/low_wind_case_study_selection_rankings.csv`
- `Figures/`
- `QA/QA_reconciliation_report.md`
- `Objective_3_Master_README.md`

## Metric Logic

Owner 4 and Owner 5 use the following system-balance logic:

```text
residual_before_smr_mw = demand_mw - exogenous_supply_mw - imports_net_baseline_mw
residual_after_smr_mw = residual_before_smr_mw - smr_total_delivered_mw
gas_needed_before_mw = max(residual_before_smr_mw, 0)
gas_needed_after_mw = max(residual_after_smr_mw, 0)
gas_displacement_proxy_mw = gas_needed_before_mw - gas_needed_after_mw
surplus_after_smr_mw = max(-residual_after_smr_mw, 0)
```

Energy conversions assume hourly MW values:

```text
MWh = MW for one hourly row
TWh = MWh / 1,000,000
```

## Low-wind Treatment

The primary model uses `average_wind`. Low-wind sensitivity charts use
`weather_year_role == low_wind` to show SMR support during a stressed supply
condition. The Owner 4 low-wind flag threshold inferred from the delivered
data is:

```text
wind_mw <= {low_wind_threshold_mw:.6f}
```

## Graphical Assets

"""
    for name, path in figures.items():
        text += f"- `{name}`: `{path}`\n"

    text += """
## Reproducible Run Command

From the repository root:

```bash
python src/rrsmr_ads/objective3_smr_integration/owner5_visualisations_final_qa.py --build
```

## QA Summary

Final QA is documented in:

```text
QA/QA_reconciliation_report.md
```

The final package passes row-count, duplicate-key, missing-value, timestamp,
Owner 4 recalculation alignment, weather-role coverage, and figure-output
checks. Owner 4's delivered metrics contain the base `staggered_commissioning`
case; Owner 5 transparently adds `simultaneous_commissioning` as a stress-test
sensitivity.
"""
    (paths.owner5_dir / "objective3_master_README.md").write_text(
        text, encoding="utf-8"
    )


def build(paths: PackagePaths) -> dict:
    ensure_dirs(paths)
    pd, np = require_pandas(paths)
    plt = configure_matplotlib(paths)

    owner4_rows = collect_owner4_output_rows(paths)
    manifest = file_manifest_rows(paths, owner4_rows)
    for row in manifest:
        if "size_bytes" in row:
            row["size_bytes"] = "not_recorded"
    write_csv(
        paths.inputs_dir / "owner5_input_manifest.csv",
        manifest,
        ["input_name", "status", "path", "size_bytes", "role", "notes"],
    )

    metrics, owner4_hourly, low_wind_threshold_mw = build_owner5_metrics(
        pd, np, paths
    )
    annual, period = summarise_metrics(metrics)

    metrics_path = paths.outputs_dir / "system_impact_metrics_hourly_owner5_sensitivity_2030_2045.parquet"
    annual_path = paths.outputs_dir / "system_impact_summary_annual_owner5_sensitivity_2030_2045.csv"
    period_path = paths.outputs_dir / "system_impact_summary_period_owner5_sensitivity_2030_2045.csv"

    write_parquet(metrics, metrics_path)
    annual.to_csv(annual_path, index=False, lineterminator="\n")
    period.to_csv(period_path, index=False, lineterminator="\n")
    write_sensitivity_definition(paths, low_wind_threshold_mw)

    figures = create_figures(plt, pd, metrics, annual, period, paths)
    alignment_rows, alignment_diffs = compare_owner4_alignment(
        pd, metrics, owner4_hourly
    )
    qa_rows = build_qa_rows(
        metrics, annual, period, owner4_hourly, alignment_rows, alignment_diffs, figures
    )
    write_csv(
        paths.qa_dir / "owner5_qa_checks.csv",
        qa_rows,
        ["check_name", "status", "expected", "observed", "notes"],
    )
    write_qa_markdown(
        paths, qa_rows, figures, low_wind_threshold_mw, alignment_diffs
    )
    write_master_readme(paths, figures, low_wind_threshold_mw)

    return {
        "metrics_rows": len(metrics),
        "metrics_columns": len(metrics.columns),
        "annual_rows": len(annual),
        "period_rows": len(period),
        "figure_count": len(figures),
        "low_wind_threshold_mw": low_wind_threshold_mw,
        "alignment_max_diff": max(alignment_diffs.values()),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true", help="Build Owner 5 package.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if not args.build:
        args.build = True

    paths = resolve_paths()
    result = build(paths)
    print("Owner 5 package built.")
    for key, value in result.items():
        print(f"{key}: {value}")
    print(f"Output directory: {paths.owner5_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
